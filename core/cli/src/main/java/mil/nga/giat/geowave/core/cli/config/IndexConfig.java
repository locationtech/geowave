package mil.nga.giat.geowave.core.cli.config;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import mil.nga.giat.geowave.core.cli.DataAdapterProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.CompoundIndexStrategy;
import mil.nga.giat.geowave.core.index.simple.HashKeyIndexStrategy;
import mil.nga.giat.geowave.core.index.simple.RoundRobinKeyIndexStrategy;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class IndexConfig
{
	private final static Logger LOGGER = LoggerFactory.getLogger(
			IndexConfig.class);
	private static Map<String, IngestDimensionalityTypeProviderSpi> registeredDimensionalityTypes = null;
	@Parameter(description = "name")
	private String indexName;
	@Parameter(names = {
		"-t",
		"--type"
	}, required = true, description = "The type of the index, defined by dimensional definitions such as spatial or spatial-temporal")
	private String type;

	@Parameter(names = {
		"-np",
		"--numpartitions"
	}, required = false, description = "The number of partitions.  Default partitions will be 1.")
	protected int numPartitions = 1;

	@Parameter(names = {
		"-ps",
		"--partitionstrategy"
	}, required = false, description = "The partition strategy to use.  Default will be none.", converter = PartitionStrategyConverter.class)
	protected PartitionStrategy partitionStrategy = PartitionStrategy.NONE;

	@DynamicParameter(names = {
		"-t.",
		"--type."
	}, description = "Parameters specific to the particular index type go here")
	private final Map<String, String> params = new HashMap<String, String>();

	public String getType() {
		return type;
	}

	public void setType(
			final String type ) {
		this.type = type;
	}

	public int getNumPartitions() {
		return numPartitions;
	}

	public PartitionStrategy getPartitionStrategy() {
		return partitionStrategy;
	}

	public PrimaryIndex wrapIndexWithOptions(
			final PrimaryIndex index ) {
		PrimaryIndex retVal = index;
		if ((numPartitions > 1) && partitionStrategy.equals(
				PartitionStrategy.HASH)) {
			retVal = new CustomIdIndex(
					new CompoundIndexStrategy(
							new HashKeyIndexStrategy(
									index.getIndexStrategy().getOrderedDimensionDefinitions(),
									numPartitions),
							index.getIndexStrategy()),
					index.getIndexModel(),
					new ByteArrayId(
							index.getId().getString() + "_" + PartitionStrategy.HASH.name() + "_" + numPartitions));
		}
		else if (numPartitions > 1) {
			// default to round robin partitioning (none is not valid if there
			// are more than 1 partition)
			if (partitionStrategy.equals(
					PartitionStrategy.NONE)) {
				LOGGER.warn(
						"Partition strategy is necessary when using more than 1 partition, defaulting to 'round_robin' partitioning.");
			}
			retVal = new CustomIdIndex(
					new CompoundIndexStrategy(
							new RoundRobinKeyIndexStrategy(
									numPartitions),
							index.getIndexStrategy()),
					index.getIndexModel(),
					new ByteArrayId(
							index.getId().getString() + "_" + PartitionStrategy.ROUND_ROBIN.name() + "_" + numPartitions));
		}
		if ((indexName != null) && (indexName.length() > 0)) {
			retVal = new CustomIdIndex(
					retVal.getIndexStrategy(),
					retVal.getIndexModel(),
					new ByteArrayId(
							indexName));
		}
		return retVal;
	}

	public PrimaryIndex getIndex(
			final DataAdapterProvider<?> adapterProvider ) {
		final IngestDimensionalityTypeProviderSpi dimensionalityType = getSelectedDimensionalityProvider(
				type);

		if (isCompatible(
				adapterProvider,
				dimensionalityType)) {
			final JCommander commander = new JCommander();
			commander.addObject(
					dimensionalityType.getOptions());
			commander.setAcceptUnknownOptions(
					true);
			String[] args = new String[params.size()];
			commander.parse(args);
			final PrimaryIndex index = dimensionalityType.createPrimaryIndex();
			return wrapIndexWithOptions(
					index);

		}
		return null;
	}

	/**
	 * Determine whether an index is compatible with the visitor
	 *
	 * @param index
	 *            an index that an ingest type supports
	 * @return whether the adapter is compatible with the common index model
	 */
	public static boolean isCompatible(
			final DataAdapterProvider<?> adapterProvider,
			final IngestDimensionalityTypeProviderSpi dimensionalityProvider ) {
		final Class<? extends CommonIndexValue>[] supportedTypes = adapterProvider.getSupportedIndexableTypes();
		if ((supportedTypes == null) || (supportedTypes.length == 0)) {
			return false;
		}
		final Class<? extends CommonIndexValue>[] requiredTypes = dimensionalityProvider.getRequiredIndexTypes();
		for (final Class<? extends CommonIndexValue> requiredType : requiredTypes) {
			boolean fieldFound = false;
			for (final Class<? extends CommonIndexValue> supportedType : supportedTypes) {
				if (requiredType.isAssignableFrom(
						supportedType)) {
					fieldFound = true;
					break;
				}
			}
			if (!fieldFound) {
				return false;
			}
		}
		return true;

	}

	protected static enum PartitionStrategy {
		NONE,
		HASH,
		ROUND_ROBIN;
		// converter that will be used later
		public static PartitionStrategy fromString(
				final String code ) {

			for (final PartitionStrategy output : PartitionStrategy.values()) {
				if (output.toString().equalsIgnoreCase(
						code)) {
					return output;
				}
			}

			return null;
		}
	}

	public static class PartitionStrategyConverter implements
			IStringConverter<PartitionStrategy>
	{
		@Override
		public PartitionStrategy convert(
				final String value ) {
			final PartitionStrategy convertedValue = PartitionStrategy.fromString(
					value);

			if (convertedValue == null) {
				throw new ParameterException(
						"Value '" + value + "' can not be converted to a partition strategy. " + "Available values are: " + StringUtils.join(
								PartitionStrategy.values(),
								", ").toLowerCase());
			}
			return convertedValue;
		}

	}

	private static IngestDimensionalityTypeProviderSpi getSelectedDimensionalityProvider(
			final String dimensionalityType ) {
		if (registeredDimensionalityTypes == null) {
			initDimensionalityTypeRegistry();
		}

		return registeredDimensionalityTypes.get(
				dimensionalityType);
	}

	private static synchronized void initDimensionalityTypeRegistry() {
		registeredDimensionalityTypes = new HashMap<String, IngestDimensionalityTypeProviderSpi>();
		final Iterator<IngestDimensionalityTypeProviderSpi> dimensionalityTypesProviders = ServiceLoader.load(
				IngestDimensionalityTypeProviderSpi.class).iterator();
		while (dimensionalityTypesProviders.hasNext()) {
			final IngestDimensionalityTypeProviderSpi dimensionalityTypeProvider = dimensionalityTypesProviders.next();
			if (registeredDimensionalityTypes.containsKey(
					dimensionalityTypeProvider.getDimensionalityTypeName())) {
				LOGGER.warn(
						"Dimensionality type '" + dimensionalityTypeProvider.getDimensionalityTypeName() + "' already registered.  Unable to register type provided by " + dimensionalityTypeProvider.getClass().getName());
			}
			else {
				registeredDimensionalityTypes.put(
						dimensionalityTypeProvider.getDimensionalityTypeName(),
						dimensionalityTypeProvider);
			}
		}
	}

}
