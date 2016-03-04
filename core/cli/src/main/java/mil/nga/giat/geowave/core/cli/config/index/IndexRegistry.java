package mil.nga.giat.geowave.core.cli.config.index;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.cli.DataAdapterProvider;
import mil.nga.giat.geowave.core.cli.config.IngestDimensionalityTypeProviderSpi;
import mil.nga.giat.geowave.core.cli.config.index.BaseIndexConfig.PartitionStrategy;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.CompoundIndexStrategy;
import mil.nga.giat.geowave.core.index.simple.HashKeyIndexStrategy;
import mil.nga.giat.geowave.core.index.simple.RoundRobinKeyIndexStrategy;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class IndexRegistry
{
	private final static Logger LOGGER = LoggerFactory.getLogger(
			IndexRegistry.class);

	private static Map<String, IngestDimensionalityTypeProviderSpi> registeredDimensionalityTypes = null;

	public PrimaryIndex wrapIndexWithOptions(
			final BaseIndexConfig config,
			final PrimaryIndex index ) {
		PrimaryIndex retVal = index;
		if ((config.getNumPartitions() > 1) && config.getPartitionStrategy().equals(
				PartitionStrategy.HASH)) {
			retVal = new CustomIdIndex(
					new CompoundIndexStrategy(
							new HashKeyIndexStrategy(
									index.getIndexStrategy().getOrderedDimensionDefinitions(),
									config.getNumPartitions()),
							index.getIndexStrategy()),
					index.getIndexModel(),
					new ByteArrayId(
							index.getId().getString() + "_" + PartitionStrategy.HASH.name() + "_" + config.getNumPartitions()));
		}
		else if (config.getNumPartitions() > 1) {
			// default to round robin partitioning (none is not valid if there
			// are more than 1 partition)
			if (config.getPartitionStrategy().equals(
					PartitionStrategy.NONE)) {
				LOGGER.warn(
						"Partition strategy is necessary when using more than 1 partition, defaulting to 'round_robin' partitioning.");
			}
			retVal = new CustomIdIndex(
					new CompoundIndexStrategy(
							new RoundRobinKeyIndexStrategy(
									config.getNumPartitions()),
							index.getIndexStrategy()),
					index.getIndexModel(),
					new ByteArrayId(
							index.getId().getString() + "_" + PartitionStrategy.ROUND_ROBIN.name() + "_" + config.getNumPartitions()));
		}
		if ((config.getIndexName() != null) && (config.getIndexName().length() > 0)) {
			retVal = new CustomIdIndex(
					retVal.getIndexStrategy(),
					retVal.getIndexModel(),
					new ByteArrayId(
							config.getIndexName()));
		}
		return retVal;
	}

	public PrimaryIndex getIndex(
			final BaseIndexConfig config,
			final DataAdapterProvider<?> adapterProvider ) {
		final IngestDimensionalityTypeProviderSpi dimensionalityType = getSelectedDimensionalityProvider(
				config.getIndexName());

		if (isCompatible(
				adapterProvider,
				dimensionalityType)) {
			// TODO need to make sure whatever dynamic options get set on this
			// dimensionality type instance
			final PrimaryIndex index = dimensionalityType.createPrimaryIndex();
			return wrapIndexWithOptions(
					config,
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
