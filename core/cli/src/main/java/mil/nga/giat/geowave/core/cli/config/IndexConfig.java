package mil.nga.giat.geowave.core.cli.config;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import mil.nga.giat.geowave.core.cli.DataAdapterProvider;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class IndexConfig
{
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

	public PrimaryIndex getIndex(
			final DataAdapterProvider<?> adapterProvider,
			final String[] args ) {
		final IngestDimensionalityTypeProviderSpi dimensionalityType = getSelectedDimensionalityProvider(getDimensionalityType());

		if (isCompatible(
				adapterProvider,
				dimensionalityType)) {
			final JCommander commander = new JCommander();
			commander.addObject(dimensionalityType.getOptions());

			final List<Object> options = getIndexOptions();
			for (final Object opt : options) {
				commander.addObject(opt);
			}
			commander.setAcceptUnknownOptions(true);
			commander.parse(args);
			final PrimaryIndex index = dimensionalityType.createPrimaryIndex();
			return wrapIndexWithOptions(index);

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
				if (requiredType.isAssignableFrom(supportedType)) {
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
		RANDOM,
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
}
