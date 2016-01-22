package mil.nga.giat.geowave.core.ingest.index;

import org.apache.commons.lang3.StringUtils;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class BaseIndexOptions
{
	@Parameter(names = {
		"-indexName"
	}, required = false, description = "A custom name can be given to this index. Default name will be the based on configuration parameters.")
	protected String nameOverride = null;

	@Parameter(names = {
		"-numPartitions"
	}, required = false, description = "The number of partitions.  Default partitions will be 1.")
	protected int numPartitions = 1;

	@Parameter(names = {
		"-partitionStrategy"
	}, required = false, description = "The partition strategy to use.  Default will be none.", converter = PartitionStrategyConverter.class)
	protected PartitionStrategy partitionStrategy = PartitionStrategy.NONE;

	public String getNameOverride() {
		return nameOverride;
	}

	public int getNumPartitions() {
		return numPartitions;
	}

	public PartitionStrategy getPartitionStrategy() {
		return partitionStrategy;
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
			final PartitionStrategy convertedValue = PartitionStrategy.fromString(value);

			if (convertedValue == null) {
				throw new ParameterException(
						"Value " + value + "can not be converted to an partition strategy. " + "Available values are: " + StringUtils.join(
								PartitionStrategy.values(),
								", ").toLowerCase());
			}
			return convertedValue;
		}

	}
}
