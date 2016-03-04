package mil.nga.giat.geowave.core.cli.config.index;

import org.apache.commons.lang3.StringUtils;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class BaseIndexConfig
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

	protected String indexName;

	public BaseIndexConfig(
			final String indexName ) {
		this.indexName = indexName;
	}

	public String getIndexName() {
		return indexName;
	}

	public String getType() {
		return type;
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
