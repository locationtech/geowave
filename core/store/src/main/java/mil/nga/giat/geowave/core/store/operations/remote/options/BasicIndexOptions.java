package mil.nga.giat.geowave.core.store.operations.remote.options;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions.PartitionStrategy;

public class BasicIndexOptions
{

	@Parameter(names = {
		"--indexName"
	}, description = "A custom name can be given to this index. Default name will be the based on configuration parameters.")
	protected String nameOverride = null;

	@Parameter(names = {
		"-np",
		"--numPartitions"
	}, description = "The number of partitions.  Default partitions will be 1.")
	protected int numPartitions = 1;

	@Parameter(names = {
		"-ps",
		"--partitionStrategy"
	}, description = "The partition strategy to use.  Default will be none.")
	protected PartitionStrategy partitionStrategy = PartitionStrategy.NONE;

	public String getNameOverride() {
		return nameOverride;
	}

	public void setNameOverride(
			String nameOverride ) {
		this.nameOverride = nameOverride;
	}

	public int getNumPartitions() {
		return numPartitions;
	}

	public void setNumPartitions(
			int numPartitions ) {
		this.numPartitions = numPartitions;
	}

	public PartitionStrategy getPartitionStrategy() {
		return partitionStrategy;
	}

	public void setPartitionStrategy(
			PartitionStrategy partitionStrategy ) {
		this.partitionStrategy = partitionStrategy;
	}

}
