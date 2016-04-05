package mil.nga.giat.geowave.core.ingest.index;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.CompoundIndexStrategy;
import mil.nga.giat.geowave.core.index.simple.HashKeyIndexStrategy;
import mil.nga.giat.geowave.core.index.simple.RoundRobinKeyIndexStrategy;
import mil.nga.giat.geowave.core.ingest.index.BaseIndexOptions.PartitionStrategy;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

import org.apache.log4j.Logger;

public class BaseIndexOptionProvider implements
		IndexOptionProviderSpi
{
	private final static Logger LOGGER = Logger.getLogger(BaseIndexOptionProvider.class);
	private final BaseIndexOptions options = new BaseIndexOptions();

	public BaseIndexOptionProvider() {
		super();
	}

	@Override
	public Object getOptions() {
		return options;
	}

	@Override
	public PrimaryIndex wrapIndexWithOptions(
			final PrimaryIndex index ) {
		PrimaryIndex retVal = index;
		if ((options.numPartitions > 1) && options.partitionStrategy.equals(PartitionStrategy.HASH)) {
			retVal = new CustomIdIndex(
					new CompoundIndexStrategy(
							new HashKeyIndexStrategy(
									index.getIndexStrategy().getOrderedDimensionDefinitions(),
									options.numPartitions),
							index.getIndexStrategy()),
					index.getIndexModel(),
					new ByteArrayId(
							index.getId().getString() + "_" + PartitionStrategy.HASH.name() + "_" + options.numPartitions));
		}
		else if (options.numPartitions > 1) {
			// default to round robin partitioning (none is not valid if there
			// are more than 1 partition)
			if (options.partitionStrategy.equals(PartitionStrategy.NONE)) {
				LOGGER.warn("Partition strategy is necessary when using more than 1 partition, defaulting to 'round_robin' partitioning.");
			}
			retVal = new CustomIdIndex(
					new CompoundIndexStrategy(
							new RoundRobinKeyIndexStrategy(
									options.numPartitions),
							index.getIndexStrategy()),
					index.getIndexModel(),
					new ByteArrayId(
							index.getId().getString() + "_" + PartitionStrategy.ROUND_ROBIN.name() + "_" + options.numPartitions));
		}
		if ((options.nameOverride != null) && (options.nameOverride.length() > 0)) {
			retVal = new CustomIdIndex(
					retVal.getIndexStrategy(),
					retVal.getIndexModel(),
					new ByteArrayId(
							options.nameOverride));
		}
		return retVal;
	}

	@Override
	public int getResolutionOrder() {
		// this is arbitrary, any other option providers would be in order
		// relative to this
		return 10;
	}

}
