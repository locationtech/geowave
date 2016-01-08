package mil.nga.giat.geowave.core.ingest.index;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.CompoundIndexStrategy;
import mil.nga.giat.geowave.core.index.simple.RoundRobinKeyIndexStrategy;
import mil.nga.giat.geowave.core.ingest.index.BaseIndexOptions.PartitionStrategy;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class BaseIndexOptionProvider implements
		IndexOptionProviderSpi
{
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
		if ((options.numPartitions > 1) && !options.partitionStrategy.equals(PartitionStrategy.NONE)) {
			// TODO add random partition strategy
			retVal = new CustomIdIndex(
					new CompoundIndexStrategy(
							new RoundRobinKeyIndexStrategy(
									options.numPartitions),
							index.getIndexStrategy()),
					index.getIndexModel(),
					new ByteArrayId(
							index.getId().getString() + "_" + options.partitionStrategy.name() + "_" + options.numPartitions));
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
