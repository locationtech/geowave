package mil.nga.giat.geowave.datastore.accumulo.mapreduce;

import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveInputSplit;
import mil.nga.giat.geowave.mapreduce.splits.RangeLocationPair;

public class GeoWaveAccumuloInputSplit extends
		GeoWaveInputSplit
{

	public GeoWaveAccumuloInputSplit() {
		super();
	}

	public GeoWaveAccumuloInputSplit(
			final Map<PrimaryIndex, List<RangeLocationPair>> splitInfo,
			final String[] locations ) {
		super(
				splitInfo,
				locations);
	}

	@Override
	protected RangeLocationPair getRangeLocationPairInstance() {
		return AccumuloSplitsProvider.defaultConstructRangeLocationPair();
	}
}
