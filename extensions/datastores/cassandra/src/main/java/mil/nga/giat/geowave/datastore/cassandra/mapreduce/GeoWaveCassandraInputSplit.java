package mil.nga.giat.geowave.datastore.cassandra.mapreduce;

import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.cassandra.split.CassandraSplitsProvider;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveInputSplit;
import mil.nga.giat.geowave.mapreduce.splits.RangeLocationPair;

public class GeoWaveCassandraInputSplit extends
		GeoWaveInputSplit
{
	public GeoWaveCassandraInputSplit() {
		super();
	}

	public GeoWaveCassandraInputSplit(
			final Map<PrimaryIndex, List<RangeLocationPair>> splitInfo,
			final String[] locations ) {
		super(
				splitInfo,
				locations);
	}

	@Override
	protected RangeLocationPair getRangeLocationPairInstance() {
		return CassandraSplitsProvider.defaultConstructRangeLocationPair();
	}
}
