package mil.nga.giat.geowave.datastore.dynamodb.mapreduce;

import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.dynamodb.split.DynamoDBSplitsProvider;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveInputSplit;
import mil.nga.giat.geowave.mapreduce.splits.RangeLocationPair;

public class GeoWaveDynamoDBInputSplit extends
		GeoWaveInputSplit
{
	public GeoWaveDynamoDBInputSplit() {
		super();
	}

	public GeoWaveDynamoDBInputSplit(
			final Map<PrimaryIndex, List<RangeLocationPair>> splitInfo,
			final String[] locations ) {
		super(
				splitInfo,
				locations);
	}

	@Override
	protected RangeLocationPair getRangeLocationPairInstance() {
		return DynamoDBSplitsProvider.defaultConstructRangeLocationPair();
	}
}
