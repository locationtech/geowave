package mil.nga.giat.geowave.datastore.dynamodb.mapreduce;

import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;
import mil.nga.giat.geowave.mapreduce.splits.RangeLocationPair;

public class DynamoDBRangeLocatorPair extends
		RangeLocationPair
{
	public DynamoDBRangeLocatorPair(
			final GeoWaveRowRange range,
			final String location,
			final double cardinality ) {
		super(
				range,
				location,
				cardinality);
	}

	public DynamoDBRangeLocatorPair() {
		super();
	}

	@Override
	protected GeoWaveRowRange buildRowRangeInstance() {
		return new DynamoDBRowRange();
	}
}
