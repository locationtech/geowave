package mil.nga.giat.geowave.datastore.cassandra.mapreduce;

import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;
import mil.nga.giat.geowave.mapreduce.splits.RangeLocationPair;

public class CassandraRangeLocatorPair extends
		RangeLocationPair
{
	public CassandraRangeLocatorPair(
			final GeoWaveRowRange range,
			final String location,
			final double cardinality ) {
		super(
				range,
				location,
				cardinality);
	}

	public CassandraRangeLocatorPair() {
		super();
	}

	@Override
	protected GeoWaveRowRange buildRowRangeInstance() {
		return new CassandraRowRange();
	}
}
