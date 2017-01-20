package mil.nga.giat.geowave.datastore.hbase.mapreduce;

import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;
import mil.nga.giat.geowave.mapreduce.splits.RangeLocationPair;

public class HBaseRangeLocationPair extends
		RangeLocationPair
{
	public HBaseRangeLocationPair(
			final GeoWaveRowRange range,
			final String location,
			final double cardinality ) {
		super(
				range,
				location,
				cardinality);
	}

	public HBaseRangeLocationPair() {
		super();
	}

	@Override
	protected GeoWaveRowRange buildRowRangeInstance() {
		return new HBaseRowRange();
	}
}
