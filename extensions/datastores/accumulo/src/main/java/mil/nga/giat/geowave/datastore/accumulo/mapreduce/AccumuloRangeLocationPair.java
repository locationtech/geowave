package mil.nga.giat.geowave.datastore.accumulo.mapreduce;

import org.apache.accumulo.core.data.Range;

import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;
import mil.nga.giat.geowave.mapreduce.splits.RangeLocationPair;

public class AccumuloRangeLocationPair extends
		RangeLocationPair
{
	public AccumuloRangeLocationPair(
			final GeoWaveRowRange range,
			final String location,
			final double cardinality ) {
		super(
				range,
				location,
				cardinality);
	}

	public AccumuloRangeLocationPair() {
		super();
	}

	@Override
	protected GeoWaveRowRange buildRowRangeInstance() {
		return new AccumuloRowRange(
				new Range());
	}
}
