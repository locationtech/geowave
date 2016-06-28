package mil.nga.giat.geowave.datastore.accumulo.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.data.Range;

import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;

public class AccumuloRowRange implements
		GeoWaveRowRange
{

	private Range range;

	public AccumuloRowRange(
			final Range range ) {
		this.range = range;
	}

	public Range getRange() {
		return range;
	}

	@Override
	public void write(
			final DataOutput out )
			throws IOException {
		range.write(out);
	}

	@Override
	public void readFields(
			final DataInput in )
			throws IOException {
		range = new Range();
		range.readFields(in);
	}

	@Override
	public byte[] getStartKey() {
		return range.getStartKey().getRowData().getBackingArray();
	}

	@Override
	public byte[] getEndKey() {
		return range.getEndKey().getRowData().getBackingArray();
	}

	@Override
	public boolean isStartKeyInclusive() {
		return range.isStartKeyInclusive();
	}

	@Override
	public boolean isEndKeyInclusive() {
		return range.isEndKeyInclusive();
	}

	@Override
	public boolean isInfiniteStartKey() {
		return range.isInfiniteStartKey();
	}

	@Override
	public boolean isInfiniteStopKey() {
		return range.isInfiniteStopKey();
	}

}
