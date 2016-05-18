package mil.nga.giat.geowave.datastore.hbase.mapreduce.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InvalidObjectException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;

public class RangeLocationPair
{
	// Type of 'range' is the only difference between this and the Accumulo
	// version

	// Should change to a generic type and reuse

	private HBaseMRRowRange range;
	private String location;
	private double cardinality;

	public RangeLocationPair() {}

	public RangeLocationPair(
			final HBaseMRRowRange range,
			final String location,
			final double cardinality ) {
		this.location = location;
		this.range = range;
		this.cardinality = cardinality;
	}

	public double getCardinality() {
		return cardinality;
	}

	public HBaseMRRowRange getRange() {
		return range;
	}

	public String getLocation() {
		return location;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((location == null) ? 0 : location.hashCode());
		result = (prime * result) + ((range == null) ? 0 : range.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final RangeLocationPair other = (RangeLocationPair) obj;
		if (location == null) {
			if (other.location != null) {
				return false;
			}
		}
		else if (!location.equals(other.location)) {
			return false;
		}
		if (range == null) {
			if (other.range != null) {
				return false;
			}
		}
		else if (!range.equals(other.range)) {
			return false;
		}
		return true;
	}

	public void readFields(
			final DataInput in )
			throws IOException,
			InstantiationException,
			IllegalAccessException {
		range = HBaseMRRowRange.class.newInstance();
		range.readFields(in);
		location = in.readUTF();
		cardinality = in.readDouble();
	}

	public void write(
			final DataOutput out )
			throws IOException {
		range.write(out);
		out.writeUTF(location);
		out.writeDouble(cardinality);
	}

	// Wraps ByteArrayRange adding Writable implementation
	// Does not support singleValue or non-default inclusivity
	public static class HBaseMRRowRange extends
			ByteArrayRange implements
			Writable
	{

		public HBaseMRRowRange(
				final ByteArrayRange copy ) {
			super(
					copy.getStart(),
					copy.getEnd());
		}

		public HBaseMRRowRange(
				final ByteArrayId start,
				final ByteArrayId end ) {
			super(
					start,
					end);
		}

		public HBaseMRRowRange() {
			super(
					null,
					null);
		}

		@Override
		public void readFields(
				final DataInput in )
				throws IOException {

			final int startKeyLen = WritableUtils.readVInt(in);
			final int endKeyLen = WritableUtils.readVInt(in);

			final byte[] startBytes = new byte[startKeyLen];
			final byte[] endBytes = new byte[endKeyLen];

			in.readFully(startBytes);
			in.readFully(endBytes);

			start = new ByteArrayId(
					startBytes);
			end = new ByteArrayId(
					endBytes);

			if (start.compareTo(end) > 0) {
				throw new InvalidObjectException(
						"Start key must be less than end key in range (" + start.getString() + ", " + end.getString()
								+ ")");
			}
		}

		@Override
		public void write(
				final DataOutput out )
				throws IOException {

			final byte[] startBytes = start.getBytes();
			final byte[] endBytes = end.getBytes();

			WritableUtils.writeVInt(
					out,
					startBytes.length);
			WritableUtils.writeVInt(
					out,
					endBytes.length);

			out.write(startBytes);
			out.write(endBytes);
		}
	}
}
