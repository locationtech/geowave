package mil.nga.giat.geowave.datastore.accumulo.mapreduce.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.data.Range;

public class RangeLocationPair
{

	private Range range;
	private String location;
	private double cardinality;

	public RangeLocationPair() {}

	public RangeLocationPair(
			final Range range,
			final String location,
			final double cardinality ) {
		this.location = location;
		this.range = range;
		this.cardinality = cardinality;
	}

	public double getCardinality() {
		return cardinality;
	}

	public Range getRange() {
		return range;
	}

	public String getLocation() {
		return location;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((location == null) ? 0 : location.hashCode());
		result = prime * result + ((range == null) ? 0 : range.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		RangeLocationPair other = (RangeLocationPair) obj;
		if (location == null) {
			if (other.location != null) return false;
		}
		else if (!location.equals(other.location)) return false;
		if (range == null) {
			if (other.range != null) return false;
		}
		else if (!range.equals(other.range)) return false;
		return true;
	}

	public void readFields(
			final DataInput in )
			throws IOException,
			InstantiationException,
			IllegalAccessException {
		range = Range.class.newInstance();
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
}
