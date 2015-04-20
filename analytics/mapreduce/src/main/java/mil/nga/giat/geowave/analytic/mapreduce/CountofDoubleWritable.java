package mil.nga.giat.geowave.analytic.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Used for (1) representation of collections (2) summation in a combiner (3)
 * and finally, for computation of averages
 */
public class CountofDoubleWritable implements
		Writable,
		WritableComparable
{

	private double value = 0.0;
	private double count = 0.0;

	public CountofDoubleWritable() {

	}

	public CountofDoubleWritable(
			double value,
			double count ) {
		set(
				value,
				count);
	}

	@Override
	public void readFields(
			DataInput in )
			throws IOException {
		value = in.readDouble();
		count = in.readDouble();
	}

	@Override
	public void write(
			DataOutput out )
			throws IOException {
		out.writeDouble(value);
		out.writeDouble(count);
	}

	public void set(
			double value,
			double count ) {
		this.value = value;
		this.count = count;
	}

	public double getValue() {
		return value;
	}

	public double getCount() {
		return count;
	}

	/**
	 * Returns true iff <code>o</code> is a DoubleWritable with the same value.
	 */
	@Override
	public boolean equals(
			Object o ) {
		if (!(o instanceof CountofDoubleWritable)) {
			return false;
		}
		return this.compareTo(o) == 0;
	}

	@Override
	public int hashCode() {
		return (int) Double.doubleToLongBits(value / count);
	}

	@Override
	public int compareTo(
			Object o ) {
		CountofDoubleWritable other = (CountofDoubleWritable) o;
		final double diff = (value / count) - (other.value / other.count);
		return (Math.abs(diff) < 0.0000001) ? 0 : (diff < 0 ? -1 : 0);
	}

	public String toString() {
		return Double.toString(value) + "/" + Double.toString(count);
	}

	/** A Comparator optimized for DoubleWritable. */
	public static class Comparator extends
			WritableComparator implements
			Serializable
	{
		public Comparator() {
			super(
					CountofDoubleWritable.class);
		}

		public int compare(
				byte[] b1,
				int s1,
				int l1,
				byte[] b2,
				int s2,
				int l2 ) {
			double thisValue = readDouble(
					b1,
					s1);
			double thatValue = readDouble(
					b2,
					s2);
			return Double.compare(
					thisValue,
					thatValue);
		}
	}

	static { // register this comparator
		WritableComparator.define(
				CountofDoubleWritable.class,
				new Comparator());
	}
}
