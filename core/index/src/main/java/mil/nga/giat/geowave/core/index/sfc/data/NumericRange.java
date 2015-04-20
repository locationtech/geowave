package mil.nga.giat.geowave.core.index.sfc.data;

/**
 * Concrete implementation defining a numeric range associated with a space
 * filling curve.
 * 
 */
public class NumericRange implements
		NumericData
{
	private final double min;
	private final double max;

	/**
	 * Constructor used to create a IndexRange object
	 * 
	 * @param min
	 *            the minimum bounds of a unique index range
	 * @param max
	 *            the maximum bounds of a unique index range
	 */
	public NumericRange(
			final double min,
			final double max ) {
		this.min = min;
		this.max = max;
	}

	/**
	 * 
	 * @return min the minimum bounds of a index range object
	 */
	@Override
	public double getMin() {
		return min;
	}

	/**
	 * 
	 * @return max the maximum bounds of a index range object
	 */
	@Override
	public double getMax() {
		return max;
	}

	/**
	 * 
	 * @return centroid the center of a unique index range object
	 */
	@Override
	public double getCentroid() {
		return (min + max) / 2;
	}

	/**
	 * Flag to determine if the object is a range
	 */
	@Override
	public boolean isRange() {
		return true;
	}

	@Override
	public String toString() {
		return "NumericRange [min=" + min + ", max=" + max + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(max);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(min);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		NumericRange other = (NumericRange) obj;
		return (Math.abs(max - other.max) < NumericValue.EPSILON) && (Math.abs(min - other.min) < NumericValue.EPSILON);
	}
}
