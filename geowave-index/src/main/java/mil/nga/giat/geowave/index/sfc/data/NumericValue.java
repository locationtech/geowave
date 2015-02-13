package mil.nga.giat.geowave.index.sfc.data;

/**
 * Concrete implementation defining a single numeric value associated with a
 * space filling curve.
 * 
 */
public class NumericValue implements
		NumericData
{
	private final double value;

	/**
	 * Constructor used to create a new NumericValue object
	 * 
	 * @param value
	 *            the particular numeric value
	 */
	public NumericValue(
			final double value ) {
		this.value = value;
	}

	/**
	 * 
	 * @return value the value of a numeric value object
	 */
	@Override
	public double getMin() {
		return value;
	}

	/**
	 * 
	 * @return value the value of a numeric value object
	 */
	@Override
	public double getMax() {
		return value;
	}

	/**
	 * 
	 * @return value the value of a numeric value object
	 */
	@Override
	public double getCentroid() {
		return value;
	}

	/**
	 * Determines if this object is a range or not
	 */
	@Override
	public boolean isRange() {
		return false;
	}

	@Override
	public String toString() {
		return "NumericRange [value=" + value + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(value);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	protected static final double EPSILON = 1E-10;

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		NumericValue other = (NumericValue) obj;
		return (Math.abs(value - other.value) < EPSILON);
	}
}
