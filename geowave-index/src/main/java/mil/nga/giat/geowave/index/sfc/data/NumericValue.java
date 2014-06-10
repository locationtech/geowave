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
}
