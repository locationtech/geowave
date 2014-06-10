package mil.nga.giat.geowave.index.sfc.data;

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

}
