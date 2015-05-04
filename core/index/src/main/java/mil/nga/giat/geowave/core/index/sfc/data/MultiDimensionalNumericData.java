package mil.nga.giat.geowave.core.index.sfc.data;

/**
 * Interface which defines the methods associated with a multi-dimensional
 * numeric data range.
 * 
 */
public interface MultiDimensionalNumericData
{
	/**
	 * @return an array of object QueryRange
	 */
	public NumericData[] getDataPerDimension();

	public double[] getMaxValuesPerDimension();

	public double[] getMinValuesPerDimension();

	public double[] getCentroidPerDimension();

	public int getDimensionCount();

	/**
	 * Unconstrained?
	 * 
	 * @return return if unconstrained on a dimension
	 */
	public boolean isEmpty();
}
