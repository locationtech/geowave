package mil.nga.giat.geowave.core.index.sfc.data;

import mil.nga.giat.geowave.core.index.QueryConstraints;

/**
 * Interface which defines the methods associated with a multi-dimensional
 * numeric data range.
 * 
 */
public interface MultiDimensionalNumericData extends
		QueryConstraints
{
	/**
	 * @return an array of object QueryRange
	 */
	public NumericData[] getDataPerDimension();

	public double[] getMaxValuesPerDimension();

	public double[] getMinValuesPerDimension();

	public double[] getCentroidPerDimension();
}
