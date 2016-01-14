package mil.nga.giat.geowave.core.index;

import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

/**
 * Interface which defines a numeric index strategy.
 * 
 */
public interface NumericIndexStrategy extends
		IndexStrategy<MultiDimensionalNumericData, MultiDimensionalNumericData>
{

	/**
	 * Return an integer coordinate in each dimension for the given insertion ID
	 * 
	 * @param insertionId
	 *            the insertion ID to determine the coordinates for
	 * @return the integer coordinate that the given insertion ID represents
	 */
	public long[] getCoordinatesPerDimension(
			ByteArrayId insertionId );

	/**
	 * Returns an array of dimension definitions that defines this index
	 * strategy, the array is in the order that is expected within
	 * multidimensional numeric data that is passed to this index strategy
	 * 
	 * @return the ordered array of dimension definitions that represents this
	 *         index strategy
	 */
	public NumericDimensionDefinition[] getOrderedDimensionDefinitions();

	/***
	 * Get the range/size of a single insertion ID for each dimension at the
	 * highest precision supported by this index strategy
	 * 
	 * @return the range of a single insertion ID for each dimension
	 */
	public double[] getHighestPrecisionIdRangePerDimension();

}
