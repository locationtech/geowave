package mil.nga.giat.geowave.core.index;

import java.util.List;

import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

/**
 * Interface which defines a numeric index strategy.
 * 
 */
public interface NumericIndexStrategy extends
		Persistable
{
	/**
	 * Returns a list of query ranges for an specified numeric range.
	 * 
	 * @param indexedRange
	 *            defines the numeric range for the query
	 * @return a List of query ranges
	 */
	public List<ByteArrayRange> getQueryRanges(
			MultiDimensionalNumericData indexedRange );

	/**
	 * Returns a list of query ranges for an specified numeric range.
	 * 
	 * @param indexedRange
	 *            defines the numeric range for the query
	 * @param maxRangeDecomposition
	 *            the maximum number of ranges provided by a single query
	 *            decomposition, this is a best attempt and not a guarantee
	 * @return a List of query ranges
	 */
	public List<ByteArrayRange> getQueryRanges(
			MultiDimensionalNumericData indexedRange,
			int maxEstimatedRangeDecomposition );

	/**
	 * Returns a list of id's for insertion. The index strategy will use a
	 * reasonable default for the maximum duplication of insertion IDs
	 * 
	 * @param indexedData
	 *            defines the numeric data to be indexed
	 * @return a List of insertion ID's
	 */
	public List<ByteArrayId> getInsertionIds(
			MultiDimensionalNumericData indexedData );

	/**
	 * Returns a list of id's for insertion.
	 * 
	 * @param indexedData
	 *            defines the numeric data to be indexed
	 * @param maxDuplicateInsertionIds
	 *            defines the maximum number of insertion IDs that can be used,
	 *            this is a best attempt and not a guarantee
	 * @return a List of insertion ID's
	 */
	public List<ByteArrayId> getInsertionIds(
			MultiDimensionalNumericData indexedData,
			int maxEstimatedDuplicateIds );

	/**
	 * Returns the range that the given ID represents
	 * 
	 * @param insertionId
	 *            the insertion ID to determine a range for
	 * @return the range that the given insertion ID represents, inclusive on
	 *         the start and exclusive on the end for the range
	 */
	public MultiDimensionalNumericData getRangeForId(
			ByteArrayId insertionId );

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

	/**
	 * 
	 * @return a unique ID associated with the index strategy
	 */
	public String getId();

	/***
	 * Get the range/size of a single insertion ID for each dimension at the
	 * highest precision supported by this index strategy
	 * 
	 * @return the range of a single insertion ID for each dimension
	 */
	public double[] getHighestPrecisionIdRangePerDimension();
}
