package mil.nga.giat.geowave.core.index.sfc;

import java.math.BigInteger;

import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

/***
 * Base class which defines common methods for any space filling curve. Hosts
 * standard access methods shared between implementation. A space filling curve
 * is expected to provide a reversible n-dimensional <-> 1-dimensional mapping.
 *
 */
public interface SpaceFillingCurve extends
		Persistable
{
	/***
	 * Maps a n-dimensional value to a single dimension, i.e. [12,33] -> 0033423
	 *
	 * @param values
	 *            n-dimensional value to be encoded in the SFC. The size of
	 *            value corresponds to the number of dimensions
	 * @return value derived from the the SFC transform. The value is left
	 *         padded based on the number if bits in the SFC dimension
	 */
	public byte[] getId(
			double[] values );

	/***
	 * Gets n-dimensional ranges from a single dimension, i.e. 0033423 ->
	 * [12,33]
	 *
	 * @param id
	 *            the SFC ID to calculate the ranges of values represented.
	 * @return the valid ranges per dimension of a single SFC ID derived from
	 *         the the SFC transform.
	 */
	public MultiDimensionalNumericData getRanges(
			byte[] id );

	/***
	 * Gets n-dimensional coordinates from a single dimension
	 *
	 * @param id
	 *            the SFC ID to calculate the coordinates for each dimension.
	 * @return the coordinate in each dimension for the given ID
	 */
	public long[] getCoordinates(
			byte[] id );

	/***
	 * Returns a collection of ranges on the 1-d space filling curve that
	 * correspond to the n-dimensional range described in the query parameter.
	 *
	 * This method will decompose the range all the way down to the unit
	 * interval of 1.
	 *
	 * @param query
	 *            describes the n-dimensional query window that will be
	 *            decomposed
	 * @return an object containing the ranges on the SFC that overlap the
	 *         parameters supplied in the query object
	 *
	 */
	public RangeDecomposition decomposeRangeFully(
			MultiDimensionalNumericData query );

	/***
	 * Returns a collection of ranges on the 1-d space filling curve that
	 * correspond to the n-dimensional range described in the query parameter.
	 *
	 * This method will roll up the ranges based on the maxRanges parameter.
	 * Ranges will be "connected" based on the minimization of distance between
	 * the end of one range and the start of the next.
	 *
	 * @param query
	 *            describes the n-dimensional query window that will be
	 *            decomposed
	 * @return an object containing the ranges on the SFC that overlap the
	 *         parameters supplied in the query object
	 *
	 */
	public RangeDecomposition decomposeRange(
			MultiDimensionalNumericData query,
			boolean overInclusiveOnEdge,
			int maxRanges );

	/***
	 * Determines the estimated number of rows a multi-dimensional range will
	 * span within this space filling curve
	 *
	 * @param query
	 *            describes the n-dimensional range to estimate the row count
	 *            for
	 * @return an estimate of the row count for the ranges given within this
	 *         space filling curve
	 *
	 */
	public BigInteger getEstimatedIdCount(
			MultiDimensionalNumericData data );

	/***
	 * Determines the coordinates within this space filling curve for a
	 * dimension given a range
	 *
	 * @param minValue
	 *            describes the minimum of a range in a single dimension used to
	 *            determine the SFC coordinate range
	 * @param maxValue
	 *            describes the maximum of a range in a single dimension used to
	 *            determine the SFC coordinate range
	 * @param dimension
	 *            the dimension
	 * @return the range of coordinates as an array where the first element is
	 *         the min and the second element is the max
	 *
	 */
	public long[] normalizeRange(
			double minValue,
			double maxValue,
			int dimension );

	/***
	 * Get the range/size of a single insertion ID for each dimension
	 *
	 * @return the range of a single insertion ID for each dimension
	 */
	public double[] getInsertionIdRangePerDimension();
}
