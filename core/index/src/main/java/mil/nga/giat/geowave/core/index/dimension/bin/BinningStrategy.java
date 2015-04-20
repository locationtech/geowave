package mil.nga.giat.geowave.core.index.dimension.bin;

import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;

/**
 * This interface enables a dimension to define a methodology for applying bins
 * to a full set of values which can be used by a general purpose space filling
 * curve implementation.
 */
public interface BinningStrategy extends
		Persistable
{
	/**
	 * 
	 * @return the minimum value to be used by the space filling curve
	 *         implementation
	 */
	public double getBinMin();

	/**
	 * 
	 * @return the maximum value to be used by the space filling curve
	 *         implementation
	 */
	public double getBinMax();

	/**
	 * Returns a normalized value (confined to the normalized minimum and
	 * maximum of this binning strategy) and a bin from any value within the
	 * data set
	 * 
	 * @param value
	 *            the value that needs to be normalized and binned
	 * @return the normalized value to be used by a space filling curve
	 *         implementation, and the bin
	 */
	public BinValue getBinnedValue(
			double value );

	/**
	 * Return a set of normalized ranges (each of which are confined to the
	 * normalized min and max of this binning strategy) with a bin for each of
	 * the ranges. If the passed in query range crosses multiple bins, a
	 * BinRange for each bin that it intersects will be returned, but if it is
	 * wholly contained within a single bin then a single BinRange will be
	 * returned
	 * 
	 * @param index
	 *            the data representing the query range that needs to be
	 *            normalized and binned
	 * @return the set of all corresponding bins and ranges that the passed in
	 *         query range intersects
	 */
	public BinRange[] getNormalizedRanges(
			NumericData index );

	/**
	 * Given a set of normalized ranges (each of which are confined to the
	 * normalized min and max of this binning strategy) with a bin for each of
	 * the ranges, this will calculate the original unbinned range.
	 * 
	 * @param index
	 *            the normalized and binned range
	 * @return the original query range represented by the normalized and binned
	 *         range
	 */
	public NumericRange getDenormalizedRanges(
			BinRange binnedRange );

	/**
	 * Return the fixed size for the bin ID used by this binning strategy
	 * 
	 * @return the length of the bin ID
	 */
	public int getFixedBinIdSize();
}
