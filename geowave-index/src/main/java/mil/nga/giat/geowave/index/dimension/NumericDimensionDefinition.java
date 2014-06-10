package mil.nga.giat.geowave.index.dimension;

import mil.nga.giat.geowave.index.Persistable;
import mil.nga.giat.geowave.index.dimension.bin.BinRange;
import mil.nga.giat.geowave.index.sfc.data.NumericData;

/**
 * The Numeric Dimension Definition interface defines the attributes and methods
 * of a class which forms the Space Filling Curve dimension.
 * 
 */
public interface NumericDimensionDefinition extends
		Persistable
{
	/**
	 * Used to normalize the numeric data set into the bounds of the range
	 * 
	 * @return normalized value
	 */
	public double normalize(
			double value );

	/**
	 * Returns the set of normalized ranges
	 * 
	 * @param range  a numeric range of the data set
	 * @return an array of BinRange[] objects
	 */
	public BinRange[] getNormalizedRanges(
			NumericData range );
}
