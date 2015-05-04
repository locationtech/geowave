package mil.nga.giat.geowave.core.index.sfc;

import mil.nga.giat.geowave.core.index.ByteArrayRange;

/***
 * This class encapsulates a set of ranges returned from a space filling curve
 * decomposition.
 * 
 */
public class RangeDecomposition
{
	private final ByteArrayRange[] ranges;

	/**
	 * Constructor used to create a new Range Decomposition object.
	 * 
	 * @param ranges
	 *            ranges for the space filling curve
	 */
	public RangeDecomposition(
			final ByteArrayRange[] ranges ) {
		this.ranges = ranges;
	}

	/**
	 * 
	 * @return the ranges associated with this Range Decomposition
	 */
	public ByteArrayRange[] getRanges() {
		return ranges;
	}
}
