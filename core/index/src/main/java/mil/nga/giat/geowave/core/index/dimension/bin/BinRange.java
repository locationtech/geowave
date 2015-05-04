package mil.nga.giat.geowave.core.index.dimension.bin;

/**
 * The Bin Range class is used to define the specific bins or ranges of a
 * particular Binning Strategy.
 * 
 */
public class BinRange
{
	private final byte[] binId;
	private final double normalizedMin;
	private final double normalizedMax;
	private final boolean fullExtent;

	/**
	 * Constructor used to create a new BinRange object with a only a minimum
	 * and maximum value.
	 * 
	 * @param normalizedMin
	 *            the lower bounds of the bin
	 * @param normalizedMax
	 *            the upper bounds of the bin
	 */
	public BinRange(
			final double normalizedMin,
			final double normalizedMax ) {
		this(
				null,
				normalizedMin,
				normalizedMax,
				false);
	}

	/**
	 * Constructor used to create a new BinRange object.
	 * 
	 * has a minimum and maximum value.
	 * 
	 * @param binId
	 *            a unique ID associated with the bin
	 * @param normalizedMin
	 *            the lower bounds of the bin
	 * @param normalizedMax
	 *            the upper bounds of the bin
	 * @param fullExtent
	 *            flag to indicate whether this is the full bin
	 */
	public BinRange(
			final byte[] binId,
			final double normalizedMin,
			final double normalizedMax,
			final boolean fullExtent ) {
		this.binId = binId;
		this.normalizedMin = normalizedMin;
		this.normalizedMax = normalizedMax;
		this.fullExtent = fullExtent;
	}

	/**
	 * 
	 * @return a unique ID associate with this particular bin
	 */
	public byte[] getBinId() {
		return binId;
	}

	/**
	 * 
	 * @return the lower bounds of this bin
	 */
	public double getNormalizedMin() {
		return normalizedMin;
	}

	/**
	 * 
	 * @return the upper bounds of this bin
	 */
	public double getNormalizedMax() {
		return normalizedMax;
	}

	/**
	 * 
	 * @return flag to indicate whether this is the entire bin extent
	 */
	public boolean isFullExtent() {
		return fullExtent;
	}

}
