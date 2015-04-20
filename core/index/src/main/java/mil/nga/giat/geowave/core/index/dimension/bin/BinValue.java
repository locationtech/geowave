package mil.nga.giat.geowave.core.index.dimension.bin;

/**
 * The Bin Value class is used to define the specific bins of a particular
 * Binning Strategy.
 * 
 */
public class BinValue
{
	private final byte[] binId;
	private final double normalizedValue;

	/**
	 * Constructor used to create a new BinValue object based upon a normalized
	 * value
	 * 
	 * @param normalizedValue
	 *            the incoming value to be binned
	 */
	public BinValue(
			final double normalizedValue ) {
		this(
				null,
				normalizedValue);
	}

	/**
	 * Constructor used to create a new BinValue object based upon a unique bin
	 * ID and normalized value
	 * 
	 * @param binId
	 *            a unique ID to associate with this Bin Value
	 * @param normalizedValue
	 *            the incoming value to be binned
	 */
	public BinValue(
			final byte[] binId,
			final double normalizedValue ) {
		this.binId = binId;
		this.normalizedValue = normalizedValue;
	}

	/**
	 * 
	 * @return a unique ID associated with this Bin Value
	 */
	public byte[] getBinId() {
		return binId;
	}

	/**
	 * 
	 * @return the normalized value of this particular Bin Value
	 */
	public double getNormalizedValue() {
		return normalizedValue;
	}

}
