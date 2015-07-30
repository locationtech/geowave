package mil.nga.giat.geowave.core.index.simple;

import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;

/**
 * A simple 1-dimensional NumericIndexStrategy that represents an index of
 * signed integer values. The strategy doesn't use any binning. The ids are
 * simply the byte arrays of the value. This index strategy will not perform
 * well for inserting ranges because there will be too much replication of data.
 * 
 */
public class SimpleIntegerIndexStrategy extends
		SimpleNumericIndexStrategy<Integer>
{

	public SimpleIntegerIndexStrategy() {
		super(
				Lexicoders.INT);
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

	@Override
	protected Integer cast(
			final double value ) {
		return (int) value;
	}

}
