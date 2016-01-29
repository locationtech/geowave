package mil.nga.giat.geowave.core.index.lexicoder;

/**
 * A lexicoder for preserving the native Java sort order of Double values.
 * 
 */
public class DoubleLexicoder implements
		NumberLexicoder<Double>
{

	@Override
	public byte[] toByteArray(
			final Double value ) {
		long l = Double.doubleToRawLongBits(value);
		if (l < 0) {
			l = ~l;
		}
		else {
			l = l ^ 0x8000000000000000l;
		}
		return Lexicoders.LONG.toByteArray(l);
	}

	@Override
	public Double fromByteArray(
			final byte[] bytes ) {
		long l = Lexicoders.LONG.fromByteArray(bytes);
		if (l < 0) {
			l = l ^ 0x8000000000000000l;
		}
		else {
			l = ~l;
		}
		return Double.longBitsToDouble(l);
	}

	@Override
	public Double getMinimumValue() {
		return Double.MIN_VALUE;
	}

	@Override
	public Double getMaximumValue() {
		return Double.MAX_VALUE;
	}

}
