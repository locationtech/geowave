package mil.nga.giat.geowave.core.index.lexicoder;

import com.google.common.primitives.Ints;

/**
 * A lexicoder for signed integers (in the range from Integer.MIN_VALUE to
 * Integer.MAX_VALUE). Does an exclusive or on the most significant bit to
 * invert the sign, so that lexicographic ordering of the byte arrays matches
 * the natural order of the numbers.
 * 
 * See Apache Accumulo
 * (org.apache.accumulo.core.client.lexicoder.IntegerLexicoder)
 */
public class IntegerLexicoder implements
		NumberLexicoder<Integer>
{

	protected IntegerLexicoder() {}

	@Override
	public byte[] toByteArray(
			final Integer value ) {
		return Ints.toByteArray(value ^ 0x80000000);
	}

	@Override
	public Integer fromByteArray(
			final byte[] bytes ) {
		final int value = Ints.fromByteArray(bytes);
		return value ^ 0x80000000;
	}

	@Override
	public Integer getMinimumValue() {
		return Integer.MIN_VALUE;
	}

	@Override
	public Integer getMaximumValue() {
		return Integer.MAX_VALUE;
	}

}
