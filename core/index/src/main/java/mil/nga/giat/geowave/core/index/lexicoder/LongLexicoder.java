package mil.nga.giat.geowave.core.index.lexicoder;

import com.google.common.primitives.Longs;

/**
 * A lexicoder for signed integers (in the range from Long.MIN_VALUE to
 * Long.MAX_VALUE). Does an exclusive or on the most significant bit to invert
 * the sign, so that lexicographic ordering of the byte arrays matches the
 * natural order of the numbers.
 * 
 * See Apache Accumulo (org.apache.accumulo.core.client.lexicoder.LongLexicoder)
 */
public class LongLexicoder implements
		NumberLexicoder<Long>
{

	protected LongLexicoder() {}

	@Override
	public byte[] toByteArray(
			final Long value ) {
		return Longs.toByteArray(value ^ 0x8000000000000000l);
	}

	@Override
	public Long fromByteArray(
			final byte[] bytes ) {
		final long value = Longs.fromByteArray(bytes);
		return value ^ 0x8000000000000000l;
	}

	@Override
	public Long getMinimumValue() {
		return Long.MIN_VALUE;
	}

	@Override
	public Long getMaximumValue() {
		return Long.MAX_VALUE;
	}

}
