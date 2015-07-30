package mil.nga.giat.geowave.core.index.lexicoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.primitives.UnsignedBytes;

public class ShortLexicoderTest
{
	private final ShortLexicoder shortLexicoder = Lexicoders.SHORT;

	@Test
	public void testRanges() {
		Assert.assertTrue(shortLexicoder.getMinimumValue().equals(
				Short.MIN_VALUE));
		Assert.assertTrue(shortLexicoder.getMaximumValue().equals(
				Short.MAX_VALUE));
	}

	@Test
	public void testSortOrder() {
		final List<Short> values = Arrays.asList(
				(short) -10,
				Short.MIN_VALUE,
				(short) 2678,
				Short.MAX_VALUE,
				(short) 0);
		final List<byte[]> byteArrays = new ArrayList<>(
				values.size());
		for (final short value : values) {
			byteArrays.add(shortLexicoder.toByteArray(value));
		}
		Collections.sort(
				byteArrays,
				UnsignedBytes.lexicographicalComparator());
		Collections.sort(values);
		final List<Short> convertedBytes = new ArrayList<>(
				byteArrays.size());
		for (final byte[] bytes : byteArrays) {
			convertedBytes.add(shortLexicoder.fromByteArray(bytes));
		}
		Assert.assertTrue(values.equals(convertedBytes));
	}
}
