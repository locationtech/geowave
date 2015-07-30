package mil.nga.giat.geowave.core.index.lexicoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.primitives.UnsignedBytes;

public class LongLexicoderTest
{
	private final LongLexicoder longLexicoder = Lexicoders.LONG;

	@Test
	public void testRanges() {
		Assert.assertTrue(longLexicoder.getMinimumValue().equals(
				Long.MIN_VALUE));
		Assert.assertTrue(longLexicoder.getMaximumValue().equals(
				Long.MAX_VALUE));
	}

	@Test
	public void testSortOrder() {
		final List<Long> values = Arrays.asList(
				-10l,
				Long.MIN_VALUE,
				2678l,
				Long.MAX_VALUE,
				0l);
		final List<byte[]> byteArrays = new ArrayList<>(
				values.size());
		for (final long value : values) {
			byteArrays.add(longLexicoder.toByteArray(value));
		}
		Collections.sort(
				byteArrays,
				UnsignedBytes.lexicographicalComparator());
		Collections.sort(values);
		final List<Long> convertedBytes = new ArrayList<>(
				byteArrays.size());
		for (final byte[] bytes : byteArrays) {
			convertedBytes.add(longLexicoder.fromByteArray(bytes));
		}
		Assert.assertTrue(values.equals(convertedBytes));
	}
}
