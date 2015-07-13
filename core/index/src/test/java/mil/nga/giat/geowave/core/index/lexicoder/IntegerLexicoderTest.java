package mil.nga.giat.geowave.core.index.lexicoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.primitives.UnsignedBytes;

public class IntegerLexicoderTest
{
	private final IntegerLexicoder integerLexicoder = Lexicoders.INT;

	@Test
	public void testRanges() {
		Assert.assertTrue(integerLexicoder.getMinimumValue().equals(
				Integer.MIN_VALUE));
		Assert.assertTrue(integerLexicoder.getMaximumValue().equals(
				Integer.MAX_VALUE));
	}

	@Test
	public void testSortOrder() {
		final List<Integer> values = Arrays.asList(
				-10,
				Integer.MIN_VALUE,
				2678,
				Integer.MAX_VALUE,
				0);
		final List<byte[]> byteArrays = new ArrayList<>(
				values.size());
		for (final int value : values) {
			byteArrays.add(integerLexicoder.toByteArray(value));
		}
		Collections.sort(
				byteArrays,
				UnsignedBytes.lexicographicalComparator());
		Collections.sort(values);
		final List<Integer> convertedBytes = new ArrayList<>(
				byteArrays.size());
		for (final byte[] bytes : byteArrays) {
			convertedBytes.add(integerLexicoder.fromByteArray(bytes));
		}
		Assert.assertTrue(values.equals(convertedBytes));
	}

}
