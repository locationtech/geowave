package mil.nga.giat.geowave.core.index.lexicoder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.primitives.SignedBytes;

public class DoubleLexicoderTest
{
	private final DoubleLexicoder doubleLexicoder = Lexicoders.DOUBLE;

	@Test
	public void testRanges() {
		Assert.assertTrue(doubleLexicoder.getMinimumValue().equals(
				Double.MIN_VALUE));
		Assert.assertTrue(doubleLexicoder.getMaximumValue().equals(
				Double.MAX_VALUE));
	}

	@Test
	public void testSortOrder() {
		final List<Double> doubleList = Arrays.asList(
				-10d,
				Double.MIN_VALUE,
				11d,
				-14.2,
				14.2,
				-100.002,
				100.002,
				-11d,
				Double.MAX_VALUE,
				0d);
		final Map<byte[], Double> sortedByteArrayToDoubleMappings = new TreeMap<>(
				SignedBytes.lexicographicalComparator());
		for (final Double d : doubleList) {
			sortedByteArrayToDoubleMappings.put(
					doubleLexicoder.toByteArray(d),
					d);
		}
		Collections.sort(doubleList);
		int idx = 0;
		final Set<byte[]> sortedByteArrays = sortedByteArrayToDoubleMappings.keySet();
		for (final byte[] byteArray : sortedByteArrays) {
			final Double doubleValue = sortedByteArrayToDoubleMappings.get(byteArray);
			Assert.assertTrue(doubleValue.equals(doubleList.get(idx++)));
		}
	}
}
