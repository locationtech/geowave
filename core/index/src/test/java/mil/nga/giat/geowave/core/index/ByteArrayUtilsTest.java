package mil.nga.giat.geowave.core.index;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

public class ByteArrayUtilsTest
{

	@Test
	public void testSplit() {
		final ByteArrayId first = new ByteArrayId(
				"first");
		final ByteArrayId second = new ByteArrayId(
				"second");
		final byte[] combined = ByteArrayUtils.combineVariableLengthArrays(
				first.getBytes(),
				second.getBytes());
		final Pair<byte[], byte[]> split = ByteArrayUtils.splitVariableLengthArrays(combined);
		Assert.assertArrayEquals(
				first.getBytes(),
				split.getLeft());
		Assert.assertArrayEquals(
				second.getBytes(),
				split.getRight());
	}
}
