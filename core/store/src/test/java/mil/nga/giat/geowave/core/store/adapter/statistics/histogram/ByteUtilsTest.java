package mil.nga.giat.geowave.core.store.adapter.statistics.histogram;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

public class ByteUtilsTest
{
	@Test
	public void test() {

		double oneTwo = ByteUtils.toDouble("12".getBytes());
		double oneOneTwo = ByteUtils.toDouble("112".getBytes());
		double oneThree = ByteUtils.toDouble("13".getBytes());
		double oneOneThree = ByteUtils.toDouble("113".getBytes());
		assertTrue(oneTwo > oneOneTwo);
		assertTrue(oneThree > oneTwo);
		assertTrue(oneOneTwo < oneOneThree);
		assertTrue(Arrays.equals(
				ByteUtils.toPaddedBytes("113".getBytes()),
				ByteUtils.toBytes(oneOneThree)));
	}
}
