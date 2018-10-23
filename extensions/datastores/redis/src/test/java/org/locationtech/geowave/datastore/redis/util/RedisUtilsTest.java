package org.locationtech.geowave.datastore.redis.util;

import static org.junit.Assert.assertEquals;

import java.util.BitSet;
import java.util.Random;

import org.bouncycastle.util.Arrays;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArray;

public class RedisUtilsTest
{
	private static final int NUM_ITERATIONS = 10000;
	private static final int NUM_TRANSFORMS = 5;
	private static final long SEED = 2894647323275155231L;

	@Test
	public void testSortKeyTransform() {
		final Random rand = new Random(
				SEED);
		for (int i = 0; i < NUM_ITERATIONS; i++) {
			// generate random long values representative of 64-bit sort keys
			long val = rand.nextLong();
			final BitSet set = BitSet.valueOf(new long[] {
				val
			});
			// clear the first 12 bits of the random long because we truly only
			// get 52-bits of precision (the mantissa) within the IEEE 754 spec
			// which is what we have to work with for Redis Z-Scores
			for (int a = 0; a < 12; a++) {
				set.clear(a);
			}
			val = set.toLongArray()[0];
			// now we have long randomly representing the 52-bits of precision
			// we can work with within a z-score, let's cast to double and make
			// sure we can go back and forth between z-score and sort key
			final double originalScore = val;
			final byte[] originalSortKey = RedisUtils.getSortKey(originalScore);
			assertRepeatedTransform(
					originalScore,
					originalSortKey);
			// now check that it still maintains consistency for lower length
			// sort keys
			for (int length = originalSortKey.length - 1; length >= 0; length--) {

				final byte[] newOriginalSortKey = Arrays.copyOf(
						originalSortKey,
						length);
				final double newOriginalScore = RedisUtils.getScore(newOriginalSortKey);
				assertRepeatedTransform(
						newOriginalScore,
						newOriginalSortKey);
			}
		}
	}

	private static void assertRepeatedTransform(
			final double originalScore,
			byte[] originalSortKey ) {
		// we try to remove trailing 0's
		int i = originalSortKey.length;
		while ((i > 0) && (originalSortKey[--i] == 0)) {
			originalSortKey = Arrays.copyOf(
					originalSortKey,
					originalSortKey.length - 1);
		}
		byte[] currentSortKey = originalSortKey;
		double currentScore = originalScore;
		for (int j = 0; j < NUM_TRANSFORMS; j++) {
			// hypothetically going back and forth one time should be a
			// sufficient check but for sanity's sake go back and forth
			// several times
			currentScore = RedisUtils.getScore(currentSortKey);
			currentSortKey = RedisUtils.getSortKey(currentScore);
			assertEquals(
					originalScore,
					currentScore,
					0);
			Assert.assertEquals(
					"transformation " + j + " failed. Current key '" + new ByteArray(
							currentSortKey).getHexString() + "' differs from original '" + new ByteArray(
							originalSortKey).getHexString() + "'",
					new ByteArray(
							originalSortKey),
					new ByteArray(
							currentSortKey));
		}
	}

}
