package mil.nga.giat.geowave.core.store.flatten;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Utility methods when dealing with bitmasks in Accumulo
 * 
 * @since 0.9.1
 */
public class BitmaskUtils
{
	public static byte[] generateANDBitmask(
			final byte[] bitmask1,
			final byte[] bitmask2 ) {
		final byte[] result = new byte[Math.min(
				bitmask1.length,
				bitmask2.length)];
		for (int i = 0; i < result.length; i++) {
			result[i] = bitmask1[i];
			result[i] &= bitmask2[i];
		}
		return result;
	}

	public static boolean isAnyBitSet(
			final byte[] array ) {
		for (byte b : array) {
			if (b != 0) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Generates a composite bitmask given a list of field positions. The
	 * composite bitmask represents a true bit for every positive field position
	 * 
	 * For example, given field 0, field 1, and field 2 this method will return
	 * 00000111
	 * 
	 * @param fieldPositions
	 *            a list of field positions
	 * @return a composite bitmask
	 */
	public static byte[] generateCompositeBitmask(
			final SortedSet<Integer> fieldPositions ) {
		final byte[] retVal = new byte[(fieldPositions.last() / 8) + 1];
		for (final Integer fieldPosition : fieldPositions) {
			final int bytePosition = fieldPosition / 8;
			final int bitPosition = fieldPosition % 8;
			retVal[bytePosition] |= (1 << bitPosition);
		}
		return retVal;
	}

	/**
	 * Generates a composite bitmask given a single field position. The
	 * composite bitmask represents a true bit for this field position
	 * 
	 * For example, given field 2 this method will return 00000100
	 * 
	 * @param fieldPosition
	 *            a field position
	 * @return a composite bitmask
	 */
	public static byte[] generateCompositeBitmask(
			final Integer fieldPosition ) {
		return generateCompositeBitmask(new TreeSet<Integer>(
				Collections.singleton(fieldPosition)));
	}

	/**
	 * Iterates the set (true) bits within the given composite bitmask and
	 * generates a list of field positions.
	 * 
	 * @param compositeBitmask
	 *            the composite bitmask
	 * @return a list of field positions
	 */
	public static List<Integer> getFieldPositions(
			final byte[] bitmask ) {
		final List<Integer> fieldPositions = new ArrayList<>();
		int currentByte = 0;
		for (final byte singleByteBitMask : bitmask) {
			for (int bit = 0; bit < 8; ++bit) {
				if (((singleByteBitMask >>> bit) & 0x1) == 1) {
					fieldPositions.add((currentByte * 8) + bit);
				}
			}
			currentByte++;
		}
		return fieldPositions;
	}

}