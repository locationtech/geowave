package mil.nga.giat.geowave.datastore.accumulo.util;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.data.PersistentValue;

/**
 * Utility methods when dealing with bitmasks in Accumulo
 * 
 * @since 0.9.1
 */
public class BitmaskUtils
{

	/**
	 * Generates a bitmask with the bit at the given index set to true
	 * 
	 * @param index
	 *            0-based index of bit to set to true
	 * @return the bitmask
	 */
	public static byte[] generateBitmask(
			final int index ) {
		final BitSet bitSet = new BitSet();
		bitSet.set(index);
		return bitSet.toByteArray();
	}

	/**
	 * Returns the ordinal (integer) this bitmask represents
	 * 
	 * It is assumed this method is given a non-composite 
	 * bitmask, that is a bitmask with only a single bit set to true
	 * 
	 * @param bitmask
	 *            the bitmask
	 * @return the ordinal
	 */
	public static int getOrdinal(
			final byte[] bitmask ) {
		final BitSet bitSet = BitSet.valueOf(bitmask);
		return bitSet.nextSetBit(0);
	}

	/**
	 * Generates a composite bitmask given a list of bitmasks. The composite
	 * bitmask represents the logical OR of all the given bitmasks
	 * 
	 * For example, given 00000001, 00000010, and 00000100 this method will
	 * return 00000111
	 * 
	 * @param bitmasks
	 *            a list of bitmasks
	 * @return a composite bitmask
	 */
	public static byte[] generateCompositeBitmask(
			final List<byte[]> bitmasks ) {
		final BitSet composite = new BitSet();
		for (final byte[] bitmaskBytes : bitmasks) {
			final BitSet bitmask = BitSet.valueOf(bitmaskBytes);
			composite.or(bitmask);
		}
		return composite.toByteArray();
	}

	/**
	 * Iterates the set (true) bits within the given composite bitmask and
	 * generates a list of individual bitmasks. The XOR of each bitmask in the
	 * resultant list equals the original composite bitmask.
	 * 
	 * @param compositeBitmask
	 *            the composite bitmask
	 * @return a list of bitmasks
	 */
	public static List<byte[]> getBitmasks(
			final byte[] compositeBitmask ) {
		final List<byte[]> bitmasks = new ArrayList<>();
		final BitSet bitSet = BitSet.valueOf(compositeBitmask);
		for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
			bitmasks.add(generateBitmask(i));
		}
		return bitmasks;
	}

	/**
	 * Creates a clone of the given FieldInfo using the given bitmask to
	 * overwrite the original dataValue id
	 * 
	 * @param fieldInfo
	 *            fieldInfo to clone
	 * @param bitmask
	 *            bitmask to use to overwrite dataValue id value
	 * @return new FieldInfo with bitmask as dataValue id
	 */
	public static FieldInfo<Object> transformField(
			final FieldInfo<?> fieldInfo,
			final byte[] bitmask ) {
		return new FieldInfo<Object>(
				new PersistentValue<Object>(
						new ByteArrayId(
								bitmask),
						fieldInfo.getDataValue().getValue()),
				fieldInfo.getWrittenValue(),
				fieldInfo.getVisibility());
	}

}