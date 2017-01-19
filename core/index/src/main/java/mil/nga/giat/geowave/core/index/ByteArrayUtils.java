package mil.nga.giat.geowave.core.index;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;

/**
 * Convenience methods for converting binary data to and from strings. The
 * encoding and decoding is done in base-64. These methods should be used for
 * converting data that is binary in nature to a String representation for
 * transport. Use StringUtils for serializing and deserializing text-based data.
 * 
 * Additionally, this class has methods for manipulating byte arrays, such as
 * combining or incrementing them.
 */
public class ByteArrayUtils
{

	private static byte[] internalCombineArrays(
			final byte[] beginning,
			final byte[] end ) {
		final byte[] combined = new byte[beginning.length + end.length];
		System.arraycopy(
				beginning,
				0,
				combined,
				0,
				beginning.length);
		System.arraycopy(
				end,
				0,
				combined,
				beginning.length,
				end.length);
		return combined;
	}

	/**
	 * Convert binary data to a string for transport
	 * 
	 * @param byteArray
	 *            the binary data
	 * @return the base64url encoded string
	 */
	public static String byteArrayToString(
			final byte[] byteArray ) {
		return BaseEncoding.base64Url().encode(
				byteArray);
	}

	/**
	 * Convert a string representation of binary data back to a String
	 * 
	 * @param str
	 *            the string representation of binary data
	 * @return the base64url decoded binary data
	 */
	public static byte[] byteArrayFromString(
			final String str ) {
		return BaseEncoding.base64Url().decode(
				str);
	}

	/**
	 * Combine 2 arrays into one large array. If both are not null it will
	 * append id2 to id1 and the result will be of length id1.length +
	 * id2.length
	 * 
	 * @param id1
	 *            the first byte array to use (the start of the result)
	 * @param id2
	 *            the second byte array to combine (appended to id1)
	 * @return the concatenated byte array
	 */
	public static byte[] combineArrays(
			final byte[] id1,
			final byte[] id2 ) {
		byte[] combinedId;
		if (id1 == null || id1.length == 0) {
			combinedId = id2;
		}
		else if (id2 == null || id2.length == 0) {
			combinedId = id1;
		}
		else {
			// concatenate bin ID 2 to the end of bin ID 1
			combinedId = ByteArrayUtils.internalCombineArrays(
					id1,
					id2);
		}
		return combinedId;
	}

	/**
	 * add 1 to the least significant bit in this byte array (the last byte in
	 * the array)
	 * 
	 * @param value
	 *            the array to increment
	 * @return will return true as long as the value did not overflow
	 */
	public static boolean increment(
			final byte[] value ) {
		for (int i = value.length - 1; i >= 0; i--) {
			value[i]++;
			if (value[i] != 0) {
				return true;
			}
		}
		return value[0] != 0;
	}

	/**
	 * Converts a UUID to a byte array
	 * 
	 * @param uuid
	 *            the uuid
	 * @return the byte array representing that UUID
	 */
	public static byte[] uuidToByteArray(
			final UUID uuid ) {
		final ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
		bb.putLong(uuid.getMostSignificantBits());
		bb.putLong(uuid.getLeastSignificantBits());
		return bb.array();
	}

	/**
	 * Converts a long to a byte array
	 * 
	 * @param l
	 *            the long
	 * @return the byte array representing that long
	 */
	public static byte[] longToByteArray(
			final long l ) {
		ByteBuffer bb = ByteBuffer.allocate(Long.BYTES);
		bb.putLong(l);
		return bb.array();
	}

	/**
	 * Converts a byte array to a long
	 * 
	 * @param bytes
	 *            the byte array the long
	 * @return the long represented by the byte array
	 */
	public static long byteArrayToLong(
			byte[] bytes ) {
		ByteBuffer bb = ByteBuffer.allocate(Long.BYTES);
		bb.put(bytes);
		bb.flip();
		return bb.getLong();
	}

	/**
	 * Combines two variable length byte arrays into one large byte array and
	 * appends the length of each individual byte array in sequential order at
	 * the end of the combined byte array.
	 * 
	 * Given byte_array_1 of length 8 + byte_array_2 of length 16, the result
	 * will be byte_array1 + byte_array_2 + 8 + 16.
	 * 
	 * Lengths are put after the individual arrays so they don't impact sorting
	 * when used within the key of a sorted key-value data store.
	 * 
	 * @param array1
	 *            the first byte array
	 * @param array2
	 *            the second byte array
	 * @return the combined byte array including the individual byte array
	 *         lengths
	 */
	public static byte[] combineVariableLengthArrays(
			final byte[] array1,
			final byte[] array2 ) {
		Preconditions.checkNotNull(
				array1,
				"First byte array cannot be null");
		Preconditions.checkNotNull(
				array2,
				"Second byte array cannot be null");
		Preconditions.checkArgument(
				array1.length > 1,
				"First byte array cannot have length 0");
		Preconditions.checkArgument(
				array2.length > 1,
				"Second byte array cannot have length 0");
		final byte[] combinedWithoutLengths = ByteArrayUtils.internalCombineArrays(
				array1,
				array2);
		final ByteBuffer combinedWithLengthsAppended = ByteBuffer.allocate(combinedWithoutLengths.length + 8); // 8
																												// for
																												// two
																												// integer
																												// lengths
		combinedWithLengthsAppended.put(combinedWithoutLengths);
		combinedWithLengthsAppended.putInt(array1.length);
		combinedWithLengthsAppended.putInt(array2.length);
		return combinedWithLengthsAppended.array();
	}

	public static Pair<byte[], byte[]> splitVariableLengthArrays(
			final byte[] combinedArray ) {
		final ByteBuffer combined = ByteBuffer.wrap(combinedArray);
		final byte[] combinedArrays = new byte[combinedArray.length - 8];
		combined.get(combinedArrays);
		final ByteBuffer bb = ByteBuffer.wrap(combinedArrays);
		final int len1 = combined.getInt();
		final int len2 = combined.getInt();
		final byte[] part1 = new byte[len1];
		final byte[] part2 = new byte[len2];
		bb.get(part1);
		bb.get(part2);
		return Pair.of(
				part1,
				part2);
	}
}