package mil.nga.giat.geowave.core.index;

import java.nio.ByteBuffer;
import java.util.UUID;

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
		if (id1 == null) {
			combinedId = id2;
		}
		else if (id2 == null) {
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
}
