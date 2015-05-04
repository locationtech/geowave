package mil.nga.giat.geowave.core.index;

import java.nio.charset.Charset;

/**
 * Convenience methods for converting to and from strings. The encoding and
 * decoding of strings uses UTF-8, and these methods should be used for
 * serializing and deserializing text-based data, not for converting binary data
 * to a String representation. Use ByteArrayUtils for converting data that is
 * binary in nature to a String for transport.
 * 
 */
public class StringUtils
{
	public static final Charset UTF8_CHAR_SET = Charset.forName("UTF-8");

	/**
	 * Utility to convert a String to bytes
	 * 
	 * @param string
	 *            incoming String to convert
	 * @return a byte array
	 */
	public static byte[] stringToBinary(
			final String string ) {
		return string.getBytes(UTF8_CHAR_SET);
	}

	/**
	 * Utility to convert bytes to a String
	 * 
	 * @param binary
	 *            a byte array to convert to a String
	 * @return a String representation of the byte array
	 */
	public static String stringFromBinary(
			final byte[] binary ) {
		return new String(
				binary,
				UTF8_CHAR_SET);
	}

	/**
	 * Convert a number to a string. In this case we ensure that it is safe for
	 * Accumulo table names by replacing '-' with '_'
	 * 
	 * @param number
	 *            the number to convert
	 * @return the safe string representing that number
	 */
	public static String intToString(
			final int number ) {
		return org.apache.commons.lang3.StringUtils.replace(
				Integer.toString(number),
				"-",
				"_");
	}

}
