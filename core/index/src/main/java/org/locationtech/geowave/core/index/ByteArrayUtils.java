/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.index;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Preconditions;

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
	private static Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();

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
		return new String(
				ENCODER.encode(byteArray),
				StringUtils.getGeoWaveCharset());
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
		return Base64.getUrlDecoder().decode(
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
		if ((id1 == null) || (id1.length == 0)) {
			combinedId = id2;
		}
		else if ((id2 == null) || (id2.length == 0)) {
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
		final ByteBuffer bb = ByteBuffer.allocate(Long.BYTES);
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
			final byte[] bytes ) {
		final ByteBuffer bb = ByteBuffer.allocate(Long.BYTES);
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

	public static String shortToString(
			final short input ) {
		return byteArrayToString(shortToByteArray(input));
	}

	public static short shortFromString(
			final String input ) {
		return byteArrayToShort(byteArrayFromString(input));
	}

	public static byte[] shortToByteArray(
			final short input ) {
		return new byte[] {
			(byte) (input & 0xFF),
			(byte) ((input >> 8) & 0xFF)
		};
	}

	public static short byteArrayToShort(
			final byte[] bytes ) {
		int r = bytes[1] & 0xFF;
		r = (r << 8) | (bytes[0] & 0xFF);
		return (short) r;
	}

	public static byte[] variableLengthEncode(
			long n ) {
		final int numRelevantBits = 64 - Long.numberOfLeadingZeros(n);
		int numBytes = (numRelevantBits + 6) / 7;
		if (numBytes == 0) {
			numBytes = 1;
		}
		final byte[] output = new byte[numBytes];
		for (int i = numBytes - 1; i >= 0; i--) {
			int curByte = (int) (n & 0x7F);
			if (i != (numBytes - 1)) {
				curByte |= 0x80;
			}
			output[i] = (byte) curByte;
			n >>>= 7;
		}
		return output;
	}

	public static long variableLengthDecode(
			final byte[] b ) {
		long n = 0;
		for (int i = 0; i < b.length; i++) {
			final int curByte = b[i] & 0xFF;
			n = (n << 7) | (curByte & 0x7F);
			if ((curByte & 0x80) == 0) {
				break;
			}
		}
		return n;
	}
}
