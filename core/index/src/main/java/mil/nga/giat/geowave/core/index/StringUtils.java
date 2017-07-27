/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.index;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private static final Logger LOGGER = LoggerFactory.getLogger(StringUtils.class);
	public static final Charset GEOWAVE_CHAR_SET = Charset.forName("ISO-8859-1");
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
		return string.getBytes(GEOWAVE_CHAR_SET);
	}

	/**
	 * Utility to convert a String to bytes
	 * 
	 * @param string
	 *            incoming String to convert
	 * @return a byte array
	 */
	public static byte[] stringsToBinary(
			final String strings[] ) {
		int len = 4;
		final List<byte[]> strsBytes = new ArrayList<byte[]>();
		for (final String str : strings) {
			final byte[] strByte = str.getBytes(GEOWAVE_CHAR_SET);
			strsBytes.add(strByte);
			len += (strByte.length + 4);

		}
		final ByteBuffer buf = ByteBuffer.allocate(len);
		buf.putInt(strings.length);
		for (final byte[] str : strsBytes) {
			buf.putInt(str.length);
			buf.put(str);
		}
		return buf.array();
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
				GEOWAVE_CHAR_SET);
	}

	/**
	 * Utility to convert bytes to a String
	 * 
	 * @param binary
	 *            a byte array to convert to a String
	 * @return a String representation of the byte array
	 */
	public static String[] stringsFromBinary(
			final byte[] binary ) {
		final ByteBuffer buf = ByteBuffer.wrap(binary);
		final int count = buf.getInt();
		final String[] result = new String[count];
		for (int i = 0; i < count; i++) {
			final int size = buf.getInt();
			final byte[] strBytes = new byte[size];
			buf.get(strBytes);
			result[i] = new String(
					strBytes,
					GEOWAVE_CHAR_SET);
		}
		return result;
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

	public static Map<String, String> parseParams(
			final String params )
			throws NullPointerException {
		final Map<String, String> paramsMap = new HashMap<String, String>();
		final String[] paramsSplit = params.split(";");
		for (final String param : paramsSplit) {
			final String[] keyValue = param.split("=");
			if (keyValue.length != 2) {
				LOGGER.warn("Unable to parse param '" + param + "'");
				continue;
			}
			paramsMap.put(
					keyValue[0].trim(),
					keyValue[1].trim());
		}
		return paramsMap;
	}
}
