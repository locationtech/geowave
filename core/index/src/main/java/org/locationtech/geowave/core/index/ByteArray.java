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
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This class is a wrapper around a byte array to ensure equals and hashcode
 * operations use the values of the bytes rather than explicit object identity
 */
public class ByteArray implements
		java.io.Serializable,
		Comparable<ByteArray>
{
	private static final long serialVersionUID = 1L;

	public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

	protected byte[] bytes;
	@SuppressFBWarnings("SE_TRANSIENT_FIELD_NOT_RESTORED")
	protected transient String string;

	public ByteArray() {
		this(
				EMPTY_BYTE_ARRAY);
	}

	public ByteArray(
			final byte[] bytes ) {
		this.bytes = bytes;
	}

	public ByteArray(
			final String string ) {
		this.bytes = StringUtils.stringToBinary(string);
		this.string = string;
	}

	public byte[] getBytes() {
		return bytes;
	}

	public byte[] getNextPrefix() {
		return getNextPrefix(bytes);
	}

	public String getString() {
		if (string == null) {
			string = StringUtils.stringFromBinary(bytes);
		}
		return string;
	}

	public String getHexString() {

		final StringBuffer str = new StringBuffer();
		for (final byte b : bytes) {
			str.append(String.format(
					"%02X ",
					b));
		}
		return str.toString();
	}

	@Override
	public String toString() {
		return "ByteArrayId [getString()=" + getString() + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + Arrays.hashCode(bytes);
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final ByteArray other = (ByteArray) obj;
		return Arrays.equals(
				bytes,
				other.bytes);
	}

	public static byte[] toBytes(
			final ByteArray[] ids ) {
		int len = 4;
		for (final ByteArray id : ids) {
			len += (id.bytes.length + 4);
		}
		final ByteBuffer buffer = ByteBuffer.allocate(len);
		buffer.putInt(ids.length);
		for (final ByteArray id : ids) {
			buffer.putInt(id.bytes.length);
			buffer.put(id.bytes);
		}
		return buffer.array();
	}

	public static ByteArray[] fromBytes(
			final byte[] idData ) {
		final ByteBuffer buffer = ByteBuffer.wrap(idData);
		final int len = buffer.getInt();
		final ByteArray[] result = new ByteArray[len];
		for (int i = 0; i < len; i++) {
			final int idSize = buffer.getInt();
			final byte[] id = new byte[idSize];
			buffer.get(id);
			result[i] = new ByteArray(
					id);
		}
		return result;
	}

	@Override
	public int compareTo(
			final ByteArray o ) {
		if (o == null) {
			return -1;
		}
		for (int i = 0, j = 0; (i < bytes.length) && (j < o.bytes.length); i++, j++) {
			final int a = (bytes[i] & 0xff);
			final int b = (o.bytes[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return bytes.length - o.bytes.length;

	}

	public static byte[] getNextPrefix(
			final byte[] rowKeyPrefix ) {
		int offset = rowKeyPrefix.length;
		while (offset > 0) {
			if (rowKeyPrefix[offset - 1] != (byte) 0xFF) {
				break;
			}
			offset--;
		}

		if (offset == 0) {
			// TODO: is this correct? an empty byte array sorts before a single
			// byte {0xFF}
			// return new byte[0];

			// it doesn't seem right, so instead, let's append several 0xFF
			// bytes
			return ByteArrayUtils.combineArrays(
					rowKeyPrefix,
					new byte[] {
						(byte) 0xFF,
						(byte) 0xFF,
						(byte) 0xFF,
						(byte) 0xFF,
						(byte) 0xFF,
						(byte) 0xFF,
						(byte) 0xFF
					});
		}

		final byte[] newStopRow = Arrays.copyOfRange(
				rowKeyPrefix,
				0,
				offset);
		// And increment the last one
		newStopRow[newStopRow.length - 1]++;
		return newStopRow;
	}
}
