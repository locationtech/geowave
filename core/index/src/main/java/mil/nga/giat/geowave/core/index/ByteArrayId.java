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
import java.util.Arrays;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This class is a wrapper around a byte array to ensure equals and hashcode
 * operations use the values of the bytes rather than explicit object identity
 */
public class ByteArrayId implements
		java.io.Serializable,
		Comparable<ByteArrayId>
{
	private static final long serialVersionUID = 1L;
	private byte[] id;
	@SuppressFBWarnings("SE_TRANSIENT_FIELD_NOT_RESTORED")
	private transient String stringId;

	public ByteArrayId() {}

	public ByteArrayId(
			final byte[] id ) {
		this.id = id;
	}

	public ByteArrayId(
			final String id ) {
		this.id = StringUtils.stringToBinary(id);
		stringId = id;
	}

	public byte[] getBytes() {
		return id;
	}

	public String getString() {
		if (stringId == null) {
			stringId = StringUtils.stringFromBinary(id);
		}
		return stringId;
	}

	public String getHexString() {

		StringBuffer str = new StringBuffer();
		for (byte b : id) {
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
		result = (prime * result) + Arrays.hashCode(id);
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
		final ByteArrayId other = (ByteArrayId) obj;
		return Arrays.equals(
				id,
				other.id);
	}

	public static byte[] toBytes(
			final ByteArrayId[] ids ) {
		int len = 4;
		for (ByteArrayId id : ids) {
			len += (id.id.length + 4);
		}
		final ByteBuffer buffer = ByteBuffer.allocate(len);
		buffer.putInt(ids.length);
		for (ByteArrayId id : ids) {
			buffer.putInt(id.id.length);
			buffer.put(id.id);
		}
		return buffer.array();
	}

	public static ByteArrayId[] fromBytes(
			byte[] idData ) {
		final ByteBuffer buffer = ByteBuffer.wrap(idData);
		final int len = buffer.getInt();
		final ByteArrayId[] result = new ByteArrayId[len];
		for (int i = 0; i < len; i++) {
			final int idSize = buffer.getInt();
			final byte[] id = new byte[idSize];
			buffer.get(id);
			result[i] = new ByteArrayId(
					id);
		}
		return result;
	}

	@Override
	public int compareTo(
			ByteArrayId o ) {

		for (int i = 0, j = 0; i < id.length && j < o.id.length; i++, j++) {
			int a = (id[i] & 0xff);
			int b = (o.id[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return id.length - o.id.length;

	}
}
