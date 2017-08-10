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
package mil.nga.giat.geowave.core.index.persist;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of convenience methods for serializing and deserializing persistable
 * objects
 *
 */
public class PersistenceUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(PersistenceUtils.class);

	public static byte[] toBinary(
			final Collection<? extends Persistable> persistables ) {
		if (persistables.isEmpty()) {
			return new byte[] {};
		}
		int byteCount = 4;

		final List<byte[]> persistableBinaries = new ArrayList<byte[]>();
		for (final Persistable persistable : persistables) {
			final byte[] binary = toBinary(persistable);
			byteCount += (4 + binary.length);
			persistableBinaries.add(binary);
		}
		final ByteBuffer buf = ByteBuffer.allocate(byteCount);
		buf.putInt(persistables.size());
		for (final byte[] binary : persistableBinaries) {
			buf.putInt(binary.length);
			buf.put(binary);
		}
		return buf.array();
	}

	public static byte[] toClassId(
			final Persistable persistable ) {
		if (persistable == null) {
			return new byte[0];
		}
		final Short classId = PersistableFactory.getInstance().getClassIdMapping().get(
				persistable.getClass());
		if (classId != null) {
			final ByteBuffer buf = ByteBuffer.allocate(2);
			buf.putShort(classId);
			return buf.array();
		}
		return new byte[0];
	}

	public static Persistable fromClassId(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final short classId = buf.getShort();

		final Persistable retVal = PersistableFactory.getInstance().newInstance(
				classId);
		return retVal;
	}

	public static byte[] toBinary(
			final Persistable persistable ) {
		if (persistable == null) {
			return new byte[0];
		}
		final Short classId = PersistableFactory.getInstance().getClassIdMapping().get(
				persistable.getClass());
		if (classId != null) {
			final byte[] persistableBinary = persistable.toBinary();
			final ByteBuffer buf = ByteBuffer.allocate(2 + persistableBinary.length);
			buf.putShort(classId);
			buf.put(persistableBinary);
			return buf.array();
		}
		return new byte[0];
	}

	public static List<Persistable> fromBinaryAsList(
			final byte[] bytes ) {
		final List<Persistable> persistables = new ArrayList<Persistable>();
		if ((bytes == null) || (bytes.length < 4)) {
			// the original binary didn't even contain the size of the
			// array, assume that nothing was persisted
			return persistables;
		}
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int size = buf.getInt();
		for (int i = 0; i < size; i++) {
			final byte[] persistableBinary = new byte[buf.getInt()];
			buf.get(persistableBinary);
			persistables.add(fromBinary(persistableBinary));
		}
		return persistables;
	}

	public static Persistable fromBinary(
			final byte[] bytes ) {
		if ((bytes == null) || (bytes.length < 2)) {
			return null;
		}
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final short classId = buf.getShort();

		final Persistable retVal = PersistableFactory.getInstance().newInstance(
				classId);

		final byte[] persistableBinary = new byte[bytes.length - 2];
		buf.get(persistableBinary);
		retVal.fromBinary(persistableBinary);
		return retVal;
	}
}