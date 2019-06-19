/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.persist;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A set of convenience methods for serializing and deserializing persistable objects */
public class PersistenceUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(PersistenceUtils.class);

  public static byte[] toBinary(final Collection<? extends Persistable> persistables) {
    if (persistables.isEmpty()) {
      return new byte[] {};
    }
    int byteCount = VarintUtils.unsignedIntByteLength(persistables.size());

    final List<byte[]> persistableBinaries = new ArrayList<>();
    for (final Persistable persistable : persistables) {
      final byte[] binary = toBinary(persistable);
      byteCount += (VarintUtils.unsignedIntByteLength(binary.length) + binary.length);
      persistableBinaries.add(binary);
    }
    final ByteBuffer buf = ByteBuffer.allocate(byteCount);
    VarintUtils.writeUnsignedInt(persistables.size(), buf);
    for (final byte[] binary : persistableBinaries) {
      VarintUtils.writeUnsignedInt(binary.length, buf);
      buf.put(binary);
    }
    return buf.array();
  }

  public static byte[] toClassId(final Persistable persistable) {
    if (persistable == null) {
      return new byte[0];
    }
    final Short classId =
        PersistableFactory.getInstance().getClassIdMapping().get(persistable.getClass());
    if (classId != null) {
      final ByteBuffer buf = ByteBuffer.allocate(2);
      buf.putShort(classId);
      return buf.array();
    }
    return new byte[0];
  }

  public static byte[] toClassId(final String className) {
    if ((className == null) || className.isEmpty()) {
      return new byte[0];
    }
    Short classId;
    try {
      classId = PersistableFactory.getInstance().getClassIdMapping().get(Class.forName(className));
      if (classId != null) {
        final ByteBuffer buf = ByteBuffer.allocate(2);
        buf.putShort(classId);
        return buf.array();
      }
    } catch (final ClassNotFoundException e) {
      LOGGER.warn("Unable to find class", e);
    }
    return new byte[0];
  }

  public static Persistable fromClassId(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final short classId = buf.getShort();

    final Persistable retVal = PersistableFactory.getInstance().newInstance(classId);
    return retVal;
  }

  public static byte[] toBinary(final Persistable persistable) {
    if (persistable == null) {
      return new byte[0];
    }
    final Short classId =
        PersistableFactory.getInstance().getClassIdMapping().get(persistable.getClass());
    if (classId != null) {
      final byte[] persistableBinary = persistable.toBinary();
      final ByteBuffer buf = ByteBuffer.allocate(2 + persistableBinary.length);
      buf.putShort(classId);
      buf.put(persistableBinary);
      return buf.array();
    }
    return new byte[0];
  }

  public static List<Persistable> fromBinaryAsList(final byte[] bytes) {
    final List<Persistable> persistables = new ArrayList<>();
    if ((bytes == null) || (bytes.length == 0)) {
      // the original binary didn't even contain the size of the
      // array, assume that nothing was persisted
      return persistables;
    }
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int size = VarintUtils.readUnsignedInt(buf);
    for (int i = 0; i < size; i++) {
      final byte[] persistableBinary =
          ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
      persistables.add(fromBinary(persistableBinary));
    }
    return persistables;
  }

  public static Persistable fromBinary(final byte[] bytes) {
    if ((bytes == null) || (bytes.length < 2)) {
      return null;
    }
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final short classId = buf.getShort();

    final Persistable retVal = PersistableFactory.getInstance().newInstance(classId);
    final byte[] persistableBinary = new byte[bytes.length - 2];
    buf.get(persistableBinary);
    retVal.fromBinary(persistableBinary);
    return retVal;
  }
}
