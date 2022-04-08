/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.field.base;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.data.field.ArrayReader;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.data.field.base.IntegerSerializationProvider.IntegerReader;

public class IntegerArraySerializationProvider implements FieldSerializationProviderSpi<Integer[]> {

  @Override
  public FieldReader<Integer[]> getFieldReader() {
    return new IntegerArrayReader();
  }

  @Override
  public FieldWriter<Integer[]> getFieldWriter() {
    return new IntegerArrayWriter();
  }

  // @see PrimitiveIntArraySerializationProvider#PrimitiveIntArrayReader
  private static class IntegerArrayReader implements FieldReader<Integer[]> {
    @Override
    public Integer[] readField(final byte[] fieldData) {
      if ((fieldData == null) || (fieldData.length == 0)) {
        return null;
      }
      final ByteBuffer buff = ByteBuffer.wrap(fieldData);
      final int count = VarintUtils.readUnsignedInt(buff);
      ByteArrayUtils.verifyBufferSize(buff, count);
      final Integer[] result = new Integer[count];
      for (int i = 0; i < count; i++) {
        if (buff.get() > 0) {
          result[i] = VarintUtils.readSignedInt(buff);
        } else {
          result[i] = null;
        }
      }
      return result;
    }

    @Override
    public Integer[] readField(final byte[] fieldData, final byte serializationVersion) {
      if (serializationVersion < FieldUtils.SERIALIZATION_VERSION) {
        return new ArrayReader<>(new IntegerReader()).readField(fieldData, serializationVersion);
      } else {
        return readField(fieldData);
      }
    }
  }

  // @see PrimitiveIntArraySerializationProvider.PrimitiveIntArrayWriter
  private static class IntegerArrayWriter implements FieldWriter<Integer[]> {
    @Override
    public byte[] writeField(final Integer[] fieldValue) {
      if (fieldValue == null) {
        return new byte[] {};
      }
      int bytes = VarintUtils.unsignedIntByteLength(fieldValue.length);
      for (final Integer value : fieldValue) {
        bytes++;
        if (value != null) {
          bytes += VarintUtils.signedIntByteLength(value);
        }
      }
      final ByteBuffer buf = ByteBuffer.allocate(bytes);
      VarintUtils.writeUnsignedInt(fieldValue.length, buf);
      for (final Integer value : fieldValue) {
        if (value == null) {
          buf.put((byte) 0x0);
        } else {
          buf.put((byte) 0x1);
          VarintUtils.writeSignedInt(value, buf);
        }
      }
      return buf.array();
    }
  }
}
