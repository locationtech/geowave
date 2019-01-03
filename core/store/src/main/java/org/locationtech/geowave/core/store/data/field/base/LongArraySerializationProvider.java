/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p>See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.field.base;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.data.field.ArrayReader;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.data.field.base.LongSerializationProvider.LongReader;

public class LongArraySerializationProvider implements FieldSerializationProviderSpi<Long[]> {

  @Override
  public FieldReader<Long[]> getFieldReader() {
    return new LongArrayReader();
  }

  @Override
  public FieldWriter<Object, Long[]> getFieldWriter() {
    return new LongArrayWriter();
  }

  // @see PrimitiveLongArraySerializationProvider.PrimitiveLongArrayReader
  private static class LongArrayReader implements FieldReader<Long[]> {
    @Override
    public Long[] readField(byte[] fieldData) {
      if ((fieldData == null) || (fieldData.length == 0)) {
        return null;
      }
      final ByteBuffer buff = ByteBuffer.wrap(fieldData);
      int count = VarintUtils.readUnsignedInt(buff);
      final Long[] result = new Long[count];
      for (int i = 0; i < count; i++) {
        if (buff.get() > 0) {
          result[i] = VarintUtils.readSignedLong(buff);
        } else {
          result[i] = null;
        }
      }
      return result;
    }

    @Override
    public Long[] readField(byte[] fieldData, byte serializationVersion) {
      if ((fieldData == null) || (fieldData.length == 0)) {
        return null;
      }
      if (serializationVersion < FieldUtils.SERIALIZATION_VERSION) {
        return new ArrayReader<Long>(new LongReader()).readField(fieldData, serializationVersion);
      } else {
        return readField(fieldData);
      }
    }
  }

  // @see PrimitiveLongArraySerializationProvider.PrimitiveLongArrayWriter
  private static class LongArrayWriter implements FieldWriter<Object, Long[]> {
    @Override
    public byte[] writeField(Long[] fieldValue) {
      if (fieldValue == null) {
        return new byte[] {};
      }
      int bytes = VarintUtils.unsignedIntByteLength(fieldValue.length);
      for (final Long value : fieldValue) {
        bytes++;
        if (value != null) {
          bytes += VarintUtils.signedLongByteLength(value);
        }
      }
      final ByteBuffer buf = ByteBuffer.allocate(bytes);
      VarintUtils.writeUnsignedInt(fieldValue.length, buf);
      for (final Long value : fieldValue) {
        if (value == null) {
          buf.put((byte) 0x0);
        } else {
          buf.put((byte) 0x1);
          VarintUtils.writeSignedLong(value, buf);
        }
      }
      return buf.array();
    }
  }
}
