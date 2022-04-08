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
import java.nio.IntBuffer;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class PrimitiveIntArraySerializationProvider implements
    FieldSerializationProviderSpi<int[]> {
  @Override
  public FieldReader<int[]> getFieldReader() {
    return new PrimitiveIntArrayReader();
  }

  @Override
  public FieldWriter<int[]> getFieldWriter() {
    return new PrimitiveIntArrayWriter();
  }

  private static class PrimitiveIntArrayReader implements FieldReader<int[]> {
    @Override
    public int[] readField(final byte[] fieldData) {
      if ((fieldData == null) || (fieldData.length == 0)) {
        return null;
      }
      final ByteBuffer buff = ByteBuffer.wrap(fieldData);
      final int count = VarintUtils.readUnsignedInt(buff);
      ByteArrayUtils.verifyBufferSize(buff, count);
      final int[] result = new int[count];
      for (int i = 0; i < count; i++) {
        result[i] = VarintUtils.readSignedInt(buff);
      }
      return result;
    }

    @Override
    public int[] readField(final byte[] fieldData, final byte serializationVersion) {
      if ((fieldData == null) || (fieldData.length == 0)) {
        return null;
      }
      if (serializationVersion < FieldUtils.SERIALIZATION_VERSION) {
        final IntBuffer buff = ByteBuffer.wrap(fieldData).asIntBuffer();
        final int[] result = new int[buff.remaining()];
        buff.get(result);
        return result;
      } else {
        return readField(fieldData);
      }
    }
  }

  private static class PrimitiveIntArrayWriter implements FieldWriter<int[]> {
    @Override
    public byte[] writeField(final int[] fieldValue) {
      if (fieldValue == null) {
        return new byte[] {};
      }
      int bytes = VarintUtils.unsignedIntByteLength(fieldValue.length);
      for (final int value : fieldValue) {
        bytes += VarintUtils.signedIntByteLength(value);
      }
      final ByteBuffer buf = ByteBuffer.allocate(bytes);
      VarintUtils.writeUnsignedInt(fieldValue.length, buf);
      for (final int value : fieldValue) {
        VarintUtils.writeSignedInt(value, buf);
      }
      return buf.array();
    }
  }
}
