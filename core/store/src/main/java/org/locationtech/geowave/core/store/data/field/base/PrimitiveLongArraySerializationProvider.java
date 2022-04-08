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
import java.nio.LongBuffer;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class PrimitiveLongArraySerializationProvider implements
    FieldSerializationProviderSpi<long[]> {
  @Override
  public FieldReader<long[]> getFieldReader() {
    return new PrimitiveLongArrayReader();
  }

  @Override
  public FieldWriter<long[]> getFieldWriter() {
    return new PrimitiveLongArrayWriter();
  }

  private static class PrimitiveLongArrayReader implements FieldReader<long[]> {
    @Override
    public long[] readField(final byte[] fieldData) {
      if ((fieldData == null) || (fieldData.length == 0)) {
        return null;
      }
      final ByteBuffer buff = ByteBuffer.wrap(fieldData);
      final int count = VarintUtils.readUnsignedInt(buff);
      ByteArrayUtils.verifyBufferSize(buff, count);
      final long[] result = new long[count];
      for (int i = 0; i < count; i++) {
        result[i] = VarintUtils.readSignedLong(buff);
      }
      return result;
    }

    @Override
    public long[] readField(final byte[] fieldData, final byte serializationVersion) {
      if ((fieldData == null) || (fieldData.length == 0)) {
        return null;
      }
      if (serializationVersion < FieldUtils.SERIALIZATION_VERSION) {
        final LongBuffer buff = ByteBuffer.wrap(fieldData).asLongBuffer();
        final long[] result = new long[buff.remaining()];
        buff.get(result);
        return result;
      } else {
        return readField(fieldData);
      }
    }
  }

  private static class PrimitiveLongArrayWriter implements FieldWriter<long[]> {
    public PrimitiveLongArrayWriter() {
      super();
    }

    @Override
    public byte[] writeField(final long[] fieldValue) {
      if (fieldValue == null) {
        return new byte[] {};
      }
      int bytes = VarintUtils.unsignedIntByteLength(fieldValue.length);
      for (final long value : fieldValue) {
        bytes += VarintUtils.signedLongByteLength(value);
      }
      final ByteBuffer buf = ByteBuffer.allocate(bytes);
      VarintUtils.writeUnsignedInt(fieldValue.length, buf);
      for (final long value : fieldValue) {
        VarintUtils.writeSignedLong(value, buf);
      }
      return buf.array();
    }
  }
}
