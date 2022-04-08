/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.field;

import java.nio.ByteBuffer;
import java.util.Date;
import org.locationtech.geowave.core.geotime.store.field.DateSerializationProvider.DateReader;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.data.field.ArrayReader;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class DateArraySerializationProvider implements FieldSerializationProviderSpi<Date[]> {
  @Override
  public FieldReader<Date[]> getFieldReader() {
    return new DateArrayReader();
  }

  @Override
  public FieldWriter<Date[]> getFieldWriter() {
    return new DateArrayWriter();
  }

  // @see LongArraySerializationProvider.LongArrayReader
  private static class DateArrayReader implements FieldReader<Date[]> {
    @Override
    public Date[] readField(final byte[] fieldData) {
      if ((fieldData == null) || (fieldData.length == 0)) {
        return null;
      }
      final ByteBuffer buff = ByteBuffer.wrap(fieldData);
      final int count = VarintUtils.readUnsignedInt(buff);
      ByteArrayUtils.verifyBufferSize(buff, count);
      final Date[] result = new Date[count];
      for (int i = 0; i < count; i++) {
        if (buff.get() > 0) {
          result[i] = new Date(VarintUtils.readTime(buff));
        } else {
          result[i] = null;
        }
      }
      return result;
    }

    @Override
    public Date[] readField(final byte[] fieldData, final byte serializationVersion) {
      if (serializationVersion < FieldUtils.SERIALIZATION_VERSION) {
        return new ArrayReader<>(new DateReader()).readField(fieldData, serializationVersion);
      } else {
        return readField(fieldData);
      }
    }
  }

  // @see LongArraySerializationProvider.LongArrayWriter
  private static class DateArrayWriter implements FieldWriter<Date[]> {
    @Override
    public byte[] writeField(final Date[] fieldValue) {
      if (fieldValue == null) {
        return new byte[] {};
      }
      int bytes = VarintUtils.unsignedIntByteLength(fieldValue.length);
      for (final Date value : fieldValue) {
        bytes++;
        if (value != null) {
          bytes += VarintUtils.timeByteLength(value.getTime());
        }
      }
      final ByteBuffer buf = ByteBuffer.allocate(bytes);
      VarintUtils.writeUnsignedInt(fieldValue.length, buf);
      for (final Date value : fieldValue) {
        if (value == null) {
          buf.put((byte) 0x0);
        } else {
          buf.put((byte) 0x1);
          VarintUtils.writeTime(value.getTime(), buf);
        }
      }
      return buf.array();
    }
  }
}
