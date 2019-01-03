/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.field.base;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class LongSerializationProvider implements FieldSerializationProviderSpi<Long> {
  @Override
  public FieldReader<Long> getFieldReader() {
    return new LongReader();
  }

  @Override
  public FieldWriter<Object, Long> getFieldWriter() {
    return new LongWriter();
  }

  protected static class LongReader implements FieldReader<Long> {
    @Override
    public Long readField(final byte[] fieldData) {
      if ((fieldData == null) || (fieldData.length == 0)) {
        return null;
      }
      return VarintUtils.readSignedLong(ByteBuffer.wrap(fieldData));
    }

    @Override
    public Long readField(final byte[] fieldData, final byte serializationVersion) {
      if ((fieldData == null) || (fieldData.length == 0)) {
        return null;
      }
      if (serializationVersion < FieldUtils.SERIALIZATION_VERSION) {
        return ByteBuffer.wrap(fieldData).getLong();
      } else {
        return readField(fieldData);
      }
    }
  }

  protected static class LongWriter implements FieldWriter<Object, Long> {
    public LongWriter() {
      super();
    }

    @Override
    public byte[] writeField(final Long fieldValue) {
      if (fieldValue == null) {
        return new byte[] {};
      }

      final ByteBuffer buf = ByteBuffer.allocate(VarintUtils.signedLongByteLength(fieldValue));
      VarintUtils.writeSignedLong(fieldValue, buf);
      return buf.array();
    }
  }
}
