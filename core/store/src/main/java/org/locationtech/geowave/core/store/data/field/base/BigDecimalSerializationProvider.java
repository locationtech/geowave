/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.field.base;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class BigDecimalSerializationProvider implements FieldSerializationProviderSpi<BigDecimal> {
  @Override
  public FieldReader<BigDecimal> getFieldReader() {
    return new BigDecimalReader();
  }

  @Override
  public FieldWriter<BigDecimal> getFieldWriter() {
    return new BigDecimalWriter();
  }

  protected static class BigDecimalReader implements FieldReader<BigDecimal> {
    @Override
    public BigDecimal readField(final byte[] fieldData) {
      if ((fieldData == null) || (fieldData.length < 2)) {
        return null;
      }
      final ByteBuffer bb = ByteBuffer.wrap(fieldData);
      final int scale = VarintUtils.readSignedInt(bb);
      final byte[] unscaled = new byte[bb.remaining()];
      bb.get(unscaled);
      return new BigDecimal(new BigInteger(unscaled), scale);
    }

    @Override
    public BigDecimal readField(final byte[] fieldData, final byte serializationVersion) {
      if (serializationVersion < FieldUtils.SERIALIZATION_VERSION) {
        if ((fieldData == null) || (fieldData.length < 2)) {
          return null;
        }
        final ByteBuffer bb = ByteBuffer.wrap(fieldData);
        final int scale = bb.getInt();
        final byte[] unscaled = new byte[bb.remaining()];
        bb.get(unscaled);
        return new BigDecimal(new BigInteger(unscaled), scale);
      } else {
        return readField(fieldData);
      }
    }
  }

  protected static class BigDecimalWriter implements FieldWriter<BigDecimal> {
    @Override
    public byte[] writeField(final BigDecimal fieldValue) {
      if (fieldValue == null) {
        return new byte[] {};
      }
      final byte[] unscaled = fieldValue.unscaledValue().toByteArray();
      final ByteBuffer buf =
          ByteBuffer.allocate(
              VarintUtils.signedIntByteLength(fieldValue.scale()) + unscaled.length);
      VarintUtils.writeSignedInt(fieldValue.scale(), buf);
      buf.put(unscaled);
      return buf.array();
    }
  }
}
