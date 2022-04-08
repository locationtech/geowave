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
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class DateSerializationProvider implements FieldSerializationProviderSpi<Date> {

  @Override
  public FieldReader<Date> getFieldReader() {
    return new DateReader();
  }

  @Override
  public FieldWriter<Date> getFieldWriter() {
    return new DateWriter();
  }

  protected static class DateReader implements FieldReader<Date> {
    @Override
    public Date readField(final byte[] fieldData) {
      if ((fieldData == null) || (fieldData.length == 0)) {
        return null;
      }
      return new Date(VarintUtils.readTime(ByteBuffer.wrap(fieldData)));
    }

    @Override
    public Date readField(final byte[] fieldData, final byte serializationVersion) {
      if ((fieldData == null) || (fieldData.length == 0)) {
        return null;
      }
      if (serializationVersion < FieldUtils.SERIALIZATION_VERSION) {
        return new Date(ByteBuffer.wrap(fieldData).getLong());
      } else {
        return readField(fieldData);
      }
    }
  }

  protected static class DateWriter implements FieldWriter<Date> {
    @Override
    public byte[] writeField(final Date fieldData) {
      if (fieldData == null) {
        return new byte[] {};
      }

      final ByteBuffer buf = ByteBuffer.allocate(VarintUtils.timeByteLength(fieldData.getTime()));
      VarintUtils.writeTime(fieldData.getTime(), buf);
      return buf.array();
    }
  }
}
