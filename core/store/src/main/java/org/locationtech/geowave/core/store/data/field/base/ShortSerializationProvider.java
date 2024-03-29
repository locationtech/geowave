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
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class ShortSerializationProvider implements FieldSerializationProviderSpi<Short> {

  @Override
  public FieldReader<Short> getFieldReader() {
    return new ShortReader();
  }

  @Override
  public FieldWriter<Short> getFieldWriter() {
    return new ShortWriter();
  }

  protected static class ShortReader implements FieldReader<Short> {
    @Override
    public Short readField(final byte[] fieldData) {
      if ((fieldData == null) || (fieldData.length < 2)) {
        return null;
      }
      return ByteBuffer.wrap(fieldData).getShort();
    }
  }

  protected static class ShortWriter implements FieldWriter<Short> {
    @Override
    public byte[] writeField(final Short fieldValue) {
      if (fieldValue == null) {
        return new byte[] {};
      }

      final ByteBuffer buf = ByteBuffer.allocate(2);
      buf.putShort(fieldValue);
      return buf.array();
    }
  }
}
