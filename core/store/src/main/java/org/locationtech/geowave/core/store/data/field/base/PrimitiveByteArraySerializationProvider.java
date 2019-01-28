/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.field.base;

import java.util.Arrays;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class PrimitiveByteArraySerializationProvider implements
    FieldSerializationProviderSpi<byte[]> {
  @Override
  public FieldReader<byte[]> getFieldReader() {
    return new PrimitiveByteArrayReader();
  }

  @Override
  public FieldWriter<Object, byte[]> getFieldWriter() {
    return new PrimitiveByteArrayWriter();
  }

  private static class PrimitiveByteArrayReader implements FieldReader<byte[]> {
    @Override
    public byte[] readField(final byte[] fieldData) {
      if ((fieldData == null) || (fieldData.length < 1)) {
        return null;
      }
      return Arrays.copyOf(fieldData, fieldData.length);
    }
  }

  private static class PrimitiveByteArrayWriter implements FieldWriter<Object, byte[]> {
    @Override
    public byte[] writeField(final byte[] fieldValue) {
      if (fieldValue == null) {
        return new byte[] {};
      }
      return fieldValue;
    }
  }
}
