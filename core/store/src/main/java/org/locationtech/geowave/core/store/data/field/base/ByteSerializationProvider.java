/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.field.base;

import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class ByteSerializationProvider implements FieldSerializationProviderSpi<Byte> {
  @Override
  public FieldReader<Byte> getFieldReader() {
    return new ByteReader();
  }

  @Override
  public FieldWriter<Object, Byte> getFieldWriter() {
    return new ByteWriter();
  }

  private static class ByteReader implements FieldReader<Byte> {
    @Override
    public Byte readField(final byte[] fieldData) {
      if ((fieldData == null) || (fieldData.length < 1)) {
        return null;
      }
      return fieldData[0];
    }
  }

  public static class ByteWriter implements FieldWriter<Object, Byte> {
    @Override
    public byte[] writeField(final Byte fieldValue) {
      if (fieldValue == null) {
        return new byte[] {};
      }

      return new byte[] {fieldValue};
    }
  }
}
