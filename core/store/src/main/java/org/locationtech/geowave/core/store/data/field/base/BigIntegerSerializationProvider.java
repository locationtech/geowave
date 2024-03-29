/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.field.base;

import java.math.BigInteger;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class BigIntegerSerializationProvider implements FieldSerializationProviderSpi<BigInteger> {
  @Override
  public FieldReader<BigInteger> getFieldReader() {
    return new BigIntegerReader();
  }

  @Override
  public FieldWriter<BigInteger> getFieldWriter() {
    return new BigIntegerWriter();
  }

  protected static class BigIntegerReader implements FieldReader<BigInteger> {
    @Override
    public BigInteger readField(final byte[] fieldData) {
      if ((fieldData == null) || (fieldData.length < 4)) {
        return null;
      }
      return new BigInteger(fieldData);
    }
  }

  protected static class BigIntegerWriter implements FieldWriter<BigInteger> {
    @Override
    public byte[] writeField(final BigInteger fieldValue) {
      if (fieldValue == null) {
        return new byte[] {};
      }
      return fieldValue.toByteArray();
    }
  }
}
