/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class BooleanSerializationProvider implements FieldSerializationProviderSpi<Boolean> {

  @Override
  public FieldReader<Boolean> getFieldReader() {
    return new BooleanReader();
  }

  @Override
  public FieldWriter<Object, Boolean> getFieldWriter() {
    return new BooleanWriter();
  }

  private static class BooleanReader implements FieldReader<Boolean> {
    @SuppressFBWarnings(
        value = {"NP_BOOLEAN_RETURN_NULL"},
        justification = "matches pattern of other read* methods")
    @Override
    public Boolean readField(final byte[] fieldData) {
      if ((fieldData == null) || (fieldData.length < 1)) {
        return null;
      }
      return fieldData[0] > 0;
    }
  }

  private static class BooleanWriter implements FieldWriter<Object, Boolean> {
    @Override
    public byte[] writeField(final Boolean fieldValue) {
      return new byte[] {((fieldValue == null) || !fieldValue) ? (byte) 0 : (byte) 1};
    }
  }
}
