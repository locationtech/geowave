/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.field.base;

import org.locationtech.geowave.core.store.data.field.ArrayReader;
import org.locationtech.geowave.core.store.data.field.ArrayWriter.FixedSizeObjectArrayWriter;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.data.field.base.BooleanSerializationProvider.BooleanReader;
import org.locationtech.geowave.core.store.data.field.base.BooleanSerializationProvider.BooleanWriter;

public class BooleanArraySerializationProvider implements FieldSerializationProviderSpi<Boolean[]> {
  @Override
  public FieldReader<Boolean[]> getFieldReader() {
    return new BooleanArrayReader();
  }

  @Override
  public FieldWriter<Boolean[]> getFieldWriter() {
    return new BooleanArrayWriter();
  }

  private static class BooleanArrayReader extends ArrayReader<Boolean> {
    public BooleanArrayReader() {
      super(new BooleanReader());
    }
  }

  private static class BooleanArrayWriter extends FixedSizeObjectArrayWriter<Boolean> {
    public BooleanArrayWriter() {
      super(new BooleanWriter());
    }
  }
}
