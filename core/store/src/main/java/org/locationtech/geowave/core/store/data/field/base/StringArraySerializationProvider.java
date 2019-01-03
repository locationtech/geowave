/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.field.base;

import org.locationtech.geowave.core.store.data.field.ArrayReader;
import org.locationtech.geowave.core.store.data.field.ArrayWriter.VariableSizeObjectArrayWriter;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.data.field.base.StringSerializationProvider.StringReader;
import org.locationtech.geowave.core.store.data.field.base.StringSerializationProvider.StringWriter;

public class StringArraySerializationProvider implements FieldSerializationProviderSpi<String[]> {

  @Override
  public FieldReader<String[]> getFieldReader() {
    return new StringArrayReader();
  }

  @Override
  public FieldWriter<Object, String[]> getFieldWriter() {
    return new StringArrayWriter();
  }

  private static class StringArrayReader extends ArrayReader<String> {
    public StringArrayReader() {
      super(new StringReader());
    }
  }

  private static class StringArrayWriter extends VariableSizeObjectArrayWriter<Object, String> {
    public StringArrayWriter() {
      super(new StringWriter());
    }
  }
}
