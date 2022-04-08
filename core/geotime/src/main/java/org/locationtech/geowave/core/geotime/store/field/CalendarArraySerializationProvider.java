/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.field;

import java.util.Calendar;
import org.locationtech.geowave.core.geotime.store.field.CalendarSerializationProvider.CalendarReader;
import org.locationtech.geowave.core.geotime.store.field.CalendarSerializationProvider.CalendarWriter;
import org.locationtech.geowave.core.store.data.field.ArrayReader;
import org.locationtech.geowave.core.store.data.field.ArrayWriter.VariableSizeObjectArrayWriter;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class CalendarArraySerializationProvider implements
    FieldSerializationProviderSpi<Calendar[]> {
  @Override
  public FieldReader<Calendar[]> getFieldReader() {
    return new CalendarArrayReader();
  }

  @Override
  public FieldWriter<Calendar[]> getFieldWriter() {
    return new CalendarArrayWriter();
  }

  private static class CalendarArrayReader implements FieldReader<Calendar[]> {
    @Override
    public Calendar[] readField(final byte[] fieldData) {
      return new ArrayReader<>(new CalendarReader()).readField(fieldData);
    }
  }

  private static class CalendarArrayWriter extends VariableSizeObjectArrayWriter<Calendar> {
    public CalendarArrayWriter() {
      super(new CalendarWriter());
    }
  }
}
