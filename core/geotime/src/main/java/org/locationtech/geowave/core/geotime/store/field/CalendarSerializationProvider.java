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
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class CalendarSerializationProvider implements FieldSerializationProviderSpi<Calendar> {
  @Override
  public FieldReader<Calendar> getFieldReader() {
    return new CalendarReader();
  }

  @Override
  public FieldWriter<Calendar> getFieldWriter() {
    return new CalendarWriter();
  }

  protected static class CalendarReader implements FieldReader<Calendar> {
    @Override
    public Calendar readField(final byte[] fieldData) {
      if ((fieldData == null) || (fieldData.length == 0)) {
        return null;
      }
      final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
      cal.setTime(new Date(VarintUtils.readTime(ByteBuffer.wrap(fieldData))));
      return cal;
    }

    @Override
    public Calendar readField(final byte[] fieldData, final byte serializationVersion) {
      if ((fieldData == null) || (fieldData.length == 0)) {
        return null;
      }
      if (serializationVersion < FieldUtils.SERIALIZATION_VERSION) {
        final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        cal.setTime(new Date(ByteBuffer.wrap(fieldData).getLong()));
        return cal;
      } else {
        return readField(fieldData);
      }
    }
  }

  protected static class CalendarWriter implements FieldWriter<Calendar> {
    @Override
    public byte[] writeField(final Calendar cal) {
      if (cal == null) {
        return new byte[] {};
      }
      final long time = TimeUtils.calendarToGMTMillis(cal);
      final ByteBuffer buf = ByteBuffer.allocate(VarintUtils.timeByteLength(time));
      VarintUtils.writeTime(time, buf);
      return buf.array();
    }
  }
}
