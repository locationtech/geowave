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
import java.time.Instant;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.threeten.extra.Interval;

public class IntervalSerializationProvider implements FieldSerializationProviderSpi<Interval> {

  @Override
  public FieldReader<Interval> getFieldReader() {
    return new IntervalReader();
  }

  @Override
  public FieldWriter<Interval> getFieldWriter() {
    return new IntervalWriter();
  }

  public static class IntervalReader implements FieldReader<Interval> {
    @Override
    public Interval readField(final byte[] fieldData) {
      Interval retVal;
      // this is less generic than using the persistable interface but is a
      // little better for performance
      final ByteBuffer buf = ByteBuffer.wrap(fieldData);
      final Instant value = Instant.ofEpochMilli(VarintUtils.readTime(buf));
      if (buf.hasRemaining()) {
        retVal = Interval.of(value, Instant.ofEpochMilli(VarintUtils.readTime(buf)));
      } else {
        retVal = Interval.of(value, value);
      }
      return retVal;
    }
  }

  public static class IntervalWriter implements FieldWriter<Interval> {
    @Override
    public byte[] writeField(final Interval fieldData) {
      if (fieldData == null) {
        return new byte[] {};
      }
      if (fieldData.isEmpty()) {
        final long millis = fieldData.getStart().toEpochMilli();
        final ByteBuffer buf = ByteBuffer.allocate(VarintUtils.timeByteLength(millis));
        VarintUtils.writeTime(millis, buf);
        return buf.array();
      } else {
        final long startMillis = fieldData.getStart().toEpochMilli();
        final long endMillis = fieldData.getEnd().toEpochMilli();
        final ByteBuffer buf =
            ByteBuffer.allocate(
                VarintUtils.timeByteLength(startMillis) + VarintUtils.timeByteLength(endMillis));
        VarintUtils.writeTime(startMillis, buf);
        VarintUtils.writeTime(endMillis, buf);
        return buf.array();
      }
    }
  }
}
