/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.dimension;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.geotime.store.dimension.Time.TimeRange;
import org.locationtech.geowave.core.geotime.store.dimension.Time.Timestamp;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;

public class TimeReader implements FieldReader<Time> {
  public TimeReader() {}

  @Override
  public Time readField(final byte[] bytes) {
    Time retVal;
    // this is less generic than using the persistable interface but is a
    // little better for performance
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    long value = VarintUtils.readTime(buf);
    if (buf.hasRemaining()) {
      retVal = new TimeRange(value, VarintUtils.readTime(buf));
    } else {
      retVal = new Timestamp(value);
    }
    return retVal;
  }

  @Override
  public Time readField(final byte[] bytes, final byte serializationVersion) {
    Time retVal;
    // this is less generic than using the persistable interface but is a
    // little better for performance
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    if (serializationVersion < FieldUtils.SERIALIZATION_VERSION) {
      if (bytes.length > 8) {
        retVal = new TimeRange(buf.getLong(), buf.getLong());
      } else {
        retVal = new Timestamp(buf.getLong());
      }
    } else {
      return readField(bytes);
    }
    return retVal;
  }
}
