/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.aggregate;

import java.nio.ByteBuffer;
import java.time.Instant;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.threeten.extra.Interval;

public abstract class TimeRangeAggregation<P extends Persistable, T> implements
    Aggregation<P, Interval, T> {

  protected long min = Long.MAX_VALUE;
  protected long max = Long.MIN_VALUE;

  @Override
  public P getParameters() {
    return null;
  }

  @Override
  public void setParameters(final P parameters) {}

  public boolean isSet() {
    if ((min == Long.MAX_VALUE) || (max == Long.MIN_VALUE)) {
      return false;
    }
    return true;
  }

  @Override
  public Interval getResult() {
    if (!isSet()) {
      return null;
    }
    return Interval.of(Instant.ofEpochMilli(min), Instant.ofEpochMilli(max));
  }

  @Override
  public Interval merge(final Interval result1, final Interval result2) {
    if (result1 == null) {
      return result2;
    } else if (result2 == null) {
      return result1;
    }
    final long min = Math.min(result1.getStart().toEpochMilli(), result1.getEnd().toEpochMilli());
    final long max = Math.max(result2.getStart().toEpochMilli(), result2.getEnd().toEpochMilli());
    return Interval.of(Instant.ofEpochMilli(min), Instant.ofEpochMilli(max));
  }

  @Override
  public byte[] resultToBinary(final Interval result) {
    long start = Long.MAX_VALUE;
    long end = Long.MIN_VALUE;
    if (result != null) {
      start = result.getStart().toEpochMilli();
      end = result.getEnd().toEpochMilli();
    }
    final ByteBuffer buffer =
        ByteBuffer.allocate(VarintUtils.timeByteLength(start) + VarintUtils.timeByteLength(end));
    VarintUtils.writeTime(start, buffer);
    VarintUtils.writeTime(end, buffer);
    return buffer.array();
  }

  @Override
  public Interval resultFromBinary(final byte[] binary) {
    final ByteBuffer buffer = ByteBuffer.wrap(binary);
    final long minTime = VarintUtils.readTime(buffer);
    final long maxTime = VarintUtils.readTime(buffer);
    if ((min == Long.MAX_VALUE) || (max == Long.MIN_VALUE)) {
      return null;
    }
    return Interval.of(Instant.ofEpochMilli(minTime), Instant.ofEpochMilli(maxTime));
  }

  @Override
  public void clearResult() {
    min = Long.MAX_VALUE;
    max = Long.MIN_VALUE;
  }

  @Override
  public void aggregate(final DataTypeAdapter<T> adapter, final T entry) {
    final Interval env = getInterval(entry);
    aggregate(env);
  }

  protected void aggregate(final Interval interval) {
    if (interval != null) {
      min = Math.min(min, interval.getStart().toEpochMilli());
      max = Math.max(max, interval.getEnd().toEpochMilli());
    }
  }

  protected abstract Interval getInterval(final T entry);
}
