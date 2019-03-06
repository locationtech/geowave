/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.statistics;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.locationtech.geowave.core.geotime.store.query.TemporalRange;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsType;
import org.locationtech.geowave.core.store.api.StatisticsQueryBuilder;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.threeten.extra.Interval;

public abstract class TimeRangeDataStatistics<T, B extends StatisticsQueryBuilder<Interval, B>>
    extends
    AbstractDataStatistics<T, Interval, B> {

  private long min = Long.MAX_VALUE;
  private long max = Long.MIN_VALUE;

  protected TimeRangeDataStatistics(final StatisticsType<Interval, B> statisticsType) {
    super(null, statisticsType);
  }

  public TimeRangeDataStatistics(
      final Short internalDataAdapterId,
      final StatisticsType<Interval, B> statisticsType) {
    super(internalDataAdapterId, statisticsType);
  }

  public TimeRangeDataStatistics(
      final Short internalDataAdapterId,
      final StatisticsType<Interval, B> statisticsType,
      final String extendedId) {
    super(internalDataAdapterId, statisticsType, extendedId);
  }

  public boolean isSet() {
    if ((min == Long.MAX_VALUE) && (max == Long.MIN_VALUE)) {
      return false;
    }
    return true;
  }

  public TemporalRange asTemporalRange() {
    return new TemporalRange(new Date(getMin()), new Date(getMax()));
  }

  public long getMin() {
    return min;
  }

  public long getMax() {
    return max;
  }

  public long getRange() {
    return max - min;
  }

  @Override
  public byte[] toBinary() {
    final ByteBuffer buffer =
        super.binaryBuffer(VarintUtils.timeByteLength(min) + VarintUtils.timeByteLength(max));
    VarintUtils.writeTime(min, buffer);
    VarintUtils.writeTime(max, buffer);
    return buffer.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buffer = super.binaryBuffer(bytes);
    min = VarintUtils.readTime(buffer);
    max = VarintUtils.readTime(buffer);
  }

  @Override
  public void entryIngested(final T entry, final GeoWaveRow... kvs) {
    final Interval range = getInterval(entry);
    if (range != null) {
      min = Math.min(min, range.getStart().toEpochMilli());
      max = Math.max(max, range.getEnd().toEpochMilli());
    }
  }

  protected abstract Interval getInterval(final T entry);

  @Override
  public void merge(final Mergeable statistics) {
    if ((statistics != null) && (statistics instanceof TimeRangeDataStatistics)) {
      final TimeRangeDataStatistics<T, ?> stats = (TimeRangeDataStatistics<T, ?>) statistics;
      if (stats.isSet()) {
        min = Math.min(min, stats.getMin());
        max = Math.max(max, stats.getMax());
      }
    }
  }

  @Override
  public String toString() {
    final StringBuffer buffer = new StringBuffer();
    buffer.append("range[adapterId=").append(super.getAdapterId());
    if (isSet()) {
      buffer.append(", min=").append(new Date(getMin()));
      buffer.append(", max=").append(new Date(getMax()));
    } else {
      buffer.append(", No Values");
    }
    buffer.append("]");
    return buffer.toString();
  }

  @Override
  protected String resultsName() {
    return "range";
  }

  @Override
  protected Object resultsValue() {
    if (isSet()) {
      final Map<String, Long> map = new HashMap<>();
      map.put("min", min);
      map.put("max", max);
      return map;
    } else {
      return "undefined";
    }
  }

  @Override
  public Interval getResult() {
    if (isSet()) {
      return Interval.of(Instant.ofEpochMilli(min), Instant.ofEpochMilli(max));
    }
    return null;
  }
}
