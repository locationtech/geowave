/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.field;

import java.nio.ByteBuffer;
import org.apache.commons.lang3.Range;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.StatisticsIngestCallback;

/**
 * Tracks the range of a numeric attribute.
 */
public class NumericRangeStatistic extends FieldStatistic<NumericRangeStatistic.NumericRangeValue> {

  public static final FieldStatisticType<NumericRangeValue> STATS_TYPE =
      new FieldStatisticType<>("NUMERIC_RANGE");

  public NumericRangeStatistic() {
    super(STATS_TYPE);
  }

  public NumericRangeStatistic(final String typeName, final String fieldName) {
    super(STATS_TYPE, typeName, fieldName);
  }

  @Override
  public String getDescription() {
    return "Provides the minimum and maximum values of a numeric attribute.";
  }

  @Override
  public boolean isCompatibleWith(Class<?> fieldClass) {
    return Number.class.isAssignableFrom(fieldClass);
  }

  @Override
  public NumericRangeValue createEmpty() {
    return new NumericRangeValue(this);
  }

  public static class NumericRangeValue extends StatisticValue<Range<Double>> implements
      StatisticsIngestCallback {
    private double min = Double.MAX_VALUE;
    private double max = -Double.MAX_VALUE;

    public NumericRangeValue() {
      this(null);
    }

    private NumericRangeValue(final Statistic<?> statistic) {
      super(statistic);
    }

    public boolean isSet() {
      if ((min == Double.MAX_VALUE) && (max == -Double.MAX_VALUE)) {
        return false;
      }
      return true;
    }

    public double getMin() {
      return min;
    }

    public double getMax() {
      return max;
    }

    public double getRange() {
      return max - min;
    }

    @Override
    public void merge(Mergeable merge) {
      if (merge != null && merge instanceof NumericRangeValue) {
        final NumericRangeValue other = (NumericRangeValue) merge;
        if (other.isSet()) {
          min = Math.min(min, other.getMin());
          max = Math.max(max, other.getMax());
        }
      }
    }

    @Override
    public <T> void entryIngested(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows) {
      final Object o =
          adapter.getFieldValue(entry, ((NumericRangeStatistic) getStatistic()).getFieldName());
      if (o == null) {
        return;
      }
      final double num = ((Number) o).doubleValue();
      if (!Double.isNaN(num)) {
        min = Math.min(min, num);
        max = Math.max(max, num);
      }
    }

    @Override
    public Range<Double> getValue() {
      if (isSet()) {
        return Range.between(min, max);
      }
      return null;
    }

    @Override
    public byte[] toBinary() {
      final ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES * 2);
      buffer.putDouble(min);
      buffer.putDouble(max);
      return buffer.array();
    }

    @Override
    public void fromBinary(byte[] bytes) {
      final ByteBuffer buffer = ByteBuffer.wrap(bytes);
      min = buffer.getDouble();
      max = buffer.getDouble();
    }
  }
}
