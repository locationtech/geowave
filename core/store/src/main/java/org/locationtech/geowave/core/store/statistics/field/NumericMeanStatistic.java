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
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.StatisticsDeleteCallback;
import org.locationtech.geowave.core.store.statistics.StatisticsIngestCallback;

public class NumericMeanStatistic extends FieldStatistic<NumericMeanStatistic.NumericMeanValue> {

  public static final FieldStatisticType<NumericMeanValue> STATS_TYPE =
      new FieldStatisticType<>("NUMERIC_MEAN");

  public NumericMeanStatistic() {
    super(STATS_TYPE);
  }

  public NumericMeanStatistic(final String typeName, final String fieldName) {
    super(STATS_TYPE, typeName, fieldName);
  }

  @Override
  public String getDescription() {
    return "Provides the mean and sum of values for a numeric attribute.";
  }

  @Override
  public boolean isCompatibleWith(final Class<?> fieldClass) {
    return Number.class.isAssignableFrom(fieldClass);
  }

  @Override
  public NumericMeanValue createEmpty() {
    return new NumericMeanValue(this);
  }

  public static class NumericMeanValue extends StatisticValue<Double> implements
      StatisticsIngestCallback,
      StatisticsDeleteCallback {
    private double sum = 0;
    private long count = 0;

    public NumericMeanValue() {
      this(null);
    }

    private NumericMeanValue(final NumericMeanStatistic statistic) {
      super(statistic);
    }

    public long getCount() {
      return count;
    }

    public double getSum() {
      return sum;
    }

    public double getMean() {
      if (count <= 0) {
        return Double.NaN;
      }
      return sum / count;
    }

    @Override
    public void merge(final Mergeable merge) {
      if ((merge != null) && (merge instanceof NumericMeanValue)) {
        final NumericMeanValue other = (NumericMeanValue) merge;
        sum += other.getSum();
        count += other.count;
      }
    }

    @Override
    public <T> void entryIngested(
        final DataTypeAdapter<T> adapter,
        final T entry,
        final GeoWaveRow... rows) {
      final Object o =
          adapter.getFieldValue(entry, ((NumericMeanStatistic) getStatistic()).getFieldName());
      if (o == null) {
        return;
      }
      final double num = ((Number) o).doubleValue();
      if (!Double.isNaN(num)) {
        if (getBin() != null && getStatistic().getBinningStrategy() != null) {
          final double weight =
              getStatistic().getBinningStrategy().getWeight(getBin(), adapter, entry, rows);
          sum += (num * weight);
          count += (weight);
        } else {
          sum += num;
          count++;
        }
      }
    }

    @Override
    public <T> void entryDeleted(
        final DataTypeAdapter<T> adapter,
        final T entry,
        final GeoWaveRow... rows) {
      final Object o =
          adapter.getFieldValue(entry, ((NumericMeanStatistic) getStatistic()).getFieldName());
      if (o == null) {
        return;
      }
      final double num = ((Number) o).doubleValue();
      if (!Double.isNaN(num)) {
        if (getBin() != null && getStatistic().getBinningStrategy() != null) {
          final double weight =
              getStatistic().getBinningStrategy().getWeight(getBin(), adapter, entry, rows);
          sum -= (num * weight);
          count -= (weight);
        } else {
          sum -= num;
          count--;
        }
      }
    }

    @Override
    public Double getValue() {
      return getMean();
    }

    @Override
    public byte[] toBinary() {
      final ByteBuffer buffer =
          ByteBuffer.allocate(Double.BYTES + VarintUtils.unsignedLongByteLength(count));
      VarintUtils.writeUnsignedLong(count, buffer);
      buffer.putDouble(sum);
      return buffer.array();
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      final ByteBuffer buffer = ByteBuffer.wrap(bytes);
      count = VarintUtils.readUnsignedLong(buffer);
      sum = buffer.getDouble();
    }
  }
}

