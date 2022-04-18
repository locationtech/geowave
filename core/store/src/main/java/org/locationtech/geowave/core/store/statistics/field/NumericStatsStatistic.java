/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.field;

import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.StatisticsIngestCallback;

/**
 * Tracks the min, max, count, mean, sum, variance and standard deviation of a numeric attribute.
 */
public class NumericStatsStatistic extends FieldStatistic<NumericStatsStatistic.NumericStatsValue> {

  public static final FieldStatisticType<NumericStatsValue> STATS_TYPE =
      new FieldStatisticType<>("NUMERIC_STATS");

  public NumericStatsStatistic() {
    super(STATS_TYPE);
  }

  public NumericStatsStatistic(final String typeName, final String fieldName) {
    super(STATS_TYPE, typeName, fieldName);
  }

  @Override
  public String getDescription() {
    return "Provides the min, max, count, mean, sum, variance and standard deviation of values for a numeric attribute.";
  }

  @Override
  public boolean isCompatibleWith(final Class<?> fieldClass) {
    return Number.class.isAssignableFrom(fieldClass);
  }

  @Override
  public NumericStatsValue createEmpty() {
    return new NumericStatsValue(this);
  }

  public static class NumericStatsValue extends StatisticValue<Stats> implements
      StatisticsIngestCallback {
    private StatsAccumulator acc = new StatsAccumulator();

    public NumericStatsValue() {
      this(null);
    }

    private NumericStatsValue(final NumericStatsStatistic statistic) {
      super(statistic);
    }

    @Override
    public void merge(final Mergeable merge) {
      if ((merge != null) && (merge instanceof NumericStatsValue)) {
        final NumericStatsValue other = (NumericStatsValue) merge;
        acc.addAll(other.acc.snapshot());
      }
    }

    @Override
    public <T> void entryIngested(
        final DataTypeAdapter<T> adapter,
        final T entry,
        final GeoWaveRow... rows) {
      final Object o =
          adapter.getFieldValue(entry, ((NumericStatsStatistic) getStatistic()).getFieldName());
      if (o == null) {
        return;
      }
      final double num = ((Number) o).doubleValue();
      if (!Double.isNaN(num)) {
        acc.add(num);
      }
    }

    @Override
    public Stats getValue() {
      return acc.snapshot();
    }

    @Override
    public byte[] toBinary() {
      return acc.snapshot().toByteArray();
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      acc = new StatsAccumulator();
      acc.addAll(Stats.fromByteArray(bytes));
    }
  }
}

