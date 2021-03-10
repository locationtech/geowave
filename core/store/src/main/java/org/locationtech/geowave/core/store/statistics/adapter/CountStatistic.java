/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.adapter;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.StatisticsDeleteCallback;
import org.locationtech.geowave.core.store.statistics.StatisticsIngestCallback;

public class CountStatistic extends DataTypeStatistic<CountStatistic.CountValue> {
  public static final DataTypeStatisticType<CountValue> STATS_TYPE =
      new DataTypeStatisticType<>("COUNT");

  public CountStatistic() {
    super(STATS_TYPE);
  }

  public CountStatistic(final String typeName) {
    super(STATS_TYPE, typeName);
  }

  @Override
  public String getDescription() {
    return "Counts the number of entries in the data type.";
  }

  @Override
  public CountValue createEmpty() {
    return new CountValue(this);
  }

  public static class CountValue extends StatisticValue<Long> implements
      StatisticsIngestCallback,
      StatisticsDeleteCallback {

    public CountValue() {
      this(null);
    }

    public CountValue(Statistic<?> statistic) {
      super(statistic);
    }

    private long count = 0L;

    @Override
    public Long getValue() {
      return count;
    }

    @Override
    public <T> void entryDeleted(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows) {
      count--;
    }

    @Override
    public <T> void entryIngested(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows) {
      count++;
    }

    @Override
    public byte[] toBinary() {
      return VarintUtils.writeSignedLong(count);
    }

    @Override
    public void fromBinary(byte[] bytes) {
      count = VarintUtils.readSignedLong(ByteBuffer.wrap(bytes));
    }

    @Override
    public void merge(Mergeable merge) {
      if (merge != null && merge instanceof CountValue) {
        count = count + ((CountValue) merge).getValue();
      }
    }
  }
}
