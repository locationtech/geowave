/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.adapter;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.FloatCompareUtils;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataTypeStatistic;
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

    public CountValue(final Statistic<?> statistic) {
      super(statistic);
    }

    private long count = 0L;
    private Double weightedCount = null;

    @Override
    public Long getValue() {
      if (weightedCount != null) {
        return Math.round(weightedCount);
      }
      return count;
    }

    public Double getWeightedCount() {
      if (weightedCount != null) {
        return weightedCount;
      }
      return (double) count;
    }

    @Override
    public <T> void entryDeleted(
        final DataTypeAdapter<T> adapter,
        final T entry,
        final GeoWaveRow... rows) {
      if ((getBin() != null) && (getStatistic().getBinningStrategy() != null)) {
        final double weight =
            getStatistic().getBinningStrategy().getWeight(getBin(), adapter, entry, rows);
        if (FloatCompareUtils.checkDoublesEqual(0.0, weight)) {
          // don't mess with potentially switching to weights if the weight is 0
          return;
        } else if (!FloatCompareUtils.checkDoublesEqual(1.0, weight)) {
          // let it pass through to normal incrementing if the weight is 1, otherwise use weights
          if (weightedCount == null) {
            weightedCount = (double) count;
            count = 0;
          }
          weightedCount -= weight;
          return;
        }
      }
      if (weightedCount != null) {
        weightedCount -= 1;
      } else {
        count--;
      }
    }

    @Override
    public <T> void entryIngested(
        final DataTypeAdapter<T> adapter,
        final T entry,
        final GeoWaveRow... rows) {
      if ((getBin() != null) && (getStatistic().getBinningStrategy() != null)) {
        final double weight =
            getStatistic().getBinningStrategy().getWeight(getBin(), adapter, entry, rows);
        if (FloatCompareUtils.checkDoublesEqual(0.0, weight)) {
          // don't mess with potentially switching to weights if the weight is 0
          return;
        } else if (!FloatCompareUtils.checkDoublesEqual(1.0, weight)) {
          // let it pass through to normal incrementing if the weight is 1, otherwise use weights
          if (weightedCount == null) {
            weightedCount = (double) count;
            count = 0;
          }
          weightedCount += weight;
          return;
        }
      }
      if (weightedCount != null) {
        weightedCount += 1;
      } else {
        count++;
      }
    }

    @Override
    public byte[] toBinary() {
      // if its double lets make it 9 bytes with the last one being 0xFF (which is impossible for
      // varint encoding)
      if (weightedCount != null) {
        final ByteBuffer buf = ByteBuffer.allocate(9);
        buf.putDouble(weightedCount);
        buf.put((byte) 0xFF);
        return buf.array();
      }
      return VarintUtils.writeSignedLong(count);
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      if ((bytes.length == 9) && (bytes[8] == (byte) 0xFF)) {
        count = 0;
        weightedCount = ByteBuffer.wrap(bytes).getDouble();
      } else {
        count = VarintUtils.readSignedLong(ByteBuffer.wrap(bytes));
        weightedCount = null;
      }
    }

    @Override
    public void merge(final Mergeable merge) {
      if ((merge != null) && (merge instanceof CountValue)) {
        if (weightedCount != null) {
          if (((CountValue) merge).weightedCount != null) {
            weightedCount += ((CountValue) merge).weightedCount;
          } else {
            weightedCount += ((CountValue) merge).getValue();
          }
        } else {
          if (((CountValue) merge).weightedCount != null) {
            weightedCount = (double) count;
            count = 0;
            weightedCount += ((CountValue) merge).weightedCount;
          } else {
            count += ((CountValue) merge).getValue();
          }
        }
      }
    }
  }
}
