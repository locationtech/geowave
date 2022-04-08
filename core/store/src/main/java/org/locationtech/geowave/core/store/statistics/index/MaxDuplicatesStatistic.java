/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.index;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.IndexStatistic;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.StatisticsIngestCallback;

/**
 * Maintains the maximum number of duplicates that a single entry in the data set contains.
 */
public class MaxDuplicatesStatistic extends
    IndexStatistic<MaxDuplicatesStatistic.MaxDuplicatesValue> {
  public static final IndexStatisticType<MaxDuplicatesValue> STATS_TYPE =
      new IndexStatisticType<>("MAX_DUPLICATES");

  public MaxDuplicatesStatistic() {
    super(STATS_TYPE);
  }

  public MaxDuplicatesStatistic(final String indexName) {
    super(STATS_TYPE, indexName);
  }

  @Override
  public String getDescription() {
    return "Maintains the maximum number of duplicates for an entry in the data set.";
  }

  @Override
  public MaxDuplicatesValue createEmpty() {
    return new MaxDuplicatesValue(this);
  }

  public static class MaxDuplicatesValue extends StatisticValue<Integer> implements
      StatisticsIngestCallback {

    public MaxDuplicatesValue() {
      this(null);
    }

    public MaxDuplicatesValue(Statistic<?> statistic) {
      super(statistic);
    }

    private int maxDuplicates = 0;

    public int getEntriesWithDifferingFieldVisibilities() {
      return maxDuplicates;
    }

    @Override
    public void merge(Mergeable merge) {
      if (merge != null && merge instanceof MaxDuplicatesValue) {
        maxDuplicates = Math.max(maxDuplicates, ((MaxDuplicatesValue) merge).getValue());
      }
    }

    @Override
    public <T> void entryIngested(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows) {
      for (final GeoWaveRow kv : rows) {
        maxDuplicates = Math.max(maxDuplicates, kv.getNumberOfDuplicates());
      }
    }

    @Override
    public Integer getValue() {
      return maxDuplicates;
    }

    @Override
    public byte[] toBinary() {
      return VarintUtils.writeUnsignedInt(maxDuplicates);
    }

    @Override
    public void fromBinary(byte[] bytes) {
      maxDuplicates = VarintUtils.readUnsignedInt(ByteBuffer.wrap(bytes));
    }
  }
}
