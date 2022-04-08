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
import org.locationtech.geowave.core.store.statistics.StatisticsDeleteCallback;
import org.locationtech.geowave.core.store.statistics.StatisticsIngestCallback;

/**
 * Counts the number of entries with duplicates in the index.
 */
public class DuplicateEntryCountStatistic extends
    IndexStatistic<DuplicateEntryCountStatistic.DuplicateEntryCountValue> {
  public static final IndexStatisticType<DuplicateEntryCountValue> STATS_TYPE =
      new IndexStatisticType<>("DUPLICATE_ENTRY_COUNT");

  public DuplicateEntryCountStatistic() {
    super(STATS_TYPE);
  }

  public DuplicateEntryCountStatistic(final String indexName) {
    super(STATS_TYPE, indexName);
  }

  @Override
  public DuplicateEntryCountValue createEmpty() {
    return new DuplicateEntryCountValue(this);
  }

  @Override
  public String getDescription() {
    return "Counts the number of entries with duplicates in the index.";
  }

  public static class DuplicateEntryCountValue extends StatisticValue<Long> implements
      StatisticsIngestCallback,
      StatisticsDeleteCallback {

    private long entriesWithDuplicates = 0L;

    public DuplicateEntryCountValue() {
      this(null);
    }

    public DuplicateEntryCountValue(final Statistic<?> statistic) {
      super(statistic);
    }

    public boolean isAnyEntryHaveDuplicates() {
      return entriesWithDuplicates > 0;
    }

    @Override
    public Long getValue() {
      return entriesWithDuplicates;
    }

    @Override
    public void merge(Mergeable merge) {
      if ((merge != null) && (merge instanceof DuplicateEntryCountValue)) {
        entriesWithDuplicates += ((DuplicateEntryCountValue) merge).getValue();
      }
    }

    @Override
    public byte[] toBinary() {
      return VarintUtils.writeSignedLong(entriesWithDuplicates);
    }

    @Override
    public void fromBinary(byte[] bytes) {
      entriesWithDuplicates = VarintUtils.readSignedLong(ByteBuffer.wrap(bytes));
    }

    @Override
    public <T> void entryDeleted(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows) {
      if (rows.length > 0) {
        if (entryHasDuplicates(rows[0])) {
          entriesWithDuplicates--;
        }
      }
    }

    @Override
    public <T> void entryIngested(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows) {
      if (rows.length > 0) {
        if (entryHasDuplicates(rows[0])) {
          entriesWithDuplicates++;
        }
      }
    }

    private static boolean entryHasDuplicates(final GeoWaveRow kv) {
      return kv.getNumberOfDuplicates() > 0;
    }
  }
}
