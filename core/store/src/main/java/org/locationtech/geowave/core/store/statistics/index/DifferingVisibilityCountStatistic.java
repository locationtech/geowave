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
import java.util.HashSet;
import org.locationtech.geowave.core.index.ByteArray;
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
 * Counts the number of entries with differing visibilities.
 */
public class DifferingVisibilityCountStatistic extends
    IndexStatistic<DifferingVisibilityCountStatistic.DifferingVisibilityCountValue> {
  public static final IndexStatisticType<DifferingVisibilityCountValue> STATS_TYPE =
      new IndexStatisticType<>("DIFFERING_VISIBILITY_COUNT");


  public DifferingVisibilityCountStatistic() {
    super(STATS_TYPE);
  }

  public DifferingVisibilityCountStatistic(final String indexName) {
    super(STATS_TYPE, indexName);
  }

  @Override
  public String getDescription() {
    return "Counts the number of differing visibilities in the index.";
  }

  @Override
  public DifferingVisibilityCountValue createEmpty() {
    return new DifferingVisibilityCountValue(this);
  }

  public static class DifferingVisibilityCountValue extends StatisticValue<Long> implements
      StatisticsIngestCallback,
      StatisticsDeleteCallback {

    private long entriesWithDifferingFieldVisibilities = 0;

    public DifferingVisibilityCountValue() {
      this(null);
    }

    public DifferingVisibilityCountValue(Statistic<?> statistic) {
      super(statistic);
    }

    public boolean isAnyEntryDifferingFieldVisiblity() {
      return entriesWithDifferingFieldVisibilities > 0;
    }

    @Override
    public void merge(Mergeable merge) {
      if ((merge != null) && (merge instanceof DifferingVisibilityCountValue)) {
        entriesWithDifferingFieldVisibilities +=
            ((DifferingVisibilityCountValue) merge).entriesWithDifferingFieldVisibilities;
      }
    }

    /** This is expensive, but necessary since there may be duplicates */
    // TODO entryDeleted should only be called once with all duplicates
    private transient HashSet<ByteArray> ids = new HashSet<>();

    @Override
    public <T> void entryIngested(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows) {
      for (final GeoWaveRow kv : rows) {
        if (entryHasDifferentVisibilities(kv)) {
          if (ids.add(new ByteArray(rows[0].getDataId()))) {
            entriesWithDifferingFieldVisibilities++;
          }
        }
      }
    }

    @Override
    public <T> void entryDeleted(
        DataTypeAdapter<T> adapter,
        final T entry,
        final GeoWaveRow... kvs) {
      for (final GeoWaveRow kv : kvs) {
        if (entryHasDifferentVisibilities(kv)) {
          entriesWithDifferingFieldVisibilities--;
        }
      }
    }

    @Override
    public Long getValue() {
      return entriesWithDifferingFieldVisibilities;
    }

    @Override
    public byte[] toBinary() {
      return VarintUtils.writeUnsignedLong(entriesWithDifferingFieldVisibilities);
    }

    @Override
    public void fromBinary(byte[] bytes) {
      entriesWithDifferingFieldVisibilities = VarintUtils.readUnsignedLong(ByteBuffer.wrap(bytes));
    }

  }

  private static boolean entryHasDifferentVisibilities(final GeoWaveRow geowaveRow) {
    if ((geowaveRow.getFieldValues() != null) && (geowaveRow.getFieldValues().length > 1)) {
      // if there is 0 or 1 field, there won't be differing visibilities
      return true;
    }
    return false;
  }
}
