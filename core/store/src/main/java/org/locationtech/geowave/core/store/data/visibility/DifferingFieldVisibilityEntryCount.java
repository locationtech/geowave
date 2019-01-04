/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.visibility;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.IndexStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.IndexStatisticsType;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.callback.DeleteCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

public class DifferingFieldVisibilityEntryCount<T>
    extends AbstractDataStatistics<T, Long, IndexStatisticsQueryBuilder<Long>>
    implements DeleteCallback<T, GeoWaveRow> {
  public static final IndexStatisticsType<Long> STATS_TYPE =
      new IndexStatisticsType<>("DIFFERING_VISIBILITY_COUNT");

  private long entriesWithDifferingFieldVisibilities;

  public DifferingFieldVisibilityEntryCount() {
    super();
  }

  public long getEntriesWithDifferingFieldVisibilities() {
    return entriesWithDifferingFieldVisibilities;
  }

  public boolean isAnyEntryDifferingFieldVisiblity() {
    return entriesWithDifferingFieldVisibilities > 0;
  }

  public DifferingFieldVisibilityEntryCount(
      final short internalDataAdapterId,
      final String indexName) {
    this(internalDataAdapterId, indexName, 0);
  }

  private DifferingFieldVisibilityEntryCount(
      final short internalDataAdapterId,
      final String indexName,
      final long entriesWithDifferingFieldVisibilities) {
    super(internalDataAdapterId, STATS_TYPE, indexName);
    this.entriesWithDifferingFieldVisibilities = entriesWithDifferingFieldVisibilities;
  }

  @Override
  public InternalDataStatistics<T, Long, IndexStatisticsQueryBuilder<Long>> duplicate() {
    return new DifferingFieldVisibilityEntryCount<>(
        adapterId,
        extendedId,
        entriesWithDifferingFieldVisibilities);
  }

  @Override
  public byte[] toBinary() {
    if (entriesWithDifferingFieldVisibilities == 0) {
      return super.binaryBuffer(0).array();
    }
    final ByteBuffer buf =
        super.binaryBuffer(
            VarintUtils.unsignedLongByteLength(entriesWithDifferingFieldVisibilities));
    VarintUtils.writeUnsignedLong(entriesWithDifferingFieldVisibilities, buf);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = super.binaryBuffer(bytes);
    if (buf.hasRemaining()) {
      entriesWithDifferingFieldVisibilities = VarintUtils.readUnsignedLong(buf);
    } else {
      entriesWithDifferingFieldVisibilities = 0;
    }
  }

  @Override
  public void entryIngested(final T entry, final GeoWaveRow... kvs) {
    for (final GeoWaveRow kv : kvs) {
      if (entryHasDifferentVisibilities(kv)) {
        if (ids.add(new ByteArray(kvs[0].getDataId()))) {
          entriesWithDifferingFieldVisibilities++;
        }
      }
    }
  }

  /** This is expensive, but necessary since there may be duplicates */
  // TODO entryDeleted should only be called once with all duplicates
  private transient HashSet<ByteArray> ids = new HashSet<>();

  @Override
  public void entryDeleted(final T entry, final GeoWaveRow... kvs) {
    for (final GeoWaveRow kv : kvs) {
      if (entryHasDifferentVisibilities(kv)) {
        entriesWithDifferingFieldVisibilities--;
      }
    }
  }

  @Override
  public void merge(final Mergeable merge) {
    if ((merge != null) && (merge instanceof DifferingFieldVisibilityEntryCount)) {
      entriesWithDifferingFieldVisibilities +=
          ((DifferingFieldVisibilityEntryCount) merge).entriesWithDifferingFieldVisibilities;
    }
  }

  private static boolean entryHasDifferentVisibilities(final GeoWaveRow geowaveRow) {
    if ((geowaveRow.getFieldValues() != null) && (geowaveRow.getFieldValues().length > 1)) {
      // if there is 0 or 1 field, there won't be differing visibilities
      return true;
    }
    return false;
  }

  public static DifferingFieldVisibilityEntryCount getVisibilityCounts(
      final Index index,
      final Collection<Short> adapterIdsToQuery,
      final DataStatisticsStore statisticsStore,
      final String... authorizations) {
    DifferingFieldVisibilityEntryCount combinedVisibilityCount = null;
    for (final short adapterId : adapterIdsToQuery) {
      try (final CloseableIterator<InternalDataStatistics<?, ?, ?>> adapterVisibilityCountIt =
          statisticsStore.getDataStatistics(
              adapterId,
              index.getName(),
              STATS_TYPE,
              authorizations)) {
        if (adapterVisibilityCountIt.hasNext()) {
          final DifferingFieldVisibilityEntryCount adapterVisibilityCount =
              (DifferingFieldVisibilityEntryCount) adapterVisibilityCountIt.next();
          if (combinedVisibilityCount == null) {
            combinedVisibilityCount = adapterVisibilityCount;
          } else {
            combinedVisibilityCount.merge(adapterVisibilityCount);
          }
        }
      }
    }
    return combinedVisibilityCount;
  }

  @Override
  protected String resultsName() {
    return "entriesWithDifferingFieldVisibilities";
  }

  @Override
  protected Object resultsValue() {
    return Long.toString(entriesWithDifferingFieldVisibilities);
  }

  @Override
  public Long getResult() {
    return entriesWithDifferingFieldVisibilities;
  }
}
