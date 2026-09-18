/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.base;

import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.api.StatisticsOnlyWriter;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.StatisticUpdateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link StatisticsOnlyWriter} that computes and stores statistics from entries
 * without persisting the entries themselves. Statistic updates are batched in memory and written on
 * flush or close.
 *
 * @param <T> The type of entries to compute statistics from
 */
public class StatisticsOnlyWriterImpl<T> implements StatisticsOnlyWriter<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsOnlyWriterImpl.class);

  private final DataStatisticsStore statisticsStore;
  private final InternalDataAdapter<T> adapter;
  private final Index index;
  private final AdapterToIndexMapping indexMapping;
  private final VisibilityHandler visibilityHandler;
  private final List<StatisticUpdateHandler<T, ?, ?>> statisticUpdateHandlers;
  private final int flushStatsThreshold;
  private boolean closed = false;
  private int updateCount = 0;

  /**
   * Create a new statistics-only writer.
   *
   * @param baseDataStore the base data store to write statistics to
   * @param typeName the type name of entries to process
   * @param adapter the adapter for the type
   */
  @SuppressWarnings("unchecked")
  public StatisticsOnlyWriterImpl(
      final BaseDataStore baseDataStore,
      final String typeName,
      final DataTypeAdapter<T> adapter) {
    statisticsStore = baseDataStore.getStatisticsStore();
    flushStatsThreshold = baseDataStore.baseOptions.getFlushStatsThreshold();

    final Short adapterId = baseDataStore.internalAdapterStore.getAdapterId(typeName);
    if (adapterId == null) {
      throw new IllegalArgumentException("Type '" + typeName + "' does not exist");
    }
    this.adapter = (InternalDataAdapter<T>) baseDataStore.adapterStore.getAdapter(adapterId);
    if (this.adapter == null) {
      throw new IllegalArgumentException("Adapter for type '" + typeName + "' does not exist");
    }

    // An index is needed to encode entries into rows, which binning strategies and visibility
    // handling operate on. Without one, statistics that depend on row content cannot be binned.
    final Index[] indices = baseDataStore.getIndices(typeName);
    index = ((indices != null) && (indices.length > 0)) ? indices[0] : null;
    indexMapping =
        (index != null) ? baseDataStore.indexMappingStore.getMapping(adapterId, index.getName())
            : null;
    if (index == null) {
      LOGGER.warn(
          "No index exists for type '{}'. Statistics that rely on row content, such as partition "
              + "binning, will not be populated.",
          typeName);
    }

    // Statistics-only storage does not impose visibility constraints.
    visibilityHandler = null;

    final List<Statistic<? extends StatisticValue<?>>> statistics = loadStatistics(adapter);
    if (statistics.isEmpty()) {
      LOGGER.warn(
          "No statistics registered for type '{}'. "
              + "Add statistics using addEmptyStatistic() before creating a statistics-only writer.",
          typeName);
    }

    statisticUpdateHandlers = new ArrayList<>(statistics.size());
    for (final Statistic<? extends StatisticValue<?>> statistic : statistics) {
      @SuppressWarnings({"rawtypes", "unchecked"})
      final StatisticUpdateHandler<T, ?, ?> handler =
          new StatisticUpdateHandler(statistic, index, indexMapping, this.adapter);
      statisticUpdateHandlers.add(handler);
    }
  }

  private List<Statistic<? extends StatisticValue<?>>> loadStatistics(
      final DataTypeAdapter<T> adapter) {
    final List<Statistic<? extends StatisticValue<?>>> stats = new ArrayList<>();
    try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> it =
        statisticsStore.getDataTypeStatistics(adapter, null, null)) {
      while (it.hasNext()) {
        stats.add(it.next());
      }
    }
    try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> it =
        statisticsStore.getFieldStatistics(adapter, null, null, null)) {
      while (it.hasNext()) {
        stats.add(it.next());
      }
    }
    return stats;
  }

  @Override
  public void write(final T entry) {
    if (closed) {
      throw new IllegalStateException("Writer is closed");
    }
    if (entry == null) {
      LOGGER.warn("Attempted to write null entry, skipping");
      return;
    }

    // Rows are generated purely to drive binning and visibility; they are never persisted.
    final GeoWaveRow[] rows =
        (index == null) ? new GeoWaveRow[0]
            : BaseDataStoreUtils.getGeoWaveRows(
                entry,
                adapter,
                indexMapping,
                index,
                visibilityHandler);

    for (final StatisticUpdateHandler<T, ?, ?> handler : statisticUpdateHandlers) {
      handler.entryIngested(entry, rows);
    }

    updateCount++;
    if (updateCount >= flushStatsThreshold) {
      flush();
      updateCount = 0;
    }
  }

  @Override
  public void flush() {
    if (closed) {
      return;
    }
    for (final StatisticUpdateHandler<T, ?, ?> handler : statisticUpdateHandlers) {
      handler.writeStatistics(statisticsStore, false);
    }
  }

  @Override
  public void close() {
    if (!closed) {
      flush();
      closed = true;
    }
  }
}
