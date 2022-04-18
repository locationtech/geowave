/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.EntryVisibilityHandler;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticBinningStrategy;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.callback.DeleteCallback;
import org.locationtech.geowave.core.store.callback.IngestCallback;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.binning.CompositeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.binning.PartitionBinningStrategy;
import org.locationtech.geowave.core.store.statistics.visibility.DefaultStatisticVisibility;
import org.locationtech.geowave.core.store.statistics.visibility.FieldDependentStatisticVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * This class handles updates for a single statistic. It is responsible for creating separate
 * statistic values for each visibility and bin combination.
 */
public class StatisticUpdateHandler<T, V extends StatisticValue<R>, R> implements
    IngestCallback<T>,
    DeleteCallback<T, GeoWaveRow>,
    ScanCallback<T, GeoWaveRow> {
  private static final Logger LOGGER = LoggerFactory.getLogger(StatisticUpdateHandler.class);
  private final Statistic<V> statistic;
  private final Map<ByteArray, Map<ByteArray, V>> statisticsMap = new HashMap<>();
  private final EntryVisibilityHandler<T> visibilityHandler;
  private final InternalDataAdapter<T> adapter;
  private final IngestHandler<T, V, R> ingestHandler;
  private final DeleteHandler<T, V, R> deleteHandler;
  private final boolean supportsIngestCallback;
  private final boolean supportsDeleteCallback;
  private final boolean filterByPartition;

  private static final ByteArray NO_BIN = new ByteArray(new byte[0]);

  public StatisticUpdateHandler(
      final Statistic<V> statistic,
      final Index index,
      final AdapterToIndexMapping indexMapping,
      final InternalDataAdapter<T> adapter) {
    this.statistic = statistic;
    this.adapter = adapter;
    this.visibilityHandler = getVisibilityHandler(indexMapping, index);
    this.ingestHandler = new IngestHandler<>();
    this.deleteHandler = new DeleteHandler<>();
    final V value = statistic.createEmpty();
    supportsIngestCallback = value instanceof StatisticsIngestCallback;
    supportsDeleteCallback = value instanceof StatisticsDeleteCallback;
    final StatisticBinningStrategy binningStrategy = statistic.getBinningStrategy();
    if (binningStrategy != null) {
      filterByPartition =
          binningStrategy instanceof PartitionBinningStrategy
              || ((binningStrategy instanceof CompositeBinningStrategy)
                  && ((CompositeBinningStrategy) binningStrategy).usesStrategy(
                      PartitionBinningStrategy.class));
    } else {
      filterByPartition = false;
    }
  }

  protected void handleEntry(
      final Handler<T, V, R> handler,
      final T entry,
      final GeoWaveRow... rows) {
    if (rows.length == 0) {
      // This can happen with attribute indices when the attribute value is null
      return;
    }
    final ByteArray visibility = new ByteArray(visibilityHandler.getVisibility(entry, rows));
    Map<ByteArray, V> binnedValues = statisticsMap.get(visibility);
    if (binnedValues == null) {
      binnedValues = Maps.newHashMap();
      statisticsMap.put(visibility, binnedValues);
    }
    if (statistic.getBinningStrategy() != null) {
      if (filterByPartition) {
        for (final GeoWaveRow row : rows) {
          handleBinnedRows(handler, binnedValues, entry, row);
        }
      } else {
        handleBinnedRows(handler, binnedValues, entry, rows);
      }
    } else {
      handleBin(handler, binnedValues, NO_BIN, entry, rows);
    }
  }

  protected void handleBinnedRows(
      final Handler<T, V, R> handler,
      final Map<ByteArray, V> binnedValues,
      final T entry,
      final GeoWaveRow... rows) {
    final ByteArray[] bins = statistic.getBinningStrategy().getBins(adapter, entry, rows);
    for (final ByteArray bin : bins) {
      handleBin(handler, binnedValues, bin, entry, rows);
    }
  }

  protected void handleBin(
      final Handler<T, V, R> handler,
      final Map<ByteArray, V> binnedValues,
      final ByteArray bin,
      final T entry,
      final GeoWaveRow... rows) {
    V value = binnedValues.get(bin);
    if (value == null) {
      value = statistic.createEmpty();
      value.setBin(bin);
      binnedValues.put(bin, value);
    }
    handler.handle(value, adapter, entry, rows);
  }

  private EntryVisibilityHandler<T> getVisibilityHandler(
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    final Set<String> usedFields = Sets.newHashSet();
    if (statistic instanceof FieldStatistic) {
      usedFields.add(((FieldStatistic<?>) statistic).getFieldName());
    }
    if (statistic.getBinningStrategy() != null) {
      statistic.getBinningStrategy().addFieldsUsed(usedFields);
    }
    boolean fieldDependent = false;
    for (final String fieldName : usedFields) {
      // If all of the used fields are part of the common index model, we can use the default
      // visibility
      if ((indexMapping != null) && !adapter.isCommonIndexField(indexMapping, fieldName)) {
        fieldDependent = true;
        break;
      }
    }
    if (fieldDependent) {
      return new FieldDependentStatisticVisibility<>(
          index != null ? index.getIndexModel() : null,
          adapter,
          usedFields.toArray(new String[usedFields.size()]));
    }
    return new DefaultStatisticVisibility<>();
  }

  @Override
  public synchronized void entryIngested(final T entry, final GeoWaveRow... rows) {
    if (supportsIngestCallback) {
      handleEntry(ingestHandler, entry, rows);
    }
  }

  @Override
  public synchronized void entryDeleted(final T entry, final GeoWaveRow... rows) {
    if (supportsDeleteCallback) {
      handleEntry(deleteHandler, entry, rows);
    }
  }

  @Override
  public synchronized void entryScanned(final T entry, final GeoWaveRow row) {
    if (supportsIngestCallback) {
      handleEntry(ingestHandler, entry, row);
    }
  }

  public void writeStatistics(final DataStatisticsStore statisticsStore, final boolean overwrite) {
    if (overwrite) {
      statisticsStore.removeStatisticValues(statistic);
    }
    try (StatisticValueWriter<V> statWriter =
        statisticsStore.createStatisticValueWriter(statistic)) {
      for (final Entry<ByteArray, Map<ByteArray, V>> visibilityStatistic : statisticsMap.entrySet()) {
        final Map<ByteArray, V> bins = visibilityStatistic.getValue();
        for (final Entry<ByteArray, V> binValue : bins.entrySet()) {
          statWriter.writeStatisticValue(
              binValue.getKey().getBytes(),
              visibilityStatistic.getKey().getBytes(),
              binValue.getValue());
        }
      }
      statisticsMap.clear();
    } catch (final Exception e) {
      LOGGER.error("Unable to write statistic value.", e);
    }
  }

  private static interface Handler<T, V extends StatisticValue<R>, R> {
    public void handle(
        V value,
        DataTypeAdapter<T> adapter,
        final T entry,
        final GeoWaveRow... rows);
  }

  private static class IngestHandler<T, V extends StatisticValue<R>, R> implements
      Handler<T, V, R> {
    @Override
    public void handle(
        final V value,
        final DataTypeAdapter<T> adapter,
        final T entry,
        final GeoWaveRow... rows) {
      if (value instanceof StatisticsIngestCallback) {
        ((StatisticsIngestCallback) value).entryIngested(adapter, entry, rows);
      }
    }
  }

  private static class DeleteHandler<T, V extends StatisticValue<R>, R> implements
      Handler<T, V, R> {
    @Override
    public void handle(
        final V value,
        final DataTypeAdapter<T> adapter,
        final T entry,
        final GeoWaveRow... rows) {
      if (value instanceof StatisticsDeleteCallback) {
        ((StatisticsDeleteCallback) value).entryDeleted(adapter, entry, rows);
      }
    }
  }

  @Override
  public String toString() {
    return "StatisticUpdateHandler -> " + statistic.toString();
  }
}
