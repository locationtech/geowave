/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.base;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.callback.DeleteCallback;
import org.locationtech.geowave.core.store.callback.DeleteCallbackList;
import org.locationtech.geowave.core.store.callback.IngestCallback;
import org.locationtech.geowave.core.store.callback.IngestCallbackList;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;

public class DataStoreCallbackManager {

  private final DataStatisticsStore statsStore;
  private boolean persistStats = true;

  private final boolean captureAdapterStats;

  final Map<Short, IngestCallback<?>> icache = new HashMap<>();
  final Map<Short, DeleteCallback<?, GeoWaveRow>> dcache = new HashMap<>();

  public DataStoreCallbackManager(
      final DataStatisticsStore statsStore,
      final boolean captureAdapterStats) {
    this.statsStore = statsStore;
    this.captureAdapterStats = captureAdapterStats;
  }

  public <T> IngestCallback<T> getIngestCallback(
      final InternalDataAdapter<T> writableAdapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    if (!icache.containsKey(writableAdapter.getAdapterId())) {
      final List<IngestCallback<T>> callbackList = new ArrayList<>();
      if (persistStats) {
        callbackList.add(
            statsStore.createUpdateCallback(
                index,
                indexMapping,
                writableAdapter,
                captureAdapterStats));
      }
      icache.put(writableAdapter.getAdapterId(), new IngestCallbackList<>(callbackList));
    }
    return (IngestCallback<T>) icache.get(writableAdapter.getAdapterId());
  }

  public void setPersistStats(final boolean persistStats) {
    this.persistStats = persistStats;
  }

  public <T> DeleteCallback<T, GeoWaveRow> getDeleteCallback(
      final InternalDataAdapter<T> writableAdapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    if (!dcache.containsKey(writableAdapter.getAdapterId())) {
      final List<DeleteCallback<T, GeoWaveRow>> callbackList = new ArrayList<>();
      if (persistStats) {
        callbackList.add(
            statsStore.createUpdateCallback(
                index,
                indexMapping,
                writableAdapter,
                captureAdapterStats));
      }
      dcache.put(writableAdapter.getAdapterId(), new DeleteCallbackList<>(callbackList));
    }
    return (DeleteCallback<T, GeoWaveRow>) dcache.get(writableAdapter.getAdapterId());
  }

  public void close() throws IOException {
    for (final IngestCallback<?> callback : icache.values()) {
      if (callback instanceof Closeable) {
        ((Closeable) callback).close();
      }
    }
    for (final DeleteCallback<?, GeoWaveRow> callback : dcache.values()) {
      if (callback instanceof Closeable) {
        ((Closeable) callback).close();
      }
    }
  }
}
