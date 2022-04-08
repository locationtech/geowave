/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.base.dataidx;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Iterators;

public class BatchIndexRetrievalImpl implements BatchDataIndexRetrieval {
  private static final Logger LOGGER = LoggerFactory.getLogger(BatchIndexRetrievalImpl.class);
  private final int batchSize;
  private final Map<Short, Map<ByteArray, CompletableFuture<GeoWaveValue[]>>> currentBatchesPerAdapter =
      new HashMap<>();
  private final DataStoreOperations operations;
  private final PersistentAdapterStore adapterStore;
  private final AdapterIndexMappingStore mappingStore;
  private final InternalAdapterStore internalAdapterStore;
  private final Pair<String[], InternalDataAdapter<?>> fieldSubsets;
  private final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation;
  private final String[] additionalAuthorizations;
  private final AtomicInteger outstandingIterators = new AtomicInteger(0);

  public BatchIndexRetrievalImpl(
      final DataStoreOperations operations,
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final InternalAdapterStore internalAdapterStore,
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
      final String[] additionalAuthorizations,
      final int batchSize) {
    this.operations = operations;
    this.adapterStore = adapterStore;
    this.mappingStore = mappingStore;
    this.internalAdapterStore = internalAdapterStore;
    this.fieldSubsets = fieldSubsets;
    this.aggregation = aggregation;
    this.additionalAuthorizations = additionalAuthorizations;
    this.batchSize = batchSize;
  }

  @Override
  public GeoWaveValue[] getData(final short adapterId, final byte[] dataId) {
    try (CloseableIterator<GeoWaveValue[]> it = getData(adapterId, new byte[][] {dataId})) {
      if (it.hasNext()) {
        return it.next();
      }
    }
    return null;
  }

  private CloseableIterator<GeoWaveValue[]> getData(final short adapterId, final byte[][] dataIds) {
    final RowReader<GeoWaveRow> rowReader =
        DataIndexUtils.getRowReader(
            operations,
            adapterStore,
            mappingStore,
            internalAdapterStore,
            fieldSubsets,
            aggregation,
            additionalAuthorizations,
            adapterId,
            dataIds);
    return new CloseableIteratorWrapper<>(
        rowReader,
        Iterators.transform(rowReader, r -> r.getFieldValues()));

  }

  @Override
  public synchronized CompletableFuture<GeoWaveValue[]> getDataAsync(
      final short adapterId,
      final byte[] dataId) {
    Map<ByteArray, CompletableFuture<GeoWaveValue[]>> batch =
        currentBatchesPerAdapter.get(adapterId);
    if (batch == null) {
      batch = new HashMap<>();
      currentBatchesPerAdapter.put(adapterId, batch);
    }
    final ByteArray dataIdKey = new ByteArray(dataId);
    CompletableFuture<GeoWaveValue[]> retVal = batch.get(dataIdKey);
    if (retVal == null) {
      retVal = new CompletableFuture<>();
      retVal = retVal.exceptionally(e -> {
        LOGGER.error("Unable to retrieve from data index", e);
        return null;
      });
      batch.put(dataIdKey, retVal);
      if (batch.size() >= batchSize) {
        flush(adapterId, batch);
      }
    }
    return retVal;
  }

  private void flush(
      final Short adapterId,
      final Map<ByteArray, CompletableFuture<GeoWaveValue[]>> batch) {
    final byte[][] internalDataIds;
    final CompletableFuture<GeoWaveValue[]>[] internalSuppliers;
    internalDataIds = new byte[batch.size()][];
    internalSuppliers = new CompletableFuture[batch.size()];
    final Iterator<Entry<ByteArray, CompletableFuture<GeoWaveValue[]>>> it =
        batch.entrySet().iterator();
    for (int i = 0; i < internalDataIds.length; i++) {
      final Entry<ByteArray, CompletableFuture<GeoWaveValue[]>> entry = it.next();
      internalDataIds[i] = entry.getKey().getBytes();
      internalSuppliers[i] = entry.getValue();
    }
    batch.clear();
    if (internalSuppliers.length > 0) {
      CompletableFuture.supplyAsync(() -> getData(adapterId, internalDataIds)).whenComplete(
          (values, ex) -> {
            if (values != null) {
              try {
                int i = 0;
                while (values.hasNext() && (i < internalSuppliers.length)) {
                  // the iterator has to be in order
                  internalSuppliers[i++].complete(values.next());
                }
                if (values.hasNext()) {
                  LOGGER.warn("There are more data index results than expected");
                } else if (i < internalSuppliers.length) {
                  LOGGER.warn("There are less data index results than expected");
                  while (i < internalSuppliers.length) {
                    // there should be exactly as many results as suppliers so this shouldn't happen
                    internalSuppliers[i++].complete(null);
                  }
                }
              } finally {
                values.close();
              }
            } else if (ex != null) {
              LOGGER.warn("Unable to retrieve from data index", ex);
              Arrays.stream(internalSuppliers).forEach(s -> s.completeExceptionally(ex));
            }
          });
    }
  }

  @Override
  public synchronized void flush() {
    if (!currentBatchesPerAdapter.isEmpty()) {
      currentBatchesPerAdapter.forEach((k, v) -> flush(k, v));
    }
  }

  @Override
  public void notifyIteratorInitiated() {
    outstandingIterators.incrementAndGet();
  }

  @Override
  public void notifyIteratorExhausted() {
    if (outstandingIterators.decrementAndGet() <= 0) {
      flush();
    }
  }
}
