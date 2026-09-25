/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.base.dataidx;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
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
    try (RowReader<GeoWaveRow> rows = getRows(adapterId, new byte[][] {dataId})) {
      if (rows.hasNext()) {
        return rows.next().getFieldValues();
      }
    }
    return null;
  }

  private RowReader<GeoWaveRow> getRows(final short adapterId, final byte[][] dataIds) {
    return DataIndexUtils.getRowReader(
        operations,
        adapterStore,
        mappingStore,
        internalAdapterStore,
        fieldSubsets,
        aggregation,
        additionalAuthorizations,
        adapterId,
        dataIds);
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
    if (batch.isEmpty()) {
      return;
    }
    final Map<ByteArray, CompletableFuture<GeoWaveValue[]>> requests = new HashMap<>(batch);
    batch.clear();
    final byte[][] dataIds =
        requests.keySet().stream().map(ByteArray::getBytes).toArray(byte[][]::new);
    CompletableFuture.supplyAsync(() -> getRows(adapterId, dataIds)).whenComplete((rows, ex) -> {
      if (rows != null) {
        try {
          // datastores leave out the IDs they have no row for, so a row's position in the results
          // does not identify its request
          while (rows.hasNext()) {
            final GeoWaveRow row = rows.next();
            final CompletableFuture<GeoWaveValue[]> request =
                requests.remove(new ByteArray(row.getDataId()));
            if (request != null) {
              request.complete(row.getFieldValues());
            } else {
              LOGGER.warn("The data index returned a row that was not requested, or was repeated");
            }
          }
          if (!requests.isEmpty()) {
            LOGGER.warn(
                requests.size()
                    + " of "
                    + dataIds.length
                    + " data IDs were not found in the data index for adapter ID "
                    + adapterId);
            requests.values().forEach(r -> r.complete(null));
          }
        } catch (final Exception e) {
          LOGGER.warn("Unable to retrieve from data index", e);
          requests.values().forEach(r -> r.completeExceptionally(e));
        } finally {
          try {
            rows.close();
          } catch (final Exception e) {
            LOGGER.warn("Unable to close data index reader", e);
          }
        }
      } else if (ex != null) {
        LOGGER.warn("Unable to retrieve from data index", ex);
        requests.values().forEach(r -> r.completeExceptionally(ex));
      }
    });
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
