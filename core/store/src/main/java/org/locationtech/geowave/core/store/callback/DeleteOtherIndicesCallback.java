/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.callback;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.SinglePartitionInsertionIds;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class DeleteOtherIndicesCallback<T> implements DeleteCallback<T, GeoWaveRow>, Closeable {
  private final DataStoreOperations dataStoreOperations;
  private final InternalDataAdapter<?> adapter;
  private final List<Index> indices;
  private final Map<String, AdapterToIndexMapping> indexMappings;
  private final PersistentAdapterStore adapterStore;
  private final InternalAdapterStore internalAdapterStore;
  private final String[] authorizations;
  private final LoadingCache<String, RowDeleter> rowDeleters =
      Caffeine.newBuilder().build(new CacheLoader<String, RowDeleter>() {
        @Override
        public RowDeleter load(final String indexName) throws Exception {
          return dataStoreOperations.createRowDeleter(
              indexName,
              adapterStore,
              internalAdapterStore,
              authorizations);
        }
      });

  public DeleteOtherIndicesCallback(
      final DataStoreOperations dataStoreOperations,
      final InternalDataAdapter<?> adapter,
      final List<Index> indices,
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final InternalAdapterStore internalAdapterStore,
      final String... authorizations) {
    this.adapter = adapter;
    this.indices = indices;
    this.indexMappings =
        indices.stream().map(
            index -> mappingStore.getMapping(adapter.getAdapterId(), index.getName())).collect(
                Collectors.toMap(AdapterToIndexMapping::getIndexName, mapping -> mapping));
    this.dataStoreOperations = dataStoreOperations;
    this.adapterStore = adapterStore;
    this.internalAdapterStore = internalAdapterStore;
    this.authorizations = authorizations;
  }

  @Override
  public void close() throws IOException {
    rowDeleters.asMap().values().forEach(d -> d.close());
    rowDeleters.invalidateAll();
  }

  @Override
  public void entryDeleted(final T entry, final GeoWaveRow... rows) {
    if (rows.length > 0) {
      for (final Index index : indices) {
        final InsertionIds ids =
            DataStoreUtils.getInsertionIdsForEntry(
                entry,
                adapter,
                indexMappings.get(index.getName()),
                index);
        for (final SinglePartitionInsertionIds partitionId : ids.getPartitionKeys()) {
          for (final byte[] sortKey : partitionId.getSortKeys()) {
            rowDeleters.get(index.getName()).delete(
                new GeoWaveRowImpl(
                    new GeoWaveKeyImpl(
                        rows[0].getDataId(),
                        adapter.getAdapterId(),
                        partitionId.getPartitionKey(),
                        sortKey,
                        rows[0].getNumberOfDuplicates()),
                    rows[0].getFieldValues()));
          }
        }
      }
    }
  }
}
