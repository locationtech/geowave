/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.operations;

import java.util.Arrays;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBClient;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBIndexTable;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBRow;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class RocksDBRowDeleter implements RowDeleter {
  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBRowDeleter.class);

  private static class CacheKey {
    private final String tableName;
    private final short adapterId;
    private final byte[] partition;

    public CacheKey(final String tableName, final short adapterId, final byte[] partition) {
      this.tableName = tableName;
      this.adapterId = adapterId;
      this.partition = partition;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + ((tableName == null) ? 0 : tableName.hashCode());
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final CacheKey other = (CacheKey) obj;
      if (tableName == null) {
        if (other.tableName != null) {
          return false;
        }
      } else if (!tableName.equals(other.tableName)) {
        return false;
      }
      return true;
    }
  }

  private final LoadingCache<CacheKey, RocksDBIndexTable> tableCache =
      Caffeine.newBuilder().build(nameAndAdapterId -> getIndexTable(nameAndAdapterId));
  private final RocksDBClient client;
  private final PersistentAdapterStore adapterStore;
  private final InternalAdapterStore internalAdapterStore;
  private final String indexName;

  public RocksDBRowDeleter(
      final RocksDBClient client,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final String indexName) {
    this.client = client;
    this.adapterStore = adapterStore;
    this.internalAdapterStore = internalAdapterStore;
    this.indexName = indexName;
  }

  @Override
  public void close() {
    tableCache.asMap().forEach((k, v) -> v.flush());
    tableCache.invalidateAll();
  }

  private RocksDBIndexTable getIndexTable(final CacheKey cacheKey) {
    return RocksDBUtils.getIndexTable(
        client,
        cacheKey.tableName,
        cacheKey.adapterId,
        cacheKey.partition,
        RocksDBUtils.isSortByTime(adapterStore.getAdapter(cacheKey.adapterId)));
  }

  @Override
  public void delete(final GeoWaveRow row) {
    final RocksDBIndexTable table =
        tableCache.get(
            new CacheKey(
                RocksDBUtils.getTableName(
                    internalAdapterStore.getTypeName(row.getAdapterId()),
                    indexName,
                    row.getPartitionKey()),
                row.getAdapterId(),
                row.getPartitionKey()));
    if (row instanceof GeoWaveRowImpl) {
      final GeoWaveKey key = ((GeoWaveRowImpl) row).getKey();
      if (key instanceof RocksDBRow) {
        deleteRow(table, (RocksDBRow) key);
      } else {
        LOGGER.info(
            "Unable to convert scanned row into RocksDBRow for deletion.  Row is of type GeoWaveRowImpl.");
        table.delete(key.getSortKey(), key.getDataId());
      }
    } else if (row instanceof RocksDBRow) {
      deleteRow(table, (RocksDBRow) row);
    } else {
      LOGGER.info(
          "Unable to convert scanned row into RocksDBRow for deletion. Row is of type "
              + row.getClass());
      table.delete(row.getSortKey(), row.getDataId());
    }
  }

  private static void deleteRow(final RocksDBIndexTable table, final RocksDBRow row) {
    Arrays.stream(row.getKeys()).forEach(k -> table.delete(k));
  }

  @Override
  public void flush() {}
}
