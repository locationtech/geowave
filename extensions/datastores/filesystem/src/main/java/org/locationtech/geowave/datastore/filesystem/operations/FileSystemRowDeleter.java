/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.operations;

import java.util.Arrays;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemClient;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemIndexTable;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemRow;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class FileSystemRowDeleter implements RowDeleter {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemRowDeleter.class);

  private static class CacheKey {
    private final short adapterId;
    private final String typeName;
    private final String indexName;
    private final byte[] partition;

    public CacheKey(
        final short adapterId,
        final String typeName,
        final String indexName,
        final byte[] partition) {
      this.adapterId = adapterId;
      this.typeName = typeName;
      this.indexName = indexName;
      this.partition = partition;
    }

  }

  private final LoadingCache<CacheKey, FileSystemIndexTable> tableCache =
      Caffeine.newBuilder().build(nameAndAdapterId -> getIndexTable(nameAndAdapterId));
  private final FileSystemClient client;
  private final PersistentAdapterStore adapterStore;
  private final InternalAdapterStore internalAdapterStore;
  private final String indexName;

  public FileSystemRowDeleter(
      final FileSystemClient client,
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
    tableCache.invalidateAll();
  }

  private FileSystemIndexTable getIndexTable(final CacheKey cacheKey) {
    return FileSystemUtils.getIndexTable(
        client,
        cacheKey.adapterId,
        cacheKey.typeName,
        cacheKey.indexName,
        cacheKey.partition,
        FileSystemUtils.isSortByTime(adapterStore.getAdapter(cacheKey.adapterId)));
  }

  @Override
  public void delete(final GeoWaveRow row) {
    final FileSystemIndexTable table =
        tableCache.get(
            new CacheKey(
                row.getAdapterId(),
                internalAdapterStore.getTypeName(row.getAdapterId()),
                indexName,
                row.getPartitionKey()));
    if (row instanceof GeoWaveRowImpl) {
      final GeoWaveKey key = ((GeoWaveRowImpl) row).getKey();
      if (key instanceof FileSystemRow) {
        deleteRow(table, (FileSystemRow) key);
      } else {
        LOGGER.info(
            "Unable to convert scanned row into FileSystemRow for deletion.  Row is of type GeoWaveRowImpl.");
        table.delete(key.getSortKey(), key.getDataId());
      }
    } else if (row instanceof FileSystemRow) {
      deleteRow(table, (FileSystemRow) row);
    } else {
      LOGGER.info(
          "Unable to convert scanned row into FileSystemRow for deletion. Row is of type "
              + row.getClass());
      table.delete(row.getSortKey(), row.getDataId());
    }
  }

  private static void deleteRow(final FileSystemIndexTable table, final FileSystemRow row) {
    Arrays.stream(row.getFiles()).forEach(f -> table.deleteFile(f));
  }

  @Override
  public void flush() {}
}
