/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.operations;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.DataIndexReaderParams;
import org.locationtech.geowave.core.store.operations.Deleter;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.core.store.operations.QueryAndDeleteByRow;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.datastore.rocksdb.config.RocksDBOptions;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBClient;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBClientCache;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBDataIndexTable;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBUtils;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class RocksDBOperations implements MapReduceDataStoreOperations, Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBOperations.class);
  private static final Object CLIENT_MUTEX = new Object();
  private static final boolean READER_ASYNC = true;
  private RocksDBClient client;
  private String directory;
  private final boolean visibilityEnabled;
  private final boolean compactOnWrite;
  private final boolean walOnBatchWrite;
  private final int batchWriteSize;

  public RocksDBOperations(final RocksDBOptions options) {
    // attempt to make the directory string as unique for a given file system as possible by using
    // the canonical path
    try {
      directory =
          new File(
              options.getDirectory()
                  + File.separator
                  + ((options.getGeoWaveNamespace() == null)
                      || options.getGeoWaveNamespace().trim().isEmpty()
                      || "null".equalsIgnoreCase(options.getGeoWaveNamespace()) ? "default"
                          : options.getGeoWaveNamespace())).getCanonicalPath();
    } catch (final IOException e) {
      LOGGER.warn("Unable to get canonical path", e);
      directory =
          new File(
              options.getDirectory()
                  + File.separator
                  + ((options.getGeoWaveNamespace() == null)
                      || options.getGeoWaveNamespace().trim().isEmpty()
                      || "null".equalsIgnoreCase(options.getGeoWaveNamespace()) ? "default"
                          : options.getGeoWaveNamespace())).getAbsolutePath();
    }

    visibilityEnabled = options.getStoreOptions().isVisibilityEnabled();
    compactOnWrite = options.isCompactOnWrite();
    batchWriteSize = options.getBatchWriteSize();
    walOnBatchWrite = options.isWalOnBatchWrite();
    // a factory method that returns a RocksDB instance
    client =
        RocksDBClientCache.getInstance().getClient(
            directory,
            visibilityEnabled,
            compactOnWrite,
            batchWriteSize,
            walOnBatchWrite);
  }

  @Override
  public boolean mergeData(
      final Index index,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final AdapterIndexMappingStore adapterIndexMappingStore,
      final Integer maxRangeDecomposition) {
    final boolean retVal =
        MapReduceDataStoreOperations.super.mergeData(
            index,
            adapterStore,
            internalAdapterStore,
            adapterIndexMappingStore,
            maxRangeDecomposition);
    compactData();
    return retVal;
  }

  public void compactData() {
    getClient().mergeData();
  }

  public void compactMetadata() {
    getClient().mergeMetadata();
  }

  @Override
  public boolean mergeStats(final DataStatisticsStore statsStore) {
    final boolean retVal = MapReduceDataStoreOperations.super.mergeStats(statsStore);
    compactMetadata();
    return retVal;
  }

  @Override
  public boolean indexExists(final String indexName) throws IOException {
    return getClient().indexTableExists(indexName);
  }

  @Override
  public boolean metadataExists(final MetadataType type) throws IOException {
    return getClient().metadataTableExists(type);
  }

  @Override
  public void deleteAll() throws Exception {
    close(false);
    FileUtils.deleteDirectory(new File(directory));
  }

  @Override
  public boolean deleteAll(
      final String indexName,
      final String typeName,
      final Short adapterId,
      final String... additionalAuthorizations) {
    final String prefix = RocksDBUtils.getTablePrefix(typeName, indexName);
    getClient().close(indexName, typeName);
    Arrays.stream(new File(directory).listFiles((dir, name) -> name.startsWith(prefix))).forEach(
        f -> {
          try {
            FileUtils.deleteDirectory(f);
          } catch (final IOException e) {
            LOGGER.warn("Unable to delete directory '" + f.getAbsolutePath() + "'", e);
          }
        });
    return true;
  }

  @Override
  public RowWriter createWriter(final Index index, final InternalDataAdapter<?> adapter) {
    return new RocksDBWriter(
        getClient(),
        adapter.getAdapterId(),
        adapter.getTypeName(),
        index.getName(),
        RocksDBUtils.isSortByTime(adapter));
  }

  @Override
  public RowWriter createDataIndexWriter(final InternalDataAdapter<?> adapter) {
    return new RockDBDataIndexWriter(getClient(), adapter.getAdapterId(), adapter.getTypeName());
  }

  @Override
  public MetadataWriter createMetadataWriter(final MetadataType metadataType) {
    return new RocksDBMetadataWriter(RocksDBUtils.getMetadataTable(getClient(), metadataType));
  }

  @Override
  public MetadataReader createMetadataReader(final MetadataType metadataType) {
    return new RocksDBMetadataReader(
        RocksDBUtils.getMetadataTable(getClient(), metadataType),
        metadataType);
  }

  @Override
  public MetadataDeleter createMetadataDeleter(final MetadataType metadataType) {
    return new RocksDBMetadataDeleter(
        RocksDBUtils.getMetadataTable(getClient(), metadataType),
        metadataType);
  }

  @Override
  public <T> RowReader<T> createReader(final ReaderParams<T> readerParams) {
    return new RocksDBReader<>(getClient(), readerParams, READER_ASYNC);
  }

  @Override
  public RowReader<GeoWaveRow> createReader(final DataIndexReaderParams readerParams) {
    return new RocksDBReader<>(getClient(), readerParams);
  }

  @Override
  public <T> Deleter<T> createDeleter(final ReaderParams<T> readerParams) {
    return new QueryAndDeleteByRow<>(
        createRowDeleter(
            readerParams.getIndex().getName(),
            readerParams.getAdapterStore(),
            readerParams.getInternalAdapterStore(),
            readerParams.getAdditionalAuthorizations()),
        // intentionally don't run this reader as async because it does
        // not work well while simultaneously deleting rows
        new RocksDBReader<>(getClient(), readerParams, false));
  }

  @Override
  public void delete(final DataIndexReaderParams readerParams) {
    final String typeName =
        readerParams.getInternalAdapterStore().getTypeName(readerParams.getAdapterId());
    deleteRowsFromDataIndex(readerParams.getDataIds(), readerParams.getAdapterId(), typeName);
  }

  public void deleteRowsFromDataIndex(
      final byte[][] dataIds,
      final short adapterId,
      final String typeName) {
    final RocksDBDataIndexTable table =
        RocksDBUtils.getDataIndexTable(getClient(), typeName, adapterId);
    Arrays.stream(dataIds).forEach(d -> table.delete(d));
    table.flush();
  }

  @Override
  public RowReader<GeoWaveRow> createReader(final RecordReaderParams readerParams) {
    return new RocksDBReader<>(getClient(), readerParams);
  }

  @Override
  public RowDeleter createRowDeleter(
      final String indexName,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final String... authorizations) {
    return new RocksDBRowDeleter(getClient(), adapterStore, internalAdapterStore, indexName);
  }

  private void close(final boolean invalidateCache) {
    RocksDBClientCache.getInstance().close(
        directory,
        visibilityEnabled,
        compactOnWrite,
        batchWriteSize,
        walOnBatchWrite,
        invalidateCache);
    if (invalidateCache) {
      client = null;
    }
  }

  /**
   * This is not a typical resource, it references a static RocksDB resource used by all DataStore
   * instances with common parameters. Closing this is only recommended when the JVM no longer needs
   * any connection to this RocksDB store with common parameters.
   */
  @Override
  public void close() {
    close(true);
  }

  @SuppressFBWarnings(justification = "This is intentional to avoid unnecessary sync")
  public RocksDBClient getClient() {
    if (client != null) {
      return client;
    } else {
      synchronized (CLIENT_MUTEX) {
        if (client == null) {
          client =
              RocksDBClientCache.getInstance().getClient(
                  directory,
                  visibilityEnabled,
                  compactOnWrite,
                  batchWriteSize,
                  walOnBatchWrite);
        }
        return client;
      }
    }
  }
}
