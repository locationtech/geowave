/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.util;

import java.io.Closeable;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class RocksDBClient implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBClient.class);

  private static class CacheKey {
    protected final String directory;
    protected final boolean requiresTimestamp;

    public CacheKey(final String directory, final boolean requiresTimestamp) {
      this.directory = directory;
      this.requiresTimestamp = requiresTimestamp;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + ((directory == null) ? 0 : directory.hashCode());
      result = (prime * result) + (requiresTimestamp ? 1231 : 1237);
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
      if (directory == null) {
        if (other.directory != null) {
          return false;
        }
      } else if (!directory.equals(other.directory)) {
        return false;
      }
      if (requiresTimestamp != other.requiresTimestamp) {
        return false;
      }
      return true;
    }

  }

  private static class IndexCacheKey extends DataIndexCacheKey {
    protected final byte[] partition;

    public IndexCacheKey(
        final String directory,
        final short adapterId,
        final byte[] partition,
        final boolean requiresTimestamp) {
      super(directory, requiresTimestamp, adapterId);
      this.partition = partition;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = (prime * result) + adapterId;
      result = (prime * result) + Arrays.hashCode(partition);
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (!super.equals(obj)) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final IndexCacheKey other = (IndexCacheKey) obj;
      if (adapterId != other.adapterId) {
        return false;
      }
      if (!Arrays.equals(partition, other.partition)) {
        return false;
      }
      return true;
    }
  }
  private static class DataIndexCacheKey extends CacheKey {
    protected final short adapterId;

    public DataIndexCacheKey(final String directory, final short adapterId) {
      super(directory, false);
      this.adapterId = adapterId;
    }

    private DataIndexCacheKey(
        final String directory,
        final boolean requiresTimestamp,
        final short adapterId) {
      super(directory, requiresTimestamp);
      this.adapterId = adapterId;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = (prime * result) + adapterId;
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (!super.equals(obj)) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final DataIndexCacheKey other = (DataIndexCacheKey) obj;
      if (adapterId != other.adapterId) {
        return false;
      }
      return true;
    }
  }

  private final Cache<String, CacheKey> keyCache = Caffeine.newBuilder().build();
  private final LoadingCache<IndexCacheKey, RocksDBIndexTable> indexTableCache =
      Caffeine.newBuilder().build(key -> loadIndexTable(key));

  private final LoadingCache<DataIndexCacheKey, RocksDBDataIndexTable> dataIndexTableCache =
      Caffeine.newBuilder().build(key -> loadDataIndexTable(key));
  private final LoadingCache<CacheKey, RocksDBMetadataTable> metadataTableCache =
      Caffeine.newBuilder().build(key -> loadMetadataTable(key));
  private final String subDirectory;
  private final boolean visibilityEnabled;
  private final boolean compactOnWrite;
  private final int batchWriteSize;
  private final boolean walOnBatchWrite;

  // Only ever used under the class lock: RocksDB copies what it needs when a database opens, so
  // holding the lock for the open is enough to keep closeSharedOptions() from freeing them mid-use.
  private static Options indexOptions = null;
  private static Options metadataOptions = null;

  public RocksDBClient(
      final String subDirectory,
      final boolean visibilityEnabled,
      final boolean compactOnWrite,
      final int batchWriteSize,
      final boolean walOnBatchWrite) {
    this.subDirectory = subDirectory;
    this.visibilityEnabled = visibilityEnabled;
    this.compactOnWrite = compactOnWrite;
    this.batchWriteSize = batchWriteSize;
    this.walOnBatchWrite = walOnBatchWrite;
  }

  static synchronized RocksDB openIndexDb(final String directory) throws RocksDBException {
    if (indexOptions == null) {
      RocksDB.loadLibrary();
      final int cores = Runtime.getRuntime().availableProcessors();
      indexOptions =
          new Options().setCreateIfMissing(true).prepareForBulkLoad().setIncreaseParallelism(cores);
    }
    return RocksDB.open(indexOptions, directory);
  }

  static synchronized RocksDB openMetadataDb(final String directory) throws RocksDBException {
    if (metadataOptions == null) {
      RocksDB.loadLibrary();
      metadataOptions = new Options().setCreateIfMissing(true).optimizeForSmallDb();
    }
    return RocksDB.open(metadataOptions, directory);
  }

  static synchronized void closeSharedOptions() {
    if (metadataOptions != null) {
      metadataOptions.close();
      metadataOptions = null;
    }
    if (indexOptions != null) {
      indexOptions.close();
      indexOptions = null;
    }
  }

  private RocksDBMetadataTable loadMetadataTable(final CacheKey key) throws RocksDBException {
    final File dir = new File(key.directory);
    if (!dir.exists() && !dir.mkdirs()) {
      LOGGER.error("Unable to create directory for rocksdb store '" + key.directory + "'");
    }
    return new RocksDBMetadataTable(
        key.directory,
        openMetadataDb(key.directory),
        key.requiresTimestamp,
        visibilityEnabled,
        compactOnWrite);
  }

  private RocksDBIndexTable loadIndexTable(final IndexCacheKey key) {
    return new RocksDBIndexTable(
        key.directory,
        key.adapterId,
        key.partition,
        key.requiresTimestamp,
        visibilityEnabled,
        compactOnWrite,
        batchWriteSize,
        walOnBatchWrite);
  }

  private RocksDBDataIndexTable loadDataIndexTable(final DataIndexCacheKey key) {
    return new RocksDBDataIndexTable(
        key.directory,
        key.adapterId,
        visibilityEnabled,
        compactOnWrite,
        batchWriteSize,
        walOnBatchWrite);
  }

  public String getSubDirectory() {
    return subDirectory;
  }

  public synchronized RocksDBIndexTable getIndexTable(
      final String tableName,
      final short adapterId,
      final byte[] partition,
      final boolean requiresTimestamp) {
    final String directory = subDirectory + "/" + tableName;
    return indexTableCache.get(
        (IndexCacheKey) keyCache.get(
            directory,
            d -> new IndexCacheKey(d, adapterId, partition, requiresTimestamp)));
  }

  public synchronized RocksDBDataIndexTable getDataIndexTable(
      final String tableName,
      final short adapterId) {
    final String directory = subDirectory + "/" + tableName;
    return dataIndexTableCache.get(
        (DataIndexCacheKey) keyCache.get(directory, d -> new DataIndexCacheKey(d, adapterId)));
  }

  public synchronized RocksDBMetadataTable getMetadataTable(final MetadataType type) {
    final String directory = subDirectory + "/" + type.id();
    return metadataTableCache.get(
        keyCache.get(directory, d -> new CacheKey(d, type.isStatValues())));
  }

  public boolean indexTableExists(final String indexName) {
    // then look for prefixes of this index directory in which case there is
    // a partition key
    for (final String key : keyCache.asMap().keySet()) {
      if (key.substring(subDirectory.length()).contains(indexName)) {
        return true;
      }
    }
    // this could have been created by a different process so check the
    // directory listing
    final String[] listing = new File(subDirectory).list((dir, name) -> name.contains(indexName));
    return (listing != null) && (listing.length > 0);
  }

  public boolean metadataTableExists(final MetadataType type) {
    // this could have been created by a different process so check the
    // directory listing
    return (keyCache.getIfPresent(subDirectory + "/" + type.id()) != null)
        || new File(subDirectory + "/" + type.id()).exists();
  }

  public void close(final String indexName, final String typeName) {
    final String prefix = RocksDBUtils.getTablePrefix(typeName, indexName);
    for (final Entry<String, CacheKey> e : keyCache.asMap().entrySet()) {
      final String key = e.getKey();
      if (key.substring(subDirectory.length() + 1).startsWith(prefix)) {
        keyCache.invalidate(key);
        AbstractRocksDBTable indexTable = indexTableCache.getIfPresent(e.getValue());
        if (indexTable == null) {
          indexTable = dataIndexTableCache.getIfPresent(e.getValue());
        }
        if (indexTable != null) {
          indexTableCache.invalidate(e.getValue());
          dataIndexTableCache.invalidate(e.getValue());
          indexTable.close();
        }
      }
    }
  }

  public boolean isCompactOnWrite() {
    return compactOnWrite;
  }

  public boolean isVisibilityEnabled() {
    return visibilityEnabled;
  }

  public List<RocksDBIndexTable> getIndexTables(final Predicate<RocksDBIndexTable> filter) {
    return indexTableCache.asMap().values().stream().filter(filter).collect(Collectors.toList());
  }

  public List<RocksDBDataIndexTable> getDataIndexTables(
      final Predicate<RocksDBDataIndexTable> filter) {
    return dataIndexTableCache.asMap().values().stream().filter(filter).collect(
        Collectors.toList());
  }

  public List<RocksDBMetadataTable> getMetadataTables(
      final Predicate<RocksDBMetadataTable> filter) {
    return metadataTableCache.asMap().values().stream().filter(filter).collect(Collectors.toList());
  }

  public void mergeData() {
    indexTableCache.asMap().values().parallelStream().forEach(db -> db.compact());
    dataIndexTableCache.asMap().values().parallelStream().forEach(db -> db.compact());
  }

  public void mergeMetadata() {
    metadataTableCache.asMap().values().parallelStream().forEach(db -> db.compact());
  }

  boolean hasOpenTables() {
    return indexTableCache.asMap().values().stream().anyMatch(AbstractRocksDBTable::isOpen)
        || dataIndexTableCache.asMap().values().stream().anyMatch(AbstractRocksDBTable::isOpen)
        || metadataTableCache.asMap().values().stream().anyMatch(RocksDBMetadataTable::isOpen);
  }

  /**
   * Closes every table's database. Other DataStore instances on this directory share this client
   * and may still hold its tables, so the tables stay registered and reopen when they are used
   * again.
   */
  @Override
  public void close() {
    indexTableCache.asMap().values().forEach(db -> db.close());
    dataIndexTableCache.asMap().values().forEach(db -> db.close());
    metadataTableCache.asMap().values().forEach(db -> db.close());
  }

  /** Closes and forgets every table, for when their directories are about to be deleted. */
  void closeAndForgetTables() {
    keyCache.invalidateAll();
    close();
    indexTableCache.invalidateAll();
    dataIndexTableCache.invalidateAll();
    metadataTableCache.invalidateAll();
  }
}
