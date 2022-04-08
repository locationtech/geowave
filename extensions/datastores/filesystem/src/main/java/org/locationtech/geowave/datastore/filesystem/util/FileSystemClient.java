/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.store.operations.MetadataType;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class FileSystemClient {
  private abstract static class CacheKey {
    protected final boolean requiresTimestamp;

    public CacheKey(final boolean requiresTimestamp) {
      this.requiresTimestamp = requiresTimestamp;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
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
      if (requiresTimestamp != other.requiresTimestamp) {
        return false;
      }
      return true;
    }

  }
  private static class MetadataCacheKey extends CacheKey {
    protected final MetadataType type;

    public MetadataCacheKey(final MetadataType type) {
      // stat values also store a timestamp because they can be the exact same but
      // need to still be unique (consider multiple count statistics that are
      // exactly the same count, but need to be merged)
      super(type.isStatValues());
      this.type = type;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = (prime * result) + ((type == null) ? 0 : type.hashCode());
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
      final MetadataCacheKey other = (MetadataCacheKey) obj;
      if (type != other.type) {
        return false;
      }
      return true;
    }
  }
  private static class IndexCacheKey extends DataIndexCacheKey {
    protected final String indexName;
    protected final byte[] partition;

    public IndexCacheKey(
        final short adapterId,
        final String typeName,
        final String indexName,
        final byte[] partition,
        final boolean requiresTimestamp) {
      super(requiresTimestamp, adapterId, typeName);
      this.partition = partition;
      this.indexName = indexName;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = (prime * result) + ((indexName == null) ? 0 : indexName.hashCode());
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
      if (indexName == null) {
        if (other.indexName != null) {
          return false;
        }
      } else if (!indexName.equals(other.indexName)) {
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
    protected final String typeName;

    public DataIndexCacheKey(final short adapterId, final String typeName) {
      this(false, adapterId, typeName);
    }

    private DataIndexCacheKey(
        final boolean requiresTimestamp,
        final short adapterId,
        final String typeName) {
      super(requiresTimestamp);
      this.adapterId = adapterId;
      this.typeName = typeName;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = (prime * result) + adapterId;
      result = (prime * result) + ((typeName == null) ? 0 : typeName.hashCode());
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
      if (typeName == null) {
        if (other.typeName != null) {
          return false;
        }
      } else if (!typeName.equals(other.typeName)) {
        return false;
      }
      return true;
    }


  }

  private final LoadingCache<IndexCacheKey, FileSystemIndexTable> indexTableCache =
      Caffeine.newBuilder().build(key -> loadIndexTable(key));

  private final LoadingCache<DataIndexCacheKey, FileSystemDataIndexTable> dataIndexTableCache =
      Caffeine.newBuilder().build(key -> loadDataIndexTable(key));
  private final LoadingCache<MetadataCacheKey, FileSystemMetadataTable> metadataTableCache =
      Caffeine.newBuilder().build(key -> loadMetadataTable(key));
  private final String subDirectory;
  private final boolean visibilityEnabled;
  private final String format;

  public FileSystemClient(
      final String subDirectory,
      final String format,
      final boolean visibilityEnabled) {
    this.subDirectory = subDirectory;
    this.visibilityEnabled = visibilityEnabled;
    this.format = format;
  }

  private FileSystemMetadataTable loadMetadataTable(final MetadataCacheKey key) throws IOException {
    Path dir =
        FileSystemUtils.getMetadataTablePath(subDirectory, format, visibilityEnabled, key.type);
    if (!Files.exists(dir)) {
      dir = Files.createDirectories(dir);
    }
    return new FileSystemMetadataTable(dir, key.requiresTimestamp, visibilityEnabled);
  }

  private FileSystemIndexTable loadIndexTable(final IndexCacheKey key) throws IOException {
    return new FileSystemIndexTable(
        subDirectory,
        key.adapterId,
        key.typeName,
        key.indexName,
        key.partition,
        format,
        key.requiresTimestamp,
        visibilityEnabled);
  }

  private FileSystemDataIndexTable loadDataIndexTable(final DataIndexCacheKey key)
      throws IOException {
    return new FileSystemDataIndexTable(
        subDirectory,
        key.adapterId,
        key.typeName,
        format,
        visibilityEnabled);
  }

  public String getSubDirectory() {
    return subDirectory;
  }

  public synchronized FileSystemIndexTable getIndexTable(
      final short adapterId,
      final String typeName,
      final String indexName,
      final byte[] partition,
      final boolean requiresTimestamp) {
    return indexTableCache.get(
        new IndexCacheKey(adapterId, typeName, indexName, partition, requiresTimestamp));
  }

  public synchronized FileSystemDataIndexTable getDataIndexTable(
      final short adapterId,
      final String typeName) {
    return dataIndexTableCache.get(new DataIndexCacheKey(adapterId, typeName));
  }

  public synchronized FileSystemMetadataTable getMetadataTable(final MetadataType type) {
    return metadataTableCache.get(new MetadataCacheKey(type));
  }

  public boolean metadataTableExists(final MetadataType type) {
    // this could have been created by a different process so check the
    // directory listing
    return (metadataTableCache.getIfPresent(new MetadataCacheKey(type)) != null)
        || Files.exists(
            FileSystemUtils.getMetadataTablePath(subDirectory, format, visibilityEnabled, type));
  }

  public void invalidateDataIndexCache(final short adapterId, final String typeName) {
    dataIndexTableCache.invalidate(new DataIndexCacheKey(adapterId, typeName));
  }

  public void invalidateIndexCache(final String indexName, final String typeName) {
    indexTableCache.invalidateAll(
        indexTableCache.asMap().keySet().stream().filter(
            k -> k.typeName.equals(typeName) && k.indexName.equals(indexName)).collect(
                Collectors.toList()));
  }

  public boolean isVisibilityEnabled() {
    return visibilityEnabled;
  }

  public String getFormat() {
    return format;
  }
}
