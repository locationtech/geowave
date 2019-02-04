/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.util;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class RocksDBClientCache {
  private static RocksDBClientCache singletonInstance;

  public static synchronized RocksDBClientCache getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new RocksDBClientCache();
    }
    return singletonInstance;
  }

  private final LoadingCache<ClientKey, RocksDBClient> clientCache =
      Caffeine.newBuilder().build(subDirectoryVisiblityPair -> {
        return new RocksDBClient(
            subDirectoryVisiblityPair.directory,
            subDirectoryVisiblityPair.visibilityEnabled,
            subDirectoryVisiblityPair.compactOnWrite,
            subDirectoryVisiblityPair.batchSize);
      });

  protected RocksDBClientCache() {}

  public RocksDBClient getClient(
      final String directory,
      final boolean visibilityEnabled,
      final boolean compactOnWrite,
      final int batchWriteSize) {
    return clientCache.get(
        new ClientKey(directory, visibilityEnabled, compactOnWrite, batchWriteSize));
  }

  public synchronized void close(
      final String directory,
      final boolean visibilityEnabled,
      final boolean compactOnWrite,
      final int batchWriteSize) {
    final ClientKey key =
        new ClientKey(directory, visibilityEnabled, compactOnWrite, batchWriteSize);
    final RocksDBClient client = clientCache.getIfPresent(key);
    if (client != null) {
      clientCache.invalidate(key);
      client.close();
    }
    if (clientCache.estimatedSize() == 0) {
      if (RocksDBClient.metadataOptions != null) {
        RocksDBClient.metadataOptions.close();
        RocksDBClient.metadataOptions = null;
      }
      if (RocksDBClient.indexWriteOptions != null) {
        RocksDBClient.indexWriteOptions.close();
        RocksDBClient.indexWriteOptions = null;
      }
      if (RocksDBClient.indexReadOptions != null) {
        RocksDBClient.indexReadOptions.close();
        RocksDBClient.indexReadOptions = null;
      }
      if (RocksDBClient.batchWriteOptions != null) {
        RocksDBClient.batchWriteOptions.close();
        RocksDBClient.batchWriteOptions = null;
      }
    }
  }

  public synchronized void closeAll() {
    clientCache.asMap().forEach((k, v) -> v.close());
    clientCache.invalidateAll();
    if (RocksDBClient.metadataOptions != null) {
      RocksDBClient.metadataOptions.close();
      RocksDBClient.metadataOptions = null;
    }
    if (RocksDBClient.indexWriteOptions != null) {
      RocksDBClient.indexWriteOptions.close();
      RocksDBClient.indexWriteOptions = null;
    }
    if (RocksDBClient.indexReadOptions != null) {
      RocksDBClient.indexReadOptions.close();
      RocksDBClient.indexReadOptions = null;
    }
    if (RocksDBClient.batchWriteOptions != null) {
      RocksDBClient.batchWriteOptions.close();
      RocksDBClient.batchWriteOptions = null;
    }
  }

  private static class ClientKey {
    private final String directory;
    private final boolean visibilityEnabled;
    private final boolean compactOnWrite;;
    private final int batchSize;

    public ClientKey(
        final String directory,
        final boolean visibilityEnabled,
        final boolean compactOnWrite,
        final int batchSize) {
      super();
      this.directory = directory;
      this.visibilityEnabled = visibilityEnabled;
      this.compactOnWrite = compactOnWrite;
      this.batchSize = batchSize;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + batchSize;
      result = (prime * result) + (compactOnWrite ? 1231 : 1237);
      result = (prime * result) + ((directory == null) ? 0 : directory.hashCode());
      result = (prime * result) + (visibilityEnabled ? 1231 : 1237);
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
      final ClientKey other = (ClientKey) obj;
      if (batchSize != other.batchSize) {
        return false;
      }
      if (compactOnWrite != other.compactOnWrite) {
        return false;
      }
      if (directory == null) {
        if (other.directory != null) {
          return false;
        }
      } else if (!directory.equals(other.directory)) {
        return false;
      }
      if (visibilityEnabled != other.visibilityEnabled) {
        return false;
      }
      return true;
    }
  }
}
