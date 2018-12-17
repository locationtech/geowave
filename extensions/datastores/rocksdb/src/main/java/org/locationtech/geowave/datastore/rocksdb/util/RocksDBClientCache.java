/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.util;

import org.apache.commons.lang3.tuple.Pair;
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

  private final LoadingCache<Pair<String, Boolean>, RocksDBClient> clientCache =
      Caffeine.newBuilder().build(subDirectoryVisiblityPair -> {
        return new RocksDBClient(
            subDirectoryVisiblityPair.getLeft(),
            subDirectoryVisiblityPair.getRight());
      });

  protected RocksDBClientCache() {}

  public RocksDBClient getClient(final String directory, final boolean visibilityEnabled) {
    return clientCache.get(Pair.of(directory, visibilityEnabled));
  }

  public synchronized void close(final String directory, final boolean visibilityEnabled) {
    final Pair<String, Boolean> key = Pair.of(directory, visibilityEnabled);
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
  }
}
