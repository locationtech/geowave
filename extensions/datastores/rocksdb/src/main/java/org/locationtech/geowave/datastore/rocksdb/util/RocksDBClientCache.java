/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.util;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class RocksDBClientCache {
  private static Logger LOGGER = LoggerFactory.getLogger(RocksDBClientCache.class);
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
            subDirectoryVisiblityPair.batchSize,
            subDirectoryVisiblityPair.walOnBatchWrite);
      });
  private final Map<RocksDBClient, Integer> holds = new HashMap<>();

  protected RocksDBClientCache() {}

  public RocksDBClient getClient(
      final String directory,
      final boolean visibilityEnabled,
      final boolean compactOnWrite,
      final int batchWriteSize,
      final boolean walOnBatchWrite) {
    return clientCache.get(
        new ClientKey(
            directory,
            visibilityEnabled,
            compactOnWrite,
            batchWriteSize,
            walOnBatchWrite));
  }

  /** Keeps the client's databases open until the hold is given back with release(). */
  public synchronized void hold(final RocksDBClient client) {
    holds.merge(client, 1, Integer::sum);
  }

  /**
   * Gives back a hold. Once none remain, closes the client's databases, closing any iterators still
   * open on them first, and releases the options shared by all clients once no database is open.
   * The client stays cached, so that anything still using it reopens the same tables rather than
   * opening new ones beside them, which would fail on RocksDB's LOCK.
   */
  public synchronized void release(final RocksDBClient client) {
    final Integer held = holds.get(client);
    if (held == null) {
      // closeAll() has closed it already
      return;
    }
    if (held > 1) {
      holds.put(client, held - 1);
      return;
    }
    holds.remove(client);
    client.close();
    if (clientCache.asMap().values().stream().noneMatch(RocksDBClient::hasOpenTables)) {
      RocksDBClient.closeSharedOptions();
    }
  }

  /**
   * Closes the client's databases, whoever holds them, and forgets its tables, before its directory
   * is deleted.
   */
  public synchronized void closeAndForgetTables(
      final String directory,
      final boolean visibilityEnabled,
      final boolean compactOnWrite,
      final int batchWriteSize,
      final boolean walOnBatchWrite) {
    final RocksDBClient client =
        clientCache.getIfPresent(
            new ClientKey(
                directory,
                visibilityEnabled,
                compactOnWrite,
                batchWriteSize,
                walOnBatchWrite));
    if (client != null) {
      client.closeAndForgetTables();
    }
  }

  public synchronized void closeAll() {
    clientCache.asMap().forEach((k, v) -> v.closeAndForgetTables());
    clientCache.invalidateAll();
    holds.clear();
    RocksDBClient.closeSharedOptions();
  }

  private static class ClientKey {
    private final String directory;
    private final boolean visibilityEnabled;
    private final boolean compactOnWrite;;
    private final int batchSize;
    private final boolean walOnBatchWrite;

    public ClientKey(
        final String directory,
        final boolean visibilityEnabled,
        final boolean compactOnWrite,
        final int batchSize,
        final boolean walOnBatchWrite) {
      super();
      String path = directory;
      try {
        path = new File(directory).getCanonicalPath();
      } catch (final IOException e) {
        LOGGER.error("Error getting canonical path", e);
      }
      this.directory = path;
      this.visibilityEnabled = visibilityEnabled;
      this.compactOnWrite = compactOnWrite;
      this.batchSize = batchSize;
      this.walOnBatchWrite = walOnBatchWrite;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + batchSize;
      result = (prime * result) + (compactOnWrite ? 1231 : 1237);
      result = (prime * result) + ((directory == null) ? 0 : directory.hashCode());
      result = (prime * result) + (visibilityEnabled ? 1231 : 1237);
      result = (prime * result) + (walOnBatchWrite ? 1231 : 1237);
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
      if (walOnBatchWrite != other.walOnBatchWrite) {
        return false;
      }
      return true;
    }

  }
}
