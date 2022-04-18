/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.operations;

import java.time.Instant;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.datastore.redis.config.RedisOptions.Compression;
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisPersistedRow;
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisPersistedTimestampRow;
import org.locationtech.geowave.datastore.redis.util.RedisScoredSetWrapper;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.redisson.api.RedissonClient;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class RedisWriter implements RowWriter {
  private static ByteArray EMPTY_PARTITION_KEY = new ByteArray();
  private final RedissonClient client;
  private final Compression compression;
  private final String setNamePrefix;
  private final LoadingCache<ByteArray, RedisScoredSetWrapper<GeoWaveRedisPersistedRow>> setCache =
      Caffeine.newBuilder().build(partitionKey -> getSet(partitionKey.getBytes()));
  private final boolean isTimestampRequired;
  private final boolean visibilityEnabled;

  public RedisWriter(
      final RedissonClient client,
      final Compression compression,
      final String namespace,
      final String typeName,
      final String indexName,
      final boolean isTimestampRequired,
      final boolean visibilityEnabled) {
    this.client = client;
    this.compression = compression;
    setNamePrefix = RedisUtils.getRowSetPrefix(namespace, typeName, indexName);
    this.isTimestampRequired = isTimestampRequired;
    this.visibilityEnabled = visibilityEnabled;
  }

  private RedisScoredSetWrapper<GeoWaveRedisPersistedRow> getSet(final byte[] partitionKey) {
    return RedisUtils.getRowSet(
        client,
        compression,
        setNamePrefix,
        partitionKey,
        isTimestampRequired,
        visibilityEnabled);
  }

  @Override
  public void write(final GeoWaveRow[] rows) {
    if (rows.length == 1) {
      write(rows[0]);
    } else {
      // otherwise we should make sure we keep track of duplicates for uniqueness
      short duplicateId = 0;
      for (final GeoWaveRow row : rows) {
        internalWrite(row, duplicateId++);
      }
    }
  }

  @Override
  public void write(final GeoWaveRow row) {
    ByteArray partitionKey;
    if ((row.getPartitionKey() == null) || (row.getPartitionKey().length == 0)) {
      partitionKey = EMPTY_PARTITION_KEY;
    } else {
      partitionKey = new ByteArray(row.getPartitionKey());
    }
    for (final GeoWaveValue value : row.getFieldValues()) {
      setCache.get(partitionKey).add(
          RedisUtils.getScore(row.getSortKey()),
          isTimestampRequired
              ? new GeoWaveRedisPersistedTimestampRow(
                  (short) row.getNumberOfDuplicates(),
                  row.getDataId(),
                  row.getSortKey(),
                  value,
                  Instant.now())
              : new GeoWaveRedisPersistedRow(
                  (short) row.getNumberOfDuplicates(),
                  row.getDataId(),
                  row.getSortKey(),
                  value));
    }
  }

  private void internalWrite(GeoWaveRow row, Short duplicateId) {

    ByteArray partitionKey;
    if ((row.getPartitionKey() == null) || (row.getPartitionKey().length == 0)) {
      partitionKey = EMPTY_PARTITION_KEY;
    } else {
      partitionKey = new ByteArray(row.getPartitionKey());
    }
    for (final GeoWaveValue value : row.getFieldValues()) {
      setCache.get(partitionKey).add(
          RedisUtils.getScore(row.getSortKey()),
          isTimestampRequired
              ? new GeoWaveRedisPersistedTimestampRow(
                  (short) row.getNumberOfDuplicates(),
                  row.getDataId(),
                  row.getSortKey(),
                  value,
                  Instant.now(),
                  duplicateId)
              : new GeoWaveRedisPersistedRow(
                  (short) row.getNumberOfDuplicates(),
                  row.getDataId(),
                  row.getSortKey(),
                  value,
                  duplicateId));
    }
  }

  @Override
  public void flush() {
    setCache.asMap().forEach((k, v) -> v.flush());
  }

  @Override
  public void close() throws Exception {
    for (final RedisScoredSetWrapper<GeoWaveRedisPersistedRow> set : setCache.asMap().values()) {
      set.flush();
      set.close();
    }
  }
}
