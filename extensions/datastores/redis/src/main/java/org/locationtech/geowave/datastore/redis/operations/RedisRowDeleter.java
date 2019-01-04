/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.operations;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.Arrays;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.datastore.redis.config.RedisOptions.Compression;
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisPersistedRow;
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisRow;
import org.locationtech.geowave.datastore.redis.util.RedisScoredSetWrapper;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.redisson.api.RedissonClient;

public class RedisRowDeleter implements RowDeleter {

  private final LoadingCache<Pair<String, Short>, RedisScoredSetWrapper<GeoWaveRedisPersistedRow>> setCache =
      Caffeine.newBuilder().build(nameAndAdapterId -> getSet(nameAndAdapterId));
  private final RedissonClient client;
  private final Compression compression;
  private final PersistentAdapterStore adapterStore;
  private final InternalAdapterStore internalAdapterStore;
  private final String indexName;
  private final String namespace;

  public RedisRowDeleter(
      final RedissonClient client,
      final Compression compression,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final String indexName,
      final String namespace) {
    this.client = client;
    this.compression = compression;
    this.adapterStore = adapterStore;
    this.internalAdapterStore = internalAdapterStore;
    this.indexName = indexName;
    this.namespace = namespace;
  }

  @Override
  public void close() throws Exception {}

  private RedisScoredSetWrapper<GeoWaveRedisPersistedRow> getSet(
      final Pair<String, Short> setNameAndAdapterId) {
    return RedisUtils.getRowSet(
        client,
        compression,
        setNameAndAdapterId.getLeft(),
        RedisUtils.isSortByTime(adapterStore.getAdapter(setNameAndAdapterId.getRight())));
  }

  @Override
  public void delete(final GeoWaveRow row) {
    final RedisScoredSetWrapper<GeoWaveRedisPersistedRow> set =
        setCache.get(
            Pair.of(
                RedisUtils.getRowSetName(
                    namespace,
                    internalAdapterStore.getTypeName(row.getAdapterId()),
                    indexName,
                    row.getPartitionKey()),
                row.getAdapterId()));
    Arrays.stream(((GeoWaveRedisRow) row).getPersistedRows()).forEach(r -> set.remove(r));
  }

  @Override
  public void flush() {}
}
