/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis;

import java.util.List;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.BaseQueryOptions;
import org.locationtech.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import org.locationtech.geowave.core.store.metadata.AdapterStoreImpl;
import org.locationtech.geowave.core.store.metadata.DataStatisticsStoreImpl;
import org.locationtech.geowave.core.store.metadata.IndexStoreImpl;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.core.store.query.constraints.InsertionIdQuery;
import org.locationtech.geowave.core.store.query.filter.DedupeFilter;
import org.locationtech.geowave.datastore.redis.operations.RedisOperations;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.locationtech.geowave.mapreduce.BaseMapReduceDataStore;

public class RedisDataStore extends BaseMapReduceDataStore {
  public RedisDataStore(final RedisOperations operations, final DataStoreOptions options) {
    super(
        new IndexStoreImpl(operations, options),
        new AdapterStoreImpl(operations, options),
        new DataStatisticsStoreImpl(operations, options),
        new AdapterIndexMappingStoreImpl(operations, options),
        operations,
        options,
        new InternalAdapterStoreImpl(operations));
  }

  @Override
  protected CloseableIterator<Object> queryInsertionId(
      final InternalDataAdapter<?> adapter,
      final Index index,
      final InsertionIdQuery query,
      final DedupeFilter filter,
      final BaseQueryOptions sanitizedQueryOptions,
      final PersistentAdapterStore tempAdapterStore,
      final boolean delete) {
    // ensure Redis 52 bits of precision limitation due to double floating point precision doesn't
    // impact the sort key
    return super.queryInsertionId(
        adapter,
        index,
        new InsertionIdQuery(
            query.getPartitionKey(),
            RedisUtils.getSortKey(RedisUtils.getScore(query.getSortKey())),
            query.getDataId()),
        filter,
        sanitizedQueryOptions,
        tempAdapterStore,
        delete);
  }

  @Override
  protected CloseableIterator<Object> queryRowPrefix(
      final Index index,
      final byte[] partitionKey,
      final byte[] sortPrefix,
      final BaseQueryOptions sanitizedQueryOptions,
      final List<InternalDataAdapter<?>> adapters,
      final PersistentAdapterStore tempAdapterStore,
      final boolean delete) {
    // ensure Redis 52 bits of precision limitation due to double floating point precision doesn't
    // impact the sort key
    return super.queryRowPrefix(
        index,
        partitionKey,
        RedisUtils.getSortKey(RedisUtils.getScore(sortPrefix)),
        sanitizedQueryOptions,
        adapters,
        tempAdapterStore,
        delete);
  }
}
