/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.operations;

import java.io.IOException;
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
import org.locationtech.geowave.datastore.redis.config.RedisOptions;
import org.locationtech.geowave.datastore.redis.util.RedisMapWrapper;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.locationtech.geowave.datastore.redis.util.RedissonClientCache;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import org.redisson.api.RKeys;
import org.redisson.api.RedissonClient;

public class RedisOperations implements MapReduceDataStoreOperations {
  private static final boolean READER_ASYNC = true;
  private final String gwNamespace;
  private final RedisOptions options;
  private final RedissonClient client;

  public RedisOperations(final RedisOptions options) {
    if ((options.getGeoWaveNamespace() == null) || options.getGeoWaveNamespace().equals("")) {
      gwNamespace = "geowave";
    } else {
      gwNamespace = options.getGeoWaveNamespace();
    }
    this.options = options;
    client = RedissonClientCache.getInstance().getClient(options.getAddress());
  }

  @Override
  public boolean indexExists(final String indexName) throws IOException {
    return true;
  }

  public boolean createIndex(final Index index) throws IOException {
    return true;
  }

  @Override
  public boolean metadataExists(final MetadataType type) throws IOException {
    return true;
  }

  private void deleteByPattern(final String pattern) {
    final RKeys keySet = client.getKeys();

    keySet.getKeysByPattern(pattern).forEach(k -> keySet.delete(k));
  }

  @Override
  public void deleteAll() throws Exception {
    deleteByPattern(gwNamespace + "_*");
  }

  @Override
  public boolean deleteAll(
      final String indexName,
      final String typeName,
      final Short adapterId,
      final String... additionalAuthorizations) {
    deleteByPattern(RedisUtils.getRowSetPrefix(gwNamespace, typeName, indexName) + "*");
    return true;
  }

  @Override
  public boolean ensureAuthorizations(final String clientUser, final String... authorizations) {
    return true;
  }

  @Override
  public RowWriter createWriter(final Index index, final InternalDataAdapter<?> adapter) {
    return new RedisWriter(
        client,
        options.getCompression(),
        gwNamespace,
        adapter.getTypeName(),
        index.getName(),
        RedisUtils.isSortByTime(adapter),
        options.getStoreOptions().isVisibilityEnabled());
  }

  @Override
  public RowWriter createDataIndexWriter(final InternalDataAdapter<?> adapter) {
    return new RedisDataIndexWriter(
        client,
        options.getSerialization(),
        options.getCompression(),
        gwNamespace,
        adapter.getTypeName(),
        options.getStoreOptions().isVisibilityEnabled());
  }

  @Override
  public MetadataWriter createMetadataWriter(final MetadataType metadataType) {
    return new RedisMetadataWriter(
        RedisUtils.getMetadataSet(
            client,
            options.getCompression(),
            gwNamespace,
            metadataType,
            options.getStoreOptions().isVisibilityEnabled()),
        MetadataType.STATS.equals(metadataType));
  }

  @Override
  public MetadataReader createMetadataReader(final MetadataType metadataType) {
    return new RedisMetadataReader(
        RedisUtils.getMetadataSet(
            client,
            options.getCompression(),
            gwNamespace,
            metadataType,
            options.getStoreOptions().isVisibilityEnabled()),
        metadataType);
  }

  @Override
  public MetadataDeleter createMetadataDeleter(final MetadataType metadataType) {
    return new RedisMetadataDeleter(
        RedisUtils.getMetadataSet(
            client,
            options.getCompression(),
            gwNamespace,
            metadataType,
            options.getStoreOptions().isVisibilityEnabled()),
        metadataType);
  }

  @Override
  public <T> RowReader<T> createReader(final ReaderParams<T> readerParams) {
    return new RedisReader<>(
        client,
        options.getCompression(),
        readerParams,
        gwNamespace,
        options.getStoreOptions().isVisibilityEnabled(),
        READER_ASYNC);
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
        new RedisReader<>(
            client,
            options.getCompression(),
            readerParams,
            gwNamespace,
            options.getStoreOptions().isVisibilityEnabled(),
            false));
  }

  @Override
  public RowReader<GeoWaveRow> createReader(final RecordReaderParams readerParams) {
    return new RedisReader<>(
        client,
        options.getCompression(),
        readerParams,
        gwNamespace,
        options.getStoreOptions().isVisibilityEnabled());
  }

  @Override
  public RowDeleter createRowDeleter(
      final String indexName,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final String... authorizations) {
    return new RedisRowDeleter(
        client,
        options.getCompression(),
        adapterStore,
        internalAdapterStore,
        indexName,
        gwNamespace,
        options.getStoreOptions().isVisibilityEnabled());
  }

  @Override
  public RowReader<GeoWaveRow> createReader(final DataIndexReaderParams readerParams) {
    return new RedisReader<>(
        client,
        options.getSerialization(),
        options.getCompression(),
        readerParams,
        gwNamespace,
        options.getStoreOptions().isVisibilityEnabled());
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
    final RedisMapWrapper map =
        RedisUtils.getDataIndexMap(
            client,
            options.getSerialization(),
            options.getCompression(),
            gwNamespace,
            typeName,
            options.getStoreOptions().isVisibilityEnabled());
    map.remove(dataIds);
  }
}
