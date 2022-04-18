/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.operations;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.base.dataidx.DefaultDataIndexRowWriterWrapper;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Bytes;

public interface DataStoreOperations {

  boolean indexExists(String indexName) throws IOException;

  boolean metadataExists(MetadataType type) throws IOException;

  void deleteAll() throws Exception;

  boolean deleteAll(
      String indexName,
      String typeName,
      Short adapterId,
      String... additionalAuthorizations);

  RowWriter createWriter(Index index, InternalDataAdapter<?> adapter);

  default RowWriter createDataIndexWriter(final InternalDataAdapter<?> adapter) {
    return new DefaultDataIndexRowWriterWrapper(
        createWriter(DataIndexUtils.DATA_ID_INDEX, adapter));
  }

  default boolean ensureAuthorizations(final String clientUser, final String... authorizations) {
    return true;
  }

  default boolean clearAuthorizations(final String clientUser) {
    return true;
  }

  MetadataWriter createMetadataWriter(MetadataType metadataType);

  MetadataReader createMetadataReader(MetadataType metadataType);

  MetadataDeleter createMetadataDeleter(MetadataType metadataType);

  <T> RowReader<T> createReader(ReaderParams<T> readerParams);

  default RowReader<GeoWaveRow> createReader(final DataIndexReaderParams readerParams) {
    final List<RowReader<GeoWaveRow>> readers;
    if (readerParams.getDataIds() != null) {
      readers = Arrays.stream(readerParams.getDataIds()).map(dataId -> {
        final byte[] sortKey = Bytes.concat(new byte[] {(byte) dataId.length}, dataId);
        return createReader(
            new ReaderParams<>(
                DataIndexUtils.DATA_ID_INDEX,
                readerParams.getAdapterStore(),
                readerParams.getAdapterIndexMappingStore(),
                readerParams.getInternalAdapterStore(),
                new short[] {readerParams.getAdapterId()},
                null,
                readerParams.getAggregation(),
                readerParams.getFieldSubsets(),
                false,
                false,
                false,
                false,
                new QueryRanges(new ByteArrayRange(sortKey, sortKey, false)),
                null,
                1,
                null,
                null,
                null,
                GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER,
                new String[0]));
      }).collect(Collectors.toList());
    } else {
      final byte[] startKey =
          Bytes.concat(
              new byte[] {(byte) readerParams.getStartInclusiveDataId().length},
              readerParams.getStartInclusiveDataId());
      final byte[] endKey =
          Bytes.concat(
              new byte[] {(byte) readerParams.getEndInclusiveDataId().length},
              readerParams.getEndInclusiveDataId());
      readers =
          Collections.singletonList(
              createReader(
                  new ReaderParams<>(
                      DataIndexUtils.DATA_ID_INDEX,
                      readerParams.getAdapterStore(),
                      readerParams.getAdapterIndexMappingStore(),
                      readerParams.getInternalAdapterStore(),
                      new short[] {readerParams.getAdapterId()},
                      null,
                      readerParams.getAggregation(),
                      readerParams.getFieldSubsets(),
                      false,
                      false,
                      false,
                      false,
                      new QueryRanges(new ByteArrayRange(startKey, endKey, false)),
                      null,
                      1,
                      null,
                      null,
                      null,
                      GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER,
                      new String[0])));
    }
    return new RowReaderWrapper<>(new CloseableIteratorWrapper(new Closeable() {
      @Override
      public void close() {
        for (final RowReader<GeoWaveRow> r : readers) {
          r.close();
        }
      }
    }, Iterators.concat(readers.iterator())));
  }

  default <T> Deleter<T> createDeleter(final ReaderParams<T> readerParams) {
    return new QueryAndDeleteByRow<>(
        createRowDeleter(
            readerParams.getIndex().getName(),
            readerParams.getAdapterStore(),
            readerParams.getInternalAdapterStore(),
            readerParams.getAdditionalAuthorizations()),
        createReader(readerParams));
  }

  default void delete(final DataIndexReaderParams readerParams) {
    try (QueryAndDeleteByRow<GeoWaveRow> defaultDeleter =
        new QueryAndDeleteByRow<>(
            createRowDeleter(
                DataIndexUtils.DATA_ID_INDEX.getName(),
                readerParams.getAdapterStore(),
                readerParams.getInternalAdapterStore()),
            createReader(readerParams))) {
      while (defaultDeleter.hasNext()) {
        defaultDeleter.next();
      }
    }
  }

  RowDeleter createRowDeleter(
      String indexName,
      PersistentAdapterStore adapterStore,
      InternalAdapterStore internalAdapterStore,
      String... authorizations);

  default boolean mergeData(
      final Index index,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final AdapterIndexMappingStore adapterIndexMappingStore,
      final Integer maxRangeDecomposition) {
    return DataStoreUtils.mergeData(
        this,
        maxRangeDecomposition,
        index,
        adapterStore,
        internalAdapterStore,
        adapterIndexMappingStore);
  }


  default boolean mergeStats(final DataStatisticsStore statsStore) {
    return statsStore.mergeStats();
  }
}

