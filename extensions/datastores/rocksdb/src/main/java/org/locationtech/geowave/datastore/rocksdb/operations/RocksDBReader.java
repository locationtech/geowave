/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.operations;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.entities.GeoWaveRowMergingIterator;
import org.locationtech.geowave.core.store.operations.DataIndexReaderParams;
import org.locationtech.geowave.core.store.operations.RangeReaderParams;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.query.filter.ClientVisibilityFilter;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBClient;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBDataIndexTable;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBUtils;
import org.locationtech.geowave.mapreduce.splits.GeoWaveRowRange;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

public class RocksDBReader<T> implements RowReader<T> {
  private final CloseableIterator<T> iterator;

  public RocksDBReader(
      final RocksDBClient client,
      final ReaderParams<T> readerParams,
      final boolean async) {
    this.iterator =
        createIteratorForReader(client, readerParams, readerParams.getRowTransformer(), false);
  }

  public RocksDBReader(final RocksDBClient client, final RecordReaderParams recordReaderParams) {
    this.iterator = createIteratorForRecordReader(client, recordReaderParams);
  }

  public RocksDBReader(
      final RocksDBClient client,
      final DataIndexReaderParams dataIndexReaderParams) {
    this.iterator = new Wrapper(createIteratorForDataIndexReader(client, dataIndexReaderParams));
  }

  private CloseableIterator<T> createIteratorForReader(
      final RocksDBClient client,
      final ReaderParams<T> readerParams,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final boolean async) {
    final Collection<SinglePartitionQueryRanges> ranges =
        readerParams.getQueryRanges().getPartitionQueryRanges();

    final Set<String> authorizations = Sets.newHashSet(readerParams.getAdditionalAuthorizations());
    if ((ranges != null) && !ranges.isEmpty()) {
      return createIterator(
          client,
          readerParams,
          readerParams.getRowTransformer(),
          ranges,
          authorizations,
          async);
    } else {
      final List<CloseableIterator<GeoWaveRow>> iterators = new ArrayList<>();
      for (final short adapterId : readerParams.getAdapterIds()) {
        final Pair<Boolean, Boolean> groupByRowAndSortByTime =
            RocksDBUtils.isGroupByRowAndIsSortByTime(readerParams, adapterId);
        final String indexNamePrefix =
            RocksDBUtils.getTablePrefix(
                readerParams.getInternalAdapterStore().getTypeName(adapterId),
                readerParams.getIndex().getName());
        final Stream<CloseableIterator<GeoWaveRow>> streamIt =
            RocksDBUtils.getPartitions(client.getSubDirectory(), indexNamePrefix).stream().map(
                p -> RocksDBUtils.getIndexTableFromPrefix(
                    client,
                    indexNamePrefix,
                    adapterId,
                    p.getBytes(),
                    groupByRowAndSortByTime.getRight()).iterator());
        iterators.addAll(streamIt.collect(Collectors.toList()));
      }
      return wrapResults(new Closeable() {
        AtomicBoolean closed = new AtomicBoolean(false);

        @Override
        public void close() throws IOException {
          if (!closed.getAndSet(true)) {
            iterators.forEach(it -> it.close());
          }
        }
      },
          Iterators.concat(iterators.iterator()),
          readerParams,
          rowTransformer,
          authorizations,
          client.isVisibilityEnabled());
    }
  }

  private CloseableIterator<T> createIterator(
      final RocksDBClient client,
      final RangeReaderParams<T> readerParams,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final Collection<SinglePartitionQueryRanges> ranges,
      final Set<String> authorizations,
      final boolean async) {
    final Iterator<CloseableIterator> it =
        Arrays.stream(ArrayUtils.toObject(readerParams.getAdapterIds())).map(
            adapterId -> new RocksDBQueryExecution(
                client,
                RocksDBUtils.getTablePrefix(
                    readerParams.getInternalAdapterStore().getTypeName(adapterId),
                    readerParams.getIndex().getName()),
                adapterId,
                rowTransformer,
                ranges,
                new ClientVisibilityFilter(authorizations),
                DataStoreUtils.isMergingIteratorRequired(
                    readerParams,
                    client.isVisibilityEnabled()),
                async,
                RocksDBUtils.isGroupByRowAndIsSortByTime(readerParams, adapterId),
                RocksDBUtils.isSortByKeyRequired(readerParams)).results()).iterator();
    final CloseableIterator<T>[] itArray = Iterators.toArray(it, CloseableIterator.class);
    return new CloseableIteratorWrapper<>(new Closeable() {
      AtomicBoolean closed = new AtomicBoolean(false);

      @Override
      public void close() throws IOException {
        if (!closed.getAndSet(true)) {
          Arrays.stream(itArray).forEach(it -> it.close());
        }
      }
    }, Iterators.concat(itArray));
  }

  private CloseableIterator<T> createIteratorForRecordReader(
      final RocksDBClient client,
      final RecordReaderParams recordReaderParams) {
    final GeoWaveRowRange range = recordReaderParams.getRowRange();
    final byte[] startKey = range.isInfiniteStartSortKey() ? null : range.getStartSortKey();
    final byte[] stopKey = range.isInfiniteStopSortKey() ? null : range.getEndSortKey();
    final SinglePartitionQueryRanges partitionRange =
        new SinglePartitionQueryRanges(
            range.getPartitionKey(),
            Collections.singleton(new ByteArrayRange(startKey, stopKey)));
    final Set<String> authorizations =
        Sets.newHashSet(recordReaderParams.getAdditionalAuthorizations());
    return createIterator(
        client,
        (RangeReaderParams<T>) recordReaderParams,
        (GeoWaveRowIteratorTransformer<T>) GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER,
        Collections.singleton(partitionRange),
        authorizations,
        // there should already be sufficient parallelism created by
        // input splits for record reader use cases
        false);
  }

  private Iterator<GeoWaveRow> createIteratorForDataIndexReader(
      final RocksDBClient client,
      final DataIndexReaderParams dataIndexReaderParams) {
    final RocksDBDataIndexTable dataIndexTable =
        RocksDBUtils.getDataIndexTable(
            client,
            dataIndexReaderParams.getInternalAdapterStore().getTypeName(
                dataIndexReaderParams.getAdapterId()),
            dataIndexReaderParams.getAdapterId());
    Iterator<GeoWaveRow> iterator;
    if (dataIndexReaderParams.getDataIds() != null) {
      iterator = dataIndexTable.dataIndexIterator(dataIndexReaderParams.getDataIds());
    } else {
      iterator =
          dataIndexTable.dataIndexIterator(
              dataIndexReaderParams.getStartInclusiveDataId(),
              dataIndexReaderParams.getEndInclusiveDataId(),
              dataIndexReaderParams.isReverse());
    }
    if (client.isVisibilityEnabled()) {
      Stream<GeoWaveRow> stream = Streams.stream(iterator);
      final Set<String> authorizations =
          Sets.newHashSet(dataIndexReaderParams.getAdditionalAuthorizations());
      stream = stream.filter(new ClientVisibilityFilter(authorizations));
      iterator = stream.iterator();
    }
    return iterator;
  }

  @SuppressWarnings("unchecked")
  private CloseableIterator<T> wrapResults(
      final Closeable closeable,
      final Iterator<GeoWaveRow> results,
      final RangeReaderParams<T> params,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final Set<String> authorizations,
      final boolean visibilityEnabled) {
    Stream<GeoWaveRow> stream = Streams.stream(results);
    if (visibilityEnabled) {
      stream = stream.filter(new ClientVisibilityFilter(authorizations));
    }
    final Iterator<GeoWaveRow> iterator = stream.iterator();
    return new CloseableIteratorWrapper<>(
        closeable,
        rowTransformer.apply(
            sortBySortKeyIfRequired(
                params,
                DataStoreUtils.isMergingIteratorRequired(params, visibilityEnabled)
                    ? new GeoWaveRowMergingIterator(iterator)
                    : iterator)));
  }

  private static Iterator<GeoWaveRow> sortBySortKeyIfRequired(
      final RangeReaderParams<?> params,
      final Iterator<GeoWaveRow> it) {
    if (RocksDBUtils.isSortByKeyRequired(params)) {
      return RocksDBUtils.sortBySortKey(it);
    }
    return it;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public T next() {
    return iterator.next();
  }

  @Override
  public void close() {
    iterator.close();
  }
}
