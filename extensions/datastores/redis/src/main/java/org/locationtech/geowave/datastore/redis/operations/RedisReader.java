/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.operations;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
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
import org.locationtech.geowave.datastore.redis.config.RedisOptions.Compression;
import org.locationtech.geowave.datastore.redis.config.RedisOptions.Serialization;
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisPersistedRow;
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisRow;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.locationtech.geowave.mapreduce.splits.GeoWaveRowRange;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import org.redisson.api.RedissonClient;
import org.redisson.client.protocol.ScoredEntry;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

public class RedisReader<T> implements RowReader<T> {
  private final CloseableIterator<T> iterator;

  public RedisReader(
      final RedissonClient client,
      final Compression compression,
      final ReaderParams<T> readerParams,
      final String namespace,
      final boolean visibilityEnabled,
      final boolean async) {
    this.iterator =
        createIteratorForReader(
            client,
            compression,
            readerParams,
            readerParams.getRowTransformer(),
            namespace,
            visibilityEnabled,
            false);
  }

  public RedisReader(
      final RedissonClient client,
      final Serialization serialization,
      final Compression compression,
      final DataIndexReaderParams dataIndexReaderParams,
      final String namespace,
      final boolean visibilityEnabled) {
    this.iterator =
        new Wrapper(
            createIteratorForDataIndexReader(
                client,
                serialization,
                compression,
                dataIndexReaderParams,
                namespace,
                visibilityEnabled));
  }

  public RedisReader(
      final RedissonClient client,
      final Compression compression,
      final RecordReaderParams recordReaderParams,
      final String namespace,
      final boolean visibilityEnabled) {
    this.iterator =
        createIteratorForRecordReader(
            client,
            compression,
            recordReaderParams,
            namespace,
            visibilityEnabled);
  }

  private CloseableIterator<T> createIteratorForReader(
      final RedissonClient client,
      final Compression compression,
      final ReaderParams<T> readerParams,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final String namespace,
      final boolean visibilityEnabled,
      final boolean async) {
    final Collection<SinglePartitionQueryRanges> ranges =
        readerParams.getQueryRanges().getPartitionQueryRanges();

    final Set<String> authorizations = Sets.newHashSet(readerParams.getAdditionalAuthorizations());
    if ((ranges != null) && !ranges.isEmpty()) {
      return createIterator(
          client,
          compression,
          readerParams,
          readerParams.getRowTransformer(),
          namespace,
          ranges,
          authorizations,
          visibilityEnabled,
          async);
    } else {
      final Iterator<GeoWaveRedisRow>[] iterators =
          new Iterator[readerParams.getAdapterIds().length];
      int i = 0;
      for (final short adapterId : readerParams.getAdapterIds()) {
        final Pair<Boolean, Boolean> groupByRowAndSortByTime =
            RedisUtils.isGroupByRowAndIsSortByTime(readerParams, adapterId);
        final String setNamePrefix =
            RedisUtils.getRowSetPrefix(
                namespace,
                readerParams.getInternalAdapterStore().getTypeName(adapterId),
                readerParams.getIndex().getName());
        final Stream<Pair<ByteArray, Iterator<ScoredEntry<GeoWaveRedisPersistedRow>>>> streamIt =
            RedisUtils.getPartitions(client, setNamePrefix).stream().map(p -> {
              final Iterator<ScoredEntry<GeoWaveRedisPersistedRow>> result =
                  RedisUtils.getRowSet(
                      client,
                      compression,
                      setNamePrefix,
                      p.getBytes(),
                      groupByRowAndSortByTime.getRight(),
                      visibilityEnabled).entryRange(
                          Double.NEGATIVE_INFINITY,
                          true,
                          Double.POSITIVE_INFINITY,
                          true);
              final Iterator<ScoredEntry<GeoWaveRedisPersistedRow>> it =
                  groupByRowAndSortByTime.getLeft()
                      ? RedisUtils.groupByRow(result, groupByRowAndSortByTime.getRight())
                      : result;
              return ImmutablePair.of(p, it);
            });
        iterators[i++] =
            Iterators.concat(
                streamIt.map(
                    p -> Iterators.transform(
                        p.getRight(),
                        pr -> new GeoWaveRedisRow(
                            pr.getValue(),
                            adapterId,
                            p.getLeft().getBytes(),
                            RedisUtils.getFullSortKey(
                                pr.getScore(),
                                pr.getValue().getSortKeyPrecisionBeyondScore())))).iterator());
      }
      return wrapResults(
          Iterators.concat(iterators),
          readerParams,
          rowTransformer,
          authorizations,
          visibilityEnabled);
    }
  }

  private CloseableIterator<T> createIterator(
      final RedissonClient client,
      final Compression compression,
      final RangeReaderParams<T> readerParams,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final String namespace,
      final Collection<SinglePartitionQueryRanges> ranges,
      final Set<String> authorizations,
      final boolean visibilityEnabled,
      final boolean async) {
    final Iterator<CloseableIterator> it =
        Arrays.stream(ArrayUtils.toObject(readerParams.getAdapterIds())).map(
            adapterId -> new BatchedRangeRead(
                client,
                compression,
                RedisUtils.getRowSetPrefix(
                    namespace,
                    readerParams.getInternalAdapterStore().getTypeName(adapterId),
                    readerParams.getIndex().getName()),
                adapterId,
                ranges,
                rowTransformer,
                new ClientVisibilityFilter(authorizations),
                DataStoreUtils.isMergingIteratorRequired(readerParams, visibilityEnabled),
                async,
                RedisUtils.isGroupByRowAndIsSortByTime(readerParams, adapterId),
                RedisUtils.isSortByKeyRequired(readerParams),
                visibilityEnabled).results()).iterator();
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

  private Iterator<GeoWaveRow> createIteratorForDataIndexReader(
      final RedissonClient client,
      final Serialization serialization,
      final Compression compression,
      final DataIndexReaderParams dataIndexReaderParams,
      final String namespace,
      final boolean visibilityEnabled) {
    Iterator<GeoWaveRow> retVal;
    if (dataIndexReaderParams.getDataIds() != null) {
      retVal =
          new DataIndexRead(
              client,
              serialization,
              compression,
              namespace,
              dataIndexReaderParams.getInternalAdapterStore().getTypeName(
                  dataIndexReaderParams.getAdapterId()),
              dataIndexReaderParams.getAdapterId(),
              dataIndexReaderParams.getDataIds(),
              visibilityEnabled).results();
    } else {
      retVal =
          new DataIndexRangeRead(
              client,
              serialization,
              compression,
              namespace,
              dataIndexReaderParams.getInternalAdapterStore().getTypeName(
                  dataIndexReaderParams.getAdapterId()),
              dataIndexReaderParams.getAdapterId(),
              dataIndexReaderParams.getStartInclusiveDataId(),
              dataIndexReaderParams.getEndInclusiveDataId(),
              visibilityEnabled).results();
    }
    if (visibilityEnabled) {
      Stream<GeoWaveRow> stream = Streams.stream(retVal);
      final Set<String> authorizations =
          Sets.newHashSet(dataIndexReaderParams.getAdditionalAuthorizations());
      stream = stream.filter(new ClientVisibilityFilter(authorizations));
      retVal = stream.iterator();
    }
    return retVal;
  }

  private CloseableIterator<T> createIteratorForRecordReader(
      final RedissonClient client,
      final Compression compression,
      final RecordReaderParams recordReaderParams,
      final String namespace,
      final boolean visibilityEnabled) {
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
        compression,
        (RangeReaderParams<T>) recordReaderParams,
        (GeoWaveRowIteratorTransformer<T>) GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER,
        namespace,
        Collections.singleton(partitionRange),
        authorizations,
        visibilityEnabled,
        // there should already be sufficient parallelism created by
        // input splits for record reader use cases
        false);
  }

  @SuppressWarnings("unchecked")
  private CloseableIterator<T> wrapResults(
      final Iterator<GeoWaveRedisRow> results,
      final RangeReaderParams<T> params,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final Set<String> authorizations,
      final boolean visibilityEnabled) {
    final Iterator<GeoWaveRow> iterator =
        (Iterator) Streams.stream(results).filter(
            new ClientVisibilityFilter(authorizations)).iterator();
    return new CloseableIterator.Wrapper<>(
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
    if (RedisUtils.isSortByKeyRequired(params)) {
      return RedisUtils.sortBySortKey(it);
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
