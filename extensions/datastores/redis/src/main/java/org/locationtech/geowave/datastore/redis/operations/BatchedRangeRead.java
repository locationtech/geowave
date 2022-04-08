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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.entities.GeoWaveRowMergingIterator;
import org.locationtech.geowave.core.store.util.RowConsumer;
import org.locationtech.geowave.datastore.redis.config.RedisOptions.Compression;
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisPersistedRow;
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisRow;
import org.locationtech.geowave.datastore.redis.util.RedisScoredSetWrapper;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.redisson.api.RFuture;
import org.redisson.api.RedissonClient;
import org.redisson.client.protocol.ScoredEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.google.common.primitives.UnsignedBytes;

public class BatchedRangeRead<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BatchedRangeRead.class);

  private static class ScoreOrderComparator implements Comparator<RangeReadInfo>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final ScoreOrderComparator SINGLETON = new ScoreOrderComparator();

    @Override
    public int compare(final RangeReadInfo o1, final RangeReadInfo o2) {
      int comp = Double.compare(o1.startScore, o2.startScore);
      if (comp != 0) {
        return comp;
      }
      comp = Double.compare(o1.endScore, o2.endScore);
      if (comp != 0) {
        return comp;
      }
      final byte[] otherComp = o2.partitionKey == null ? new byte[0] : o2.partitionKey;
      final byte[] thisComp = o1.partitionKey == null ? new byte[0] : o1.partitionKey;

      return UnsignedBytes.lexicographicalComparator().compare(thisComp, otherComp);
    }
  }

  private static final int MAX_CONCURRENT_READ = 100;
  private static final int MAX_BOUNDED_READS_ENQUEUED = 1000000;
  private static ByteArray EMPTY_PARTITION_KEY = new ByteArray();
  private final LoadingCache<ByteArray, RedisScoredSetWrapper<GeoWaveRedisPersistedRow>> setCache =
      Caffeine.newBuilder().build(partitionKey -> getSet(partitionKey.getBytes()));
  private final Collection<SinglePartitionQueryRanges> ranges;
  private final short adapterId;
  private final String setNamePrefix;
  private final RedissonClient client;
  private final GeoWaveRowIteratorTransformer<T> rowTransformer;
  private final Predicate<GeoWaveRow> filter;

  // only allow so many outstanding async reads or writes, use this semaphore
  // to control it
  private final Semaphore readSemaphore = new Semaphore(MAX_CONCURRENT_READ);
  private final boolean async;
  private final Pair<Boolean, Boolean> groupByRowAndSortByTimePair;
  private final boolean isSortFinalResultsBySortKey;
  private final Compression compression;
  private final boolean rowMerging;
  private final boolean visibilityEnabled;

  protected BatchedRangeRead(
      final RedissonClient client,
      final Compression compression,
      final String setNamePrefix,
      final short adapterId,
      final Collection<SinglePartitionQueryRanges> ranges,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final Predicate<GeoWaveRow> filter,
      final boolean rowMerging,
      final boolean async,
      final Pair<Boolean, Boolean> groupByRowAndSortByTimePair,
      final boolean isSortFinalResultsBySortKey,
      final boolean visibilityEnabled) {
    this.client = client;
    this.compression = compression;
    this.setNamePrefix = setNamePrefix;
    this.adapterId = adapterId;
    this.ranges = ranges;
    this.rowTransformer = rowTransformer;
    this.filter = filter;
    this.rowMerging = rowMerging;
    // we can't efficiently guarantee sort order with async queries
    this.async = async && !isSortFinalResultsBySortKey;
    this.groupByRowAndSortByTimePair = groupByRowAndSortByTimePair;
    this.isSortFinalResultsBySortKey = isSortFinalResultsBySortKey;
    this.visibilityEnabled = visibilityEnabled;
  }

  private RedisScoredSetWrapper<GeoWaveRedisPersistedRow> getSet(final byte[] partitionKey) {
    return RedisUtils.getRowSet(
        client,
        compression,
        setNamePrefix,
        partitionKey,
        groupByRowAndSortByTimePair.getRight(),
        visibilityEnabled);
  }

  public CloseableIterator<T> results() {
    final List<RangeReadInfo> reads = new ArrayList<>();
    for (final SinglePartitionQueryRanges r : ranges) {
      reads.addAll(
          r.getSortKeyRanges().stream().flatMap(
              range -> RedisUtils.getScoreRangesFromByteArrays(range).map(
                  scoreRange -> new RangeReadInfo(
                      r.getPartitionKey(),
                      scoreRange.getMinimum(),
                      scoreRange.getMaximum(),
                      range))).collect(Collectors.toList()));
    }
    if (async) {
      return executeQueryAsync(reads);
    } else {
      return executeQuery(reads);
    }
  }

  private CloseableIterator<T> executeQuery(final List<RangeReadInfo> reads) {
    if (isSortFinalResultsBySortKey) {
      // order the reads by sort keys
      reads.sort(ScoreOrderComparator.SINGLETON);
    }
    final Iterator<ScoredEntry<GeoWaveRedisPersistedRow>> result =
        Iterators.concat(reads.stream().map(r -> {
          ByteArray partitionKey;
          if ((r.partitionKey == null) || (r.partitionKey.length == 0)) {
            partitionKey = EMPTY_PARTITION_KEY;
          } else {
            partitionKey = new ByteArray(r.partitionKey);
          }
          return new PartitionIteratorWrapper(
              Streams.stream(
                  setCache.get(partitionKey).entryRange(
                      r.startScore,
                      true,
                      r.endScore,
                      // because we have a finite precision we need to make
                      // sure the end is inclusive and do more precise client-side filtering
                      ((r.endScore <= r.startScore) || (r.explicitEndCheck != null)))).filter(
                          e -> r.passesExplicitRowChecks(e)).iterator(),
              r.partitionKey);
        }).iterator());
    return new CloseableIterator.Wrapper<>(transformAndFilter(result));
  }

  private static class PartitionIteratorWrapper implements
      Iterator<ScoredEntry<GeoWaveRedisPersistedRow>> {
    private final byte[] partitionKey;
    private final Iterator<ScoredEntry<GeoWaveRedisPersistedRow>> iteratorDelegate;

    private PartitionIteratorWrapper(
        final Iterator<ScoredEntry<GeoWaveRedisPersistedRow>> iteratorDelegate,
        final byte[] partitionKey) {
      this.partitionKey = partitionKey;
      this.iteratorDelegate = iteratorDelegate;
    }

    @Override
    public boolean hasNext() {
      return iteratorDelegate.hasNext();
    }

    @Override
    public ScoredEntry<GeoWaveRedisPersistedRow> next() {
      final ScoredEntry<GeoWaveRedisPersistedRow> retVal = iteratorDelegate.next();
      if (retVal != null) {
        retVal.getValue().setPartitionKey(partitionKey);
      }
      return retVal;
    }
  }

  private CloseableIterator<T> executeQueryAsync(final List<RangeReadInfo> reads) {
    // first create a list of asynchronous query executions
    final List<RFuture<Collection<ScoredEntry<GeoWaveRedisPersistedRow>>>> futures =
        Lists.newArrayListWithExpectedSize(reads.size());
    final BlockingQueue<Object> results = new LinkedBlockingQueue<>(MAX_BOUNDED_READS_ENQUEUED);
    new Thread(new Runnable() {
      @Override
      public void run() {
        // set it to 1 to make sure all queries are submitted in
        // the loop
        final AtomicInteger queryCount = new AtomicInteger(1);
        for (final RangeReadInfo r : reads) {
          try {
            ByteArray partitionKey;
            if ((r.partitionKey == null) || (r.partitionKey.length == 0)) {
              partitionKey = EMPTY_PARTITION_KEY;
            } else {
              partitionKey = new ByteArray(r.partitionKey);
            }
            readSemaphore.acquire();
            final RFuture<Collection<ScoredEntry<GeoWaveRedisPersistedRow>>> f =
                setCache.get(partitionKey).entryRangeAsync(
                    r.startScore,
                    true,
                    r.endScore,
                    // because we have a finite precision we need to make
                    // sure the end is inclusive and do more precise client-side filtering
                    ((r.endScore <= r.startScore) || (r.explicitEndCheck != null)));
            queryCount.incrementAndGet();
            f.handle((result, throwable) -> {
              if (!f.isSuccess()) {
                if (!f.isCancelled()) {
                  LOGGER.warn("Async Redis query failed", throwable);
                }
                checkFinalize(readSemaphore, results, queryCount);
                return result;
              } else {
                try {
                  result.forEach(i -> i.getValue().setPartitionKey(r.partitionKey));

                  transformAndFilter(
                      result.stream().filter(
                          e -> r.passesExplicitRowChecks(e)).iterator()).forEachRemaining(row -> {
                            try {
                              results.put(row);
                            } catch (final InterruptedException e) {
                              LOGGER.warn("interrupted while waiting to enqueue a redis result", e);
                            }
                          });

                } finally {
                  checkFinalize(readSemaphore, results, queryCount);
                }
                return result;
              }
            });
            synchronized (futures) {
              futures.add(f);
            }
          } catch (final InterruptedException e) {
            LOGGER.warn("Exception while executing query", e);
            readSemaphore.release();
          }
        }
        // then decrement
        if (queryCount.decrementAndGet() <= 0) {
          // and if there are no queries, there may not have
          // been any
          // statements submitted
          try {
            results.put(RowConsumer.POISON);
          } catch (final InterruptedException e) {
            LOGGER.error(
                "Interrupted while finishing blocking queue, this may result in deadlock!");
          }
        }
      }
    }, "Redis Query Executor").start();
    return new CloseableIteratorWrapper<>(new Closeable() {
      @Override
      public void close() throws IOException {
        List<RFuture<Collection<ScoredEntry<GeoWaveRedisPersistedRow>>>> newFutures;
        synchronized (futures) {
          newFutures = new ArrayList<>(futures);
        }
        for (final RFuture<Collection<ScoredEntry<GeoWaveRedisPersistedRow>>> f : newFutures) {
          f.cancel(true);
        }
      }
    }, new RowConsumer<>(results));
  }


  private Iterator<T> transformAndFilter(
      final Iterator<ScoredEntry<GeoWaveRedisPersistedRow>> result) {
    final Iterator<GeoWaveRow> iterator =
        (Iterator) Streams.stream(
            groupByRowAndSortByTimePair.getLeft()
                ? RedisUtils.groupByRow(result, groupByRowAndSortByTimePair.getRight())
                : result).map(
                    entry -> new GeoWaveRedisRow(
                        entry.getValue(),
                        adapterId,
                        entry.getValue().getPartitionKey(),
                        RedisUtils.getFullSortKey(
                            entry.getScore(),
                            entry.getValue().getSortKeyPrecisionBeyondScore()))).filter(
                                filter).iterator();
    return rowTransformer.apply(
        sortByKeyIfRequired(
            isSortFinalResultsBySortKey,
            rowMerging ? new GeoWaveRowMergingIterator(iterator) : iterator));
  }

  private static Iterator<GeoWaveRow> sortByKeyIfRequired(
      final boolean isRequired,
      final Iterator<GeoWaveRow> it) {
    if (isRequired) {
      return RedisUtils.sortBySortKey(it);
    }
    return it;
  }

  private static void checkFinalize(
      final Semaphore semaphore,
      final BlockingQueue<Object> resultQueue,
      final AtomicInteger queryCount) {
    semaphore.release();
    if (queryCount.decrementAndGet() <= 0) {
      try {
        resultQueue.put(RowConsumer.POISON);
      } catch (final InterruptedException e) {
        LOGGER.error("Interrupted while finishing blocking queue, this may result in deadlock!");
      }
    }
  }
}
