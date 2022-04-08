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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.entities.GeoWaveRowMergingIterator;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBClient;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBIndexTable;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import com.google.common.primitives.UnsignedBytes;

public class RocksDBQueryExecution<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBQueryExecution.class);

  private static class RangeReadInfo {
    byte[] partitionKey;
    ByteArrayRange sortKeyRange;

    public RangeReadInfo(final byte[] partitionKey, final ByteArrayRange sortKeyRange) {
      this.partitionKey = partitionKey;
      this.sortKeyRange = sortKeyRange;
    }
  }

  private static class ScoreOrderComparator implements Comparator<RangeReadInfo>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final ScoreOrderComparator SINGLETON = new ScoreOrderComparator();

    @Override
    public int compare(final RangeReadInfo o1, final RangeReadInfo o2) {
      int comp =
          UnsignedBytes.lexicographicalComparator().compare(
              o1.sortKeyRange.getStart(),
              o2.sortKeyRange.getStart());
      if (comp != 0) {
        return comp;
      }
      comp =
          UnsignedBytes.lexicographicalComparator().compare(
              o1.sortKeyRange.getEnd(),
              o2.sortKeyRange.getEnd());
      if (comp != 0) {
        return comp;
      }
      final byte[] otherComp = o2.partitionKey == null ? new byte[0] : o2.partitionKey;
      final byte[] thisComp = o1.partitionKey == null ? new byte[0] : o1.partitionKey;

      return UnsignedBytes.lexicographicalComparator().compare(thisComp, otherComp);
    }
  }

  private static ByteArray EMPTY_PARTITION_KEY = new ByteArray();
  private final LoadingCache<ByteArray, RocksDBIndexTable> setCache =
      Caffeine.newBuilder().build(partitionKey -> getTable(partitionKey.getBytes()));
  private final Collection<SinglePartitionQueryRanges> ranges;
  private final short adapterId;
  private final String indexNamePrefix;
  private final RocksDBClient client;
  private final GeoWaveRowIteratorTransformer<T> rowTransformer;
  private final Predicate<GeoWaveRow> filter;
  private final boolean rowMerging;

  private final Pair<Boolean, Boolean> groupByRowAndSortByTimePair;
  private final boolean isSortFinalResultsBySortKey;

  protected RocksDBQueryExecution(
      final RocksDBClient client,
      final String indexNamePrefix,
      final short adapterId,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final Collection<SinglePartitionQueryRanges> ranges,
      final Predicate<GeoWaveRow> filter,
      final boolean rowMerging,
      final boolean async,
      final Pair<Boolean, Boolean> groupByRowAndSortByTimePair,
      final boolean isSortFinalResultsBySortKey) {
    this.client = client;
    this.indexNamePrefix = indexNamePrefix;
    this.adapterId = adapterId;
    this.rowTransformer = rowTransformer;
    this.ranges = ranges;
    this.filter = filter;
    this.rowMerging = rowMerging;
    this.groupByRowAndSortByTimePair = groupByRowAndSortByTimePair;
    this.isSortFinalResultsBySortKey = isSortFinalResultsBySortKey;
  }

  private RocksDBIndexTable getTable(final byte[] partitionKey) {
    return RocksDBUtils.getIndexTableFromPrefix(
        client,
        indexNamePrefix,
        adapterId,
        partitionKey,
        groupByRowAndSortByTimePair.getRight());
  }

  public CloseableIterator<T> results() {
    final List<RangeReadInfo> reads = new ArrayList<>();
    for (final SinglePartitionQueryRanges r : ranges) {
      for (final ByteArrayRange range : r.getSortKeyRanges()) {
        reads.add(new RangeReadInfo(r.getPartitionKey(), range));
      }
    }
    return executeQuery(reads);
  }

  public CloseableIterator<T> executeQuery(final List<RangeReadInfo> reads) {
    if (isSortFinalResultsBySortKey) {
      // order the reads by sort keys
      reads.sort(ScoreOrderComparator.SINGLETON);
    }
    final List<CloseableIterator<GeoWaveRow>> iterators = reads.stream().map(r -> {
      ByteArray partitionKey;
      if ((r.partitionKey == null) || (r.partitionKey.length == 0)) {
        partitionKey = EMPTY_PARTITION_KEY;
      } else {
        partitionKey = new ByteArray(r.partitionKey);
      }
      return setCache.get(partitionKey).iterator(r.sortKeyRange);
    }).collect(Collectors.toList());
    return transformAndFilter(new CloseableIteratorWrapper<>(new Closeable() {
      @Override
      public void close() throws IOException {
        iterators.forEach(i -> i.close());
      }
    }, Iterators.concat(iterators.iterator())));
  }

  private CloseableIterator<T> transformAndFilter(final CloseableIterator<GeoWaveRow> result) {
    final Iterator<GeoWaveRow> iterator = Streams.stream(result).filter(filter).iterator();
    return new CloseableIteratorWrapper<>(
        result,
        rowTransformer.apply(
            sortByKeyIfRequired(
                isSortFinalResultsBySortKey,
                rowMerging ? new GeoWaveRowMergingIterator(iterator) : iterator)));
  }

  private static Iterator<GeoWaveRow> sortByKeyIfRequired(
      final boolean isRequired,
      final Iterator<GeoWaveRow> it) {
    if (isRequired) {
      return RocksDBUtils.sortBySortKey(it);
    }
    return it;
  }
}
