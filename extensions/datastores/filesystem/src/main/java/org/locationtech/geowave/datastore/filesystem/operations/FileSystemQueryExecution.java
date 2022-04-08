/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.operations;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
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
import org.locationtech.geowave.datastore.filesystem.util.FileSystemClient;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemIndexTable;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import com.google.common.primitives.UnsignedBytes;

public class FileSystemQueryExecution<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemQueryExecution.class);

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
  private final LoadingCache<ByteArray, FileSystemIndexTable> setCache =
      Caffeine.newBuilder().build(partitionKey -> getTable(partitionKey.getBytes()));
  private final Collection<SinglePartitionQueryRanges> ranges;
  private final short adapterId;
  private final String typeName;
  private final String indexName;
  private final FileSystemClient client;
  private final String format;
  private final GeoWaveRowIteratorTransformer<T> rowTransformer;
  private final Predicate<GeoWaveRow> filter;
  private final boolean rowMerging;

  private final Pair<Boolean, Boolean> groupByRowAndSortByTimePair;
  private final boolean isSortFinalResultsBySortKey;

  protected FileSystemQueryExecution(
      final FileSystemClient client,
      final short adapterId,
      final String typeName,
      final String indexName,
      final String format,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final Collection<SinglePartitionQueryRanges> ranges,
      final Predicate<GeoWaveRow> filter,
      final boolean rowMerging,
      final boolean async,
      final Pair<Boolean, Boolean> groupByRowAndSortByTimePair,
      final boolean isSortFinalResultsBySortKey) {
    this.client = client;
    this.adapterId = adapterId;
    this.typeName = typeName;
    this.indexName = indexName;
    this.format = format;
    this.rowTransformer = rowTransformer;
    this.ranges = ranges;
    this.filter = filter;
    this.rowMerging = rowMerging;
    this.groupByRowAndSortByTimePair = groupByRowAndSortByTimePair;
    this.isSortFinalResultsBySortKey = isSortFinalResultsBySortKey;
  }

  private FileSystemIndexTable getTable(final byte[] partitionKey) {
    return FileSystemUtils.getIndexTable(
        client,
        adapterId,
        typeName,
        indexName,
        partitionKey,
        groupByRowAndSortByTimePair.getRight());
  }

  public CloseableIterator<T> results() {
    return executeQuery();
  }

  public CloseableIterator<T> executeQuery() {
    final List<CloseableIterator<GeoWaveRow>> iterators = ranges.stream().map(r -> {
      ByteArray partitionKey;
      if ((r.getPartitionKey() == null) || (r.getPartitionKey().length == 0)) {
        partitionKey = EMPTY_PARTITION_KEY;
      } else {
        partitionKey = new ByteArray(r.getPartitionKey());
      }
      return setCache.get(partitionKey).iterator(r.getSortKeyRanges());
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
      return FileSystemUtils.sortBySortKey(it);
    }
    return it;
  }
}
