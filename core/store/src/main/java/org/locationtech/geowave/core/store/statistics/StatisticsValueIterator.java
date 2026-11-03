/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayRange.MergeOperation;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import com.google.common.collect.Iterators;

/**
 * Iterates over the values of a set of statistics.
 */
public class StatisticsValueIterator implements CloseableIterator<StatisticValue<?>> {

  private final DataStatisticsStore statisticsStore;
  private final Iterator<? extends Statistic<? extends StatisticValue<?>>> statistics;
  private final ByteArrayConstraints binConstraints;
  private final String[] authorizations;

  private CloseableIterator<? extends StatisticValue<?>> current = null;

  private StatisticValue<?> next = null;

  public StatisticsValueIterator(
      final DataStatisticsStore statisticsStore,
      final Iterator<? extends Statistic<? extends StatisticValue<?>>> statistics,
      final ByteArrayConstraints binConstraints,
      final String... authorizations) {
    this.statisticsStore = statisticsStore;
    this.statistics = statistics;
    this.binConstraints = binConstraints;
    this.authorizations = authorizations;
  }

  @SuppressWarnings("unchecked")
  private void computeNext() {
    if (next == null) {
      while (((current == null) || !current.hasNext()) && statistics.hasNext()) {
        if (current != null) {
          current.close();
          current = null;
        }
        final Statistic<StatisticValue<Object>> nextStat =
            (Statistic<StatisticValue<Object>>) statistics.next();
        if ((nextStat.getBinningStrategy() != null)
            && (binConstraints != null)
            && !binConstraints.isAllBins()) {
          if ((binConstraints.getBins().length > 0) && (binConstraints.getBinRanges().length > 0)) {
            current = binsAndRanges(nextStat);
          } else if (binConstraints.getBins().length > 0) {
            if (binConstraints.isPrefix()) {
              final List<CloseableIterator<StatisticValue<Object>>> iters =
                  Arrays.stream(binConstraints.getBins()).map(
                      bin -> statisticsStore.getStatisticValues(
                          nextStat,
                          bin,
                          authorizations)).collect(Collectors.toList());
              current =
                  new CloseableIteratorWrapper<>(
                      () -> iters.forEach(CloseableIterator::close),
                      Iterators.concat(iters.iterator()));
            } else {
              current =
                  new CloseableIterator.Wrapper<>(
                      Arrays.stream(binConstraints.getBins()).map(
                          bin -> statisticsStore.getStatisticValue(
                              nextStat,
                              bin,
                              authorizations)).filter(Objects::nonNull).iterator());
            }
          } else if (binConstraints.getBinRanges().length > 0) {
            current =
                statisticsStore.getStatisticValues(
                    nextStat,
                    binConstraints.getBinRanges(),
                    authorizations);

          } else {
            continue;
          }
        } else {
          current = statisticsStore.getStatisticValues(nextStat, authorizations);
        }
        if ((current != null) && !current.hasNext()) {
          current =
              new CloseableIterator.Wrapper<>(Iterators.singletonIterator(nextStat.createEmpty()));
        }
      }
      if ((current != null) && current.hasNext()) {
        next = current.next();
      }
    }
  }

  // Reads the union of the bins and the ranges, each value once: prefix bins become ranges, and an
  // exact bin is left to the range read when a range covers it.
  private CloseableIterator<StatisticValue<Object>> binsAndRanges(
      final Statistic<StatisticValue<Object>> statistic) {
    final List<ByteArrayRange> ranges = new ArrayList<>();
    for (final ByteArrayRange range : binConstraints.getBinRanges()) {
      // merging sorts a null start last, so an open start becomes the empty bin
      ranges.add(
          new ByteArrayRange(
              range.getStart() == null ? new byte[0] : range.getStart(),
              range.getEnd()));
    }
    if (binConstraints.isPrefix()) {
      for (final ByteArray bin : binConstraints.getBins()) {
        ranges.add(new ByteArrayRange(bin.getBytes(), bin.getBytes()));
      }
    }
    // an inverted range matches nothing, but would keep the ranges around it from merging
    ranges.removeIf(r -> ByteArrayUtils.compare(r.getStart(), r.getEndAsNextPrefix()) >= 0);
    final ByteArrayRange[] merged =
        ByteArrayRange.mergeIntersections(ranges, MergeOperation.UNION).toArray(
            new ByteArrayRange[0]);
    final List<CloseableIterator<StatisticValue<Object>>> iters = new ArrayList<>();
    if (!binConstraints.isPrefix()) {
      iters.add(
          new CloseableIterator.Wrapper<>(
              Arrays.stream(binConstraints.getBins()).distinct().filter(
                  bin -> Arrays.stream(merged).noneMatch(
                      range -> covers(range, bin.getBytes()))).map(
                          bin -> statisticsStore.getStatisticValue(
                              statistic,
                              bin,
                              authorizations)).filter(Objects::nonNull).iterator()));
    }
    // to a metadata query, no ranges means every value
    if (merged.length > 0) {
      iters.add(statisticsStore.getStatisticValues(statistic, merged, authorizations));
    }
    return new CloseableIteratorWrapper<>(
        () -> iters.forEach(CloseableIterator::close),
        Iterators.concat(iters.iterator()));
  }

  private static boolean covers(final ByteArrayRange range, final byte[] bin) {
    return (ByteArrayUtils.compare(bin, range.getStart()) >= 0)
        && ((range.getEnd() == null)
            || (ByteArrayUtils.compare(bin, range.getEndAsNextPrefix()) < 0));
  }

  @Override
  public boolean hasNext() {
    if (next == null) {
      computeNext();
    }
    return next != null;
  }

  @Override
  public StatisticValue<?> next() {
    if (next == null) {
      computeNext();
    }
    final StatisticValue<?> retVal = next;
    next = null;
    return retVal;
  }

  @Override
  public void close() {
    if (current != null) {
      current.close();
      current = null;
    }
  }

}
