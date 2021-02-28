/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics;

import java.util.Arrays;
import java.util.Iterator;
import org.locationtech.geowave.core.store.CloseableIterator;
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
          if (binConstraints.getBins().length > 0) {
            current =
                new CloseableIterator.Wrapper<>(
                    Arrays.stream(binConstraints.getBins()).map(
                        bin -> statisticsStore.getStatisticValue(
                            nextStat,
                            bin,
                            binConstraints.isPrefix(),
                            authorizations)).iterator());
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
