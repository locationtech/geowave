/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.index;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.locationtech.geowave.core.index.IndexUtils;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IndexStatistic;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.binning.CompositeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.binning.DataTypeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.binning.PartitionBinningStrategy;
import org.locationtech.geowave.core.store.statistics.index.RowRangeHistogramStatistic;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChooseBestMatchIndexQueryStrategy implements IndexQueryStrategySPI {
  public static final String NAME = "Best Match";
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ChooseBestMatchIndexQueryStrategy.class);

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public CloseableIterator<Index> getIndices(
      final DataStatisticsStore statisticsStore,
      final AdapterIndexMappingStore mappingStore,
      final QueryConstraints query,
      final Index[] indices,
      final InternalDataAdapter<?> adapter,
      final Map<QueryHint, Object> hints) {
    return new CloseableIterator<Index>() {
      Index nextIdx = null;
      boolean done = false;
      int i = 0;

      @Override
      public boolean hasNext() {
        long min = Long.MAX_VALUE;
        Index bestIdx = null;

        while (!done && (i < indices.length)) {
          nextIdx = indices[i++];
          if (nextIdx.getIndexStrategy().getOrderedDimensionDefinitions().length == 0) {
            continue;
          }
          final List<MultiDimensionalNumericData> constraints = query.getIndexConstraints(nextIdx);

          RowRangeHistogramStatistic rowRangeHistogramStatistic = null;

          try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> stats =
              statisticsStore.getIndexStatistics(
                  nextIdx,
                  RowRangeHistogramStatistic.STATS_TYPE,
                  Statistic.INTERNAL_TAG)) {
            if (stats.hasNext()) {
              final Statistic<?> statistic = stats.next();
              if ((statistic instanceof RowRangeHistogramStatistic)
                  && (statistic.getBinningStrategy() instanceof CompositeBinningStrategy)
                  && ((CompositeBinningStrategy) statistic.getBinningStrategy()).isOfType(
                      DataTypeBinningStrategy.class,
                      PartitionBinningStrategy.class)) {
                rowRangeHistogramStatistic = (RowRangeHistogramStatistic) statistic;
              }
            }
          }

          if (rowRangeHistogramStatistic == null) {
            LOGGER.warn(
                "Best Match Heuristic requires statistic RowRangeHistogramStatistics for each index to properly choose an index.");
          }

          if (IndexUtils.isFullTableScan(constraints)) {
            // keep this is as a default in case all indices
            // result in a full table scan
            if (bestIdx == null) {
              bestIdx = nextIdx;
            }
          } else {
            final int maxRangeDecomposition;
            if (hints.containsKey(QueryHint.MAX_RANGE_DECOMPOSITION)) {
              maxRangeDecomposition = (Integer) hints.get(QueryHint.MAX_RANGE_DECOMPOSITION);
            } else {
              LOGGER.warn(
                  "No max range decomposition hint was provided, this should be provided from the data store options");
              maxRangeDecomposition = 2000;
            }
            final QueryRanges ranges =
                DataStoreUtils.constraintsToQueryRanges(
                    constraints,
                    nextIdx,
                    null,
                    maxRangeDecomposition);
            final long temp =
                DataStoreUtils.cardinality(
                    statisticsStore,
                    rowRangeHistogramStatistic,
                    adapter,
                    nextIdx,
                    ranges);
            if (temp < min) {
              bestIdx = nextIdx;
              min = temp;
            }
          }
        }
        nextIdx = bestIdx;
        done = true;
        return nextIdx != null;
      }

      @Override
      public Index next() throws NoSuchElementException {
        if (nextIdx == null) {
          throw new NoSuchElementException();
        }
        final Index returnVal = nextIdx;
        nextIdx = null;
        return returnVal;
      }

      @Override
      public void remove() {}

      @Override
      public void close() {}
    };
  }

  @Override
  public boolean requiresStats() {
    return true;
  }
}
