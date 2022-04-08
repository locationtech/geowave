/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics;

import java.util.Collection;
import java.util.List;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataTypeStatistic;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IndexStatistic;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.statistics.adapter.DataTypeStatisticType;
import org.locationtech.geowave.core.store.statistics.binning.CompositeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.binning.DataTypeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.binning.PartitionBinningStrategy;
import org.locationtech.geowave.core.store.statistics.field.FieldStatisticType;
import org.locationtech.geowave.core.store.statistics.index.DifferingVisibilityCountStatistic;
import org.locationtech.geowave.core.store.statistics.index.DifferingVisibilityCountStatistic.DifferingVisibilityCountValue;
import org.locationtech.geowave.core.store.statistics.index.DuplicateEntryCountStatistic;
import org.locationtech.geowave.core.store.statistics.index.DuplicateEntryCountStatistic.DuplicateEntryCountValue;
import org.locationtech.geowave.core.store.statistics.index.FieldVisibilityCountStatistic;
import org.locationtech.geowave.core.store.statistics.index.FieldVisibilityCountStatistic.FieldVisibilityCountValue;
import org.locationtech.geowave.core.store.statistics.index.IndexMetaDataSetStatistic;
import org.locationtech.geowave.core.store.statistics.index.IndexMetaDataSetStatistic.IndexMetaDataSetValue;
import org.locationtech.geowave.core.store.statistics.index.IndexStatisticType;
import org.locationtech.geowave.core.store.statistics.index.PartitionsStatistic;
import org.locationtech.geowave.core.store.statistics.index.PartitionsStatistic.PartitionsValue;
import org.locationtech.geowave.core.store.statistics.index.RowRangeHistogramStatistic;
import org.locationtech.geowave.core.store.statistics.index.RowRangeHistogramStatistic.RowRangeHistogramValue;

/**
 * This class contains static methods to make querying internal statistics as efficient as possible.
 */
public class InternalStatisticsHelper {

  /**
   * Get the value of an internal data type statistic that does not use a binning strategy.
   *
   * @param statisticsStore the statistics store
   * @param statisticType the statistic type
   * @param typeName the data type name
   * @param authorizations authorizations for the query
   * @return the value, or {@code null} if it didn't exist
   */
  public static <V extends StatisticValue<R>, R> V getDataTypeStatistic(
      final DataStatisticsStore statisticsStore,
      final DataTypeStatisticType<V> statisticType,
      final String typeName,
      final String... authorizations) {
    final Statistic<V> statistic =
        statisticsStore.getStatisticById(
            DataTypeStatistic.generateStatisticId(typeName, statisticType, Statistic.INTERNAL_TAG));
    if (statistic != null) {
      return statisticsStore.getStatisticValue(statistic, authorizations);
    }
    return null;
  }

  /**
   * Get the value of an internal field statistic that does not use a binning strategy.
   *
   * @param statisticsStore the statistics store
   * @param statisticType the statistic type
   * @param typeName the data type name
   * @param fieldName the field name
   * @param authorizations authorizations for the query
   * @return the value, or {@code null} if it didn't exist
   */
  public static <V extends StatisticValue<R>, R> V getFieldStatistic(
      final DataStatisticsStore statisticsStore,
      final FieldStatisticType<V> statisticType,
      final String typeName,
      final String fieldName,
      final String... authorizations) {
    final Statistic<V> statistic =
        statisticsStore.getStatisticById(
            FieldStatistic.generateStatisticId(
                typeName,
                statisticType,
                fieldName,
                Statistic.INTERNAL_TAG));
    if (statistic != null) {
      return statisticsStore.getStatisticValue(statistic, authorizations);
    }
    return null;
  }

  public static <V extends StatisticValue<R>, R> V getIndexStatistic(
      final DataStatisticsStore statisticsStore,
      final IndexStatisticType<V> statisticType,
      final String indexName,
      final String typeName,
      final byte[] partitionKey,
      final String... authorizations) {
    final StatisticId<V> statisticId =
        IndexStatistic.generateStatisticId(indexName, statisticType, Statistic.INTERNAL_TAG);
    final Statistic<V> stat = statisticsStore.getStatisticById(statisticId);
    if (stat != null) {
      return statisticsStore.getStatisticValue(
          stat,
          partitionKey != null
              ? CompositeBinningStrategy.getBin(
                  DataTypeBinningStrategy.getBin(typeName),
                  PartitionBinningStrategy.getBin(partitionKey))
              : DataTypeBinningStrategy.getBin(typeName),
          authorizations);
    }
    return null;
  }

  /**
   * Get the duplicate counts for an index.
   *
   * @param index the index
   * @param adapterIdsToQuery the adapters to query
   * @param adapterStore the adapter store
   * @param statisticsStore the statistics store
   * @param authorizations authorizations for the query
   * @return the duplicate counts, or {@code null} if it didn't exist
   */
  public static DuplicateEntryCountValue getDuplicateCounts(
      final Index index,
      final Collection<Short> adapterIdsToQuery,
      final PersistentAdapterStore adapterStore,
      final DataStatisticsStore statisticsStore,
      final String... authorizations) {
    return getInternalIndexStatistic(
        DuplicateEntryCountStatistic.STATS_TYPE,
        index,
        adapterIdsToQuery,
        adapterStore,
        statisticsStore,
        authorizations);
  }


  /**
   * Get the index metadtat for an index.
   *
   * @param index the index
   * @param adapterIdsToQuery the adapters to query
   * @param adapterStore the adapter store
   * @param statisticsStore the statistics store
   * @param authorizations authorizations for the query
   * @return the index metadata, or {@code null} if it didn't exist
   */
  public static IndexMetaDataSetValue getIndexMetadata(
      final Index index,
      final Collection<Short> adapterIdsToQuery,
      final PersistentAdapterStore adapterStore,
      final DataStatisticsStore statisticsStore,
      final String... authorizations) {
    return getInternalIndexStatistic(
        IndexMetaDataSetStatistic.STATS_TYPE,
        index,
        adapterIdsToQuery,
        adapterStore,
        statisticsStore,
        authorizations);
  }


  /**
   * Get the partitions for an index.
   *
   * @param index the index
   * @param adapterIdsToQuery the adapters to query
   * @param adapterStore the adapter store
   * @param statisticsStore the statistics store
   * @param authorizations authorizations for the query
   * @return the partitions, or {@code null} if it didn't exist
   */
  public static PartitionsValue getPartitions(
      final Index index,
      final Collection<Short> adapterIdsToQuery,
      final PersistentAdapterStore adapterStore,
      final DataStatisticsStore statisticsStore,
      final String... authorizations) {
    return getInternalIndexStatistic(
        PartitionsStatistic.STATS_TYPE,
        index,
        adapterIdsToQuery,
        adapterStore,
        statisticsStore,
        authorizations);
  }


  /**
   * Get the differing visibility counts for an index.
   *
   * @param index the index
   * @param adapterIdsToQuery the adapters to query
   * @param adapterStore the adapter store
   * @param statisticsStore the statistics store
   * @param authorizations authorizations for the query
   * @return the differing visibility counts, or {@code null} if it didn't exist
   */
  public static DifferingVisibilityCountValue getDifferingVisibilityCounts(
      final Index index,
      final Collection<Short> adapterIdsToQuery,
      final PersistentAdapterStore adapterStore,
      final DataStatisticsStore statisticsStore,
      final String... authorizations) {
    return getInternalIndexStatistic(
        DifferingVisibilityCountStatistic.STATS_TYPE,
        index,
        adapterIdsToQuery,
        adapterStore,
        statisticsStore,
        authorizations);
  }


  /**
   * Get the field visibility counts for an index.
   *
   * @param index the index
   * @param adapterIdsToQuery the adapters to query
   * @param adapterStore the adapter store
   * @param statisticsStore the statistics store
   * @param authorizations authorizations for the query
   * @return the field visibility counts, or {@code null} if it didn't exist
   */
  public static FieldVisibilityCountValue getVisibilityCounts(
      final Index index,
      final Collection<Short> adapterIdsToQuery,
      final PersistentAdapterStore adapterStore,
      final DataStatisticsStore statisticsStore,
      final String... authorizations) {
    return getInternalIndexStatistic(
        FieldVisibilityCountStatistic.STATS_TYPE,
        index,
        adapterIdsToQuery,
        adapterStore,
        statisticsStore,
        authorizations);
  }


  /**
   * Get the row range histogram of an index partition.
   *
   * @param index the index
   * @param adapterIds the adapters to query
   * @param adapterStore the adapter store
   * @param statisticsStore the statistics store
   * @param partitionKey the partition key
   * @param authorizations authorizations for the query
   * @return the row range histogram, or {@code null} if it didn't exist
   */
  public static RowRangeHistogramValue getRangeStats(
      final Index index,
      final List<Short> adapterIds,
      final PersistentAdapterStore adapterStore,
      final DataStatisticsStore statisticsStore,
      final ByteArray partitionKey,
      final String... authorizations) {
    final RowRangeHistogramStatistic stat =
        (RowRangeHistogramStatistic) statisticsStore.getStatisticById(
            IndexStatistic.generateStatisticId(
                index.getName(),
                RowRangeHistogramStatistic.STATS_TYPE,
                Statistic.INTERNAL_TAG));
    if ((stat != null)
        && (stat.getBinningStrategy() instanceof CompositeBinningStrategy)
        && ((CompositeBinningStrategy) stat.getBinningStrategy()).isOfType(
            DataTypeBinningStrategy.class,
            PartitionBinningStrategy.class)) {
      RowRangeHistogramValue combinedValue = null;
      for (final Short adapterId : adapterIds) {
        final RowRangeHistogramValue value =
            statisticsStore.getStatisticValue(
                stat,
                CompositeBinningStrategy.getBin(
                    DataTypeBinningStrategy.getBin(adapterStore.getAdapter(adapterId)),
                    PartitionBinningStrategy.getBin(partitionKey.getBytes())),
                authorizations);
        if (value != null) {
          if (combinedValue == null) {
            combinedValue = value;
          } else {
            combinedValue.merge(value);
          }
        }
      }
      return combinedValue;
    }
    return null;
  }


  /**
   * Get the row range histogram of a specific partition in an index.
   *
   * @param statisticsStore the statistics store
   * @param indexName the index name
   * @param typeName the type name
   * @param partitionKey the partition key
   * @param authorizations authorizations for the query
   * @return the row range histogram, or {@code null} if it didn't exist
   */
  public static RowRangeHistogramValue getRangeStats(
      final DataStatisticsStore statisticsStore,
      final String indexName,
      final String typeName,
      final ByteArray partitionKey,
      final String... authorizations) {
    final Statistic<RowRangeHistogramValue> statistic =
        statisticsStore.getStatisticById(
            IndexStatistic.generateStatisticId(
                indexName,
                RowRangeHistogramStatistic.STATS_TYPE,
                Statistic.INTERNAL_TAG));
    if ((statistic != null)
        && (statistic.getBinningStrategy() instanceof CompositeBinningStrategy)
        && ((CompositeBinningStrategy) statistic.getBinningStrategy()).isOfType(
            DataTypeBinningStrategy.class,
            PartitionBinningStrategy.class)) {
      return statisticsStore.getStatisticValue(
          statistic,
          CompositeBinningStrategy.getBin(
              DataTypeBinningStrategy.getBin(typeName),
              PartitionBinningStrategy.getBin(partitionKey.getBytes())),
          authorizations);
    }
    return null;
  }

  private static <V extends StatisticValue<R>, R> V getInternalIndexStatistic(
      final IndexStatisticType<V> statisticType,
      final Index index,
      final Collection<Short> adapterIdsToQuery,
      final PersistentAdapterStore adapterStore,
      final DataStatisticsStore statisticsStore,
      final String... authorizations) {
    final StatisticId<V> statisticId =
        IndexStatistic.generateStatisticId(index.getName(), statisticType, Statistic.INTERNAL_TAG);
    final Statistic<V> stat = statisticsStore.getStatisticById(statisticId);
    if (stat != null) {
      V combinedValue = null;
      for (final short adapterId : adapterIdsToQuery) {
        final DataTypeAdapter<?> adapter = adapterStore.getAdapter(adapterId);

        final V value =
            statisticsStore.getStatisticValue(
                stat,
                DataTypeBinningStrategy.getBin(adapter),
                authorizations);
        if (combinedValue == null) {
          combinedValue = value;
        } else {
          combinedValue.merge(value);
        }
      }
      return combinedValue;
    }
    return null;
  }

}
