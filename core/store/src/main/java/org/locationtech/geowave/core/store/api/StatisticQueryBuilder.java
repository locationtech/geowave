/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.Range;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.IndexMetaData;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.FixedBinNumericHistogram;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.NumericHistogram;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.core.store.statistics.adapter.DataTypeStatisticType;
import org.locationtech.geowave.core.store.statistics.field.BloomFilterStatistic;
import org.locationtech.geowave.core.store.statistics.field.BloomFilterStatistic.BloomFilterValue;
import org.locationtech.geowave.core.store.statistics.field.CountMinSketchStatistic;
import org.locationtech.geowave.core.store.statistics.field.CountMinSketchStatistic.CountMinSketchValue;
import org.locationtech.geowave.core.store.statistics.field.FieldStatisticType;
import org.locationtech.geowave.core.store.statistics.field.FixedBinNumericHistogramStatistic;
import org.locationtech.geowave.core.store.statistics.field.FixedBinNumericHistogramStatistic.FixedBinNumericHistogramValue;
import org.locationtech.geowave.core.store.statistics.field.HyperLogLogStatistic;
import org.locationtech.geowave.core.store.statistics.field.HyperLogLogStatistic.HyperLogLogPlusValue;
import org.locationtech.geowave.core.store.statistics.field.NumericHistogramStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericHistogramStatistic.NumericHistogramValue;
import org.locationtech.geowave.core.store.statistics.field.NumericMeanStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericMeanStatistic.NumericMeanValue;
import org.locationtech.geowave.core.store.statistics.field.NumericRangeStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericRangeStatistic.NumericRangeValue;
import org.locationtech.geowave.core.store.statistics.field.NumericStatsStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericStatsStatistic.NumericStatsValue;
import org.locationtech.geowave.core.store.statistics.field.Stats;
import org.locationtech.geowave.core.store.statistics.index.DifferingVisibilityCountStatistic;
import org.locationtech.geowave.core.store.statistics.index.DifferingVisibilityCountStatistic.DifferingVisibilityCountValue;
import org.locationtech.geowave.core.store.statistics.index.DuplicateEntryCountStatistic;
import org.locationtech.geowave.core.store.statistics.index.DuplicateEntryCountStatistic.DuplicateEntryCountValue;
import org.locationtech.geowave.core.store.statistics.index.FieldVisibilityCountStatistic;
import org.locationtech.geowave.core.store.statistics.index.FieldVisibilityCountStatistic.FieldVisibilityCountValue;
import org.locationtech.geowave.core.store.statistics.index.IndexMetaDataSetStatistic;
import org.locationtech.geowave.core.store.statistics.index.IndexMetaDataSetStatistic.IndexMetaDataSetValue;
import org.locationtech.geowave.core.store.statistics.index.IndexStatisticType;
import org.locationtech.geowave.core.store.statistics.index.MaxDuplicatesStatistic;
import org.locationtech.geowave.core.store.statistics.index.MaxDuplicatesStatistic.MaxDuplicatesValue;
import org.locationtech.geowave.core.store.statistics.index.PartitionsStatistic;
import org.locationtech.geowave.core.store.statistics.index.PartitionsStatistic.PartitionsValue;
import org.locationtech.geowave.core.store.statistics.index.RowRangeHistogramStatistic;
import org.locationtech.geowave.core.store.statistics.index.RowRangeHistogramStatistic.RowRangeHistogramValue;
import org.locationtech.geowave.core.store.statistics.query.DataTypeStatisticQueryBuilder;
import org.locationtech.geowave.core.store.statistics.query.FieldStatisticQueryBuilder;
import org.locationtech.geowave.core.store.statistics.query.IndexStatisticQueryBuilder;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.google.common.hash.BloomFilter;

/**
 * Base interface for constructing statistic queries.
 *
 * @param <V> the statistic value type
 * @param <R> the return type of the statistic value
 * @param <B> the builder type
 */
public interface StatisticQueryBuilder<V extends StatisticValue<R>, R, B extends StatisticQueryBuilder<V, R, B>> {

  /**
   * Set the tag for the query. Only statistics that match the given tag will be queried.
   *
   * @param tag the tag to use
   * @return {@code this}
   */
  B tag(final String tag);

  /**
   * Set the tag filter to internal statistics. If this is set, only internal statistics willb e
   * queried.
   *
   * @return {@code this}
   */
  B internal();

  /**
   * Add an authorization to the query.
   *
   * @param authorization the authorization to add
   * @return {@code this}
   */
  B addAuthorization(final String authorization);

  /**
   * Set the query authorizations to the given set.
   *
   * @param authorizations the authorizations to use
   * @return {@code this}
   */
  B authorizations(final String[] authorizations);


  /**
   * Sets the bins of the query. If a queried statistic uses a binning strategy, only values
   * contained in one of the bins matching {@code BinConstraints} will be returned.
   *
   * @param binConstraints the binConstraints object to use which will be appropriately interpreted
   *        for this query
   * @return {@code this}
   */
  B binConstraints(final BinConstraints binConstraints);

  /**
   * Build the statistic query.
   *
   * @return the statistic query
   */
  StatisticQuery<V, R> build();

  /**
   * Create a new index statistic query builder for the given statistic type.
   *
   * @param statisticType the index statistic type to query
   * @return the index statistic query builder
   */
  static <V extends StatisticValue<R>, R> IndexStatisticQueryBuilder<V, R> newBuilder(
      final IndexStatisticType<V> statisticType) {
    return new IndexStatisticQueryBuilder<>(statisticType);
  }

  /**
   * Create a new data type statistic query builder for the given statistic type.
   *
   * @param statisticType the data type statistic type to query
   * @return the data type statistic query builder
   */
  static <V extends StatisticValue<R>, R> DataTypeStatisticQueryBuilder<V, R> newBuilder(
      final DataTypeStatisticType<V> statisticType) {
    return new DataTypeStatisticQueryBuilder<>(statisticType);
  }

  /**
   * Create a new field statistic query builder for the given statistic type.
   *
   * @param statisticType the field statistic type to query
   * @return the field statistic query builder
   */
  static <V extends StatisticValue<R>, R> FieldStatisticQueryBuilder<V, R> newBuilder(
      final FieldStatisticType<V> statisticType) {
    return new FieldStatisticQueryBuilder<>(statisticType);
  }

  /**
   * Create a new index statistic query builder for a differing visibility count statistic.
   * 
   * @return the index statistic query builder
   */
  static IndexStatisticQueryBuilder<DifferingVisibilityCountValue, Long> differingVisibilityCount() {
    return newBuilder(DifferingVisibilityCountStatistic.STATS_TYPE);
  }

  /**
   * Create a new index statistic query builder for a duplicate entry count statistic.
   * 
   * @return the index statistic query builder
   */
  static IndexStatisticQueryBuilder<DuplicateEntryCountValue, Long> duplicateEntryCount() {
    return newBuilder(DuplicateEntryCountStatistic.STATS_TYPE);
  }

  /**
   * Create a new index statistic query builder for a field visibility count statistic.
   * 
   * @return the index statistic query builder
   */
  static IndexStatisticQueryBuilder<FieldVisibilityCountValue, Map<ByteArray, Long>> fieldVisibilityCount() {
    return newBuilder(FieldVisibilityCountStatistic.STATS_TYPE);
  }

  /**
   * Create a new index statistic query builder for an index metadata set statistic.
   * 
   * @return the index statistic query builder
   */
  static IndexStatisticQueryBuilder<IndexMetaDataSetValue, List<IndexMetaData>> indexMetaDataSet() {
    return newBuilder(IndexMetaDataSetStatistic.STATS_TYPE);
  }

  /**
   * Create a new index statistic query builder for a max duplicates statistic.
   * 
   * @return the index statistic query builder
   */
  static IndexStatisticQueryBuilder<MaxDuplicatesValue, Integer> maxDuplicates() {
    return newBuilder(MaxDuplicatesStatistic.STATS_TYPE);
  }

  /**
   * Create a new index statistic query builder for a partitions statistic.
   * 
   * @return the index statistic query builder
   */
  static IndexStatisticQueryBuilder<PartitionsValue, Set<ByteArray>> partitions() {
    return newBuilder(PartitionsStatistic.STATS_TYPE);
  }

  /**
   * Create a new index statistic query builder for a row range histogram statistic.
   * 
   * @return the index statistic query builder
   */
  static IndexStatisticQueryBuilder<RowRangeHistogramValue, NumericHistogram> rowRangeHistogram() {
    return newBuilder(RowRangeHistogramStatistic.STATS_TYPE);
  }

  /**
   * Create a new data type statistic query builder for a count statistic.
   * 
   * @return the data type statistic query builder
   */
  static DataTypeStatisticQueryBuilder<CountValue, Long> count() {
    return newBuilder(CountStatistic.STATS_TYPE);
  }

  /**
   * Create a new field statistic query builder for a bloom filter statistic.
   * 
   * @return the field statistic query builder
   */
  static FieldStatisticQueryBuilder<BloomFilterValue, BloomFilter<CharSequence>> bloomFilter() {
    return newBuilder(BloomFilterStatistic.STATS_TYPE);
  }

  /**
   * Create a new field statistic query builder for a count min sketch statistic.
   * 
   * @return the field statistic query builder
   */
  static FieldStatisticQueryBuilder<CountMinSketchValue, CountMinSketch> countMinSketch() {
    return newBuilder(CountMinSketchStatistic.STATS_TYPE);
  }

  /**
   * Create a new field statistic query builder for a fixed bin numeric histogram statistic.
   * 
   * @return the field statistic query builder
   */
  static FieldStatisticQueryBuilder<FixedBinNumericHistogramValue, FixedBinNumericHistogram> fixedBinNumericHistogram() {
    return newBuilder(FixedBinNumericHistogramStatistic.STATS_TYPE);
  }

  /**
   * Create a new field statistic query builder for a hyper log log statistic.
   * 
   * @return the field statistic query builder
   */
  static FieldStatisticQueryBuilder<HyperLogLogPlusValue, HyperLogLogPlus> hyperLogLog() {
    return newBuilder(HyperLogLogStatistic.STATS_TYPE);
  }

  /**
   * Create a new field statistic query builder for a numeric histogram statistic.
   * 
   * @return the field statistic query builder
   */
  static FieldStatisticQueryBuilder<NumericHistogramValue, NumericHistogram> numericHistogram() {
    return newBuilder(NumericHistogramStatistic.STATS_TYPE);
  }

  /**
   * Create a new field statistic query builder for a numeric mean statistic.
   * 
   * @return the field statistic query builder
   */
  static FieldStatisticQueryBuilder<NumericMeanValue, Double> numericMean() {
    return newBuilder(NumericMeanStatistic.STATS_TYPE);
  }

  /**
   * Create a new field statistic query builder for a numeric range statistic.
   * 
   * @return the field statistic query builder
   */
  static FieldStatisticQueryBuilder<NumericRangeValue, Range<Double>> numericRange() {
    return newBuilder(NumericRangeStatistic.STATS_TYPE);
  }

  /**
   * Create a new field statistic query builder for a numeric stats statistic.
   * 
   * @return the field statistic query builder
   */
  static FieldStatisticQueryBuilder<NumericStatsValue, Stats> numericStats() {
    return newBuilder(NumericStatsStatistic.STATS_TYPE);
  }
}
