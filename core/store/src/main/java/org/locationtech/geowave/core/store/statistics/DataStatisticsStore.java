/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics;

import java.util.Iterator;
import javax.annotation.Nullable;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataTypeStatistic;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IndexStatistic;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.statistics.binning.DataTypeBinningStrategy;

/**
 * This is responsible for persisting data statistics (either in memory or to disk depending on the
 * implementation).
 */
public interface DataStatisticsStore {

  /**
   * Determines if the given statistic exists in the data store.
   *
   * @param statistic the statistic to check for
   */
  boolean exists(Statistic<? extends StatisticValue<?>> statistic);

  /**
   * Add a statistic to the data store.
   *
   * @param statistic the statistic to add
   */
  void addStatistic(Statistic<? extends StatisticValue<?>> statistic);

  /**
   * Remove a statistic from the data store.
   *
   * @param statistic the statistic to remove
   * @return {@code true} if the statistic existed and was removed
   */
  boolean removeStatistic(Statistic<? extends StatisticValue<?>> statistic);

  /**
   * Remove a set of statistics from the data store.
   *
   * @param statistics the statistics to remove
   * @return {@code true} if statistics were removed
   */
  boolean removeStatistics(Iterator<? extends Statistic<? extends StatisticValue<?>>> statistics);

  /**
   * Remove statistics associated with the given index.
   *
   * @param index the index to remove statistics for
   * @return {@code true} if statistics were removed
   */
  boolean removeStatistics(Index index);

  /**
   * Remove statistics associated with the given data type.
   *
   * @param type the type to remove statistics for
   * @param adapterIndices indices used by the data type
   * @return {@code true} if statistics were removed
   */
  boolean removeStatistics(DataTypeAdapter<?> type, Index... adapterIndices);

  /**
   * Get statistics for the given index.
   *
   * @param index the index to get statistics for
   * @param statisticType an optional statistic type filter
   * @param tag an optional tag filter
   * @return a list of index statistics for the given index
   */
  CloseableIterator<? extends IndexStatistic<? extends StatisticValue<?>>> getIndexStatistics(
      final Index index,
      final @Nullable StatisticType<? extends StatisticValue<?>> statisticType,
      final @Nullable String tag);

  /**
   * Get statistics for the given data type.
   *
   * @param type the type to get statistics for
   * @param statisticType an optional statistic type filter
   * @param tag an optional tag filter
   * @return a list of data type statistics for the given type
   */
  CloseableIterator<? extends DataTypeStatistic<? extends StatisticValue<?>>> getDataTypeStatistics(
      final DataTypeAdapter<?> type,
      final @Nullable StatisticType<? extends StatisticValue<?>> statisticType,
      final @Nullable String tag);

  /**
   * Get all field statistics for the given type. If a field name is specified, only statistics that
   * pertain to that field will be returned.
   *
   * @param type the type to get statistics for
   * @param statisticType an optional statistic type filter
   * @param fieldName an optional field name filter
   * @param tag an optional tag filter
   * @return a list of field statistics for the given type
   */
  CloseableIterator<? extends FieldStatistic<? extends StatisticValue<?>>> getFieldStatistics(
      final DataTypeAdapter<?> type,
      final @Nullable StatisticType<? extends StatisticValue<?>> statisticType,
      final @Nullable String fieldName,
      final @Nullable String tag);

  /**
   * Get all statistics in the data store.
   *
   * @param statisticType an optional statistic type filter
   * @return a list of statistics in the data store
   */
  CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> getAllStatistics(
      final @Nullable StatisticType<? extends StatisticValue<?>> statisticType);

  /**
   * Gets the statistic with the given {@link StatisticId}, or {@code null} if it could not be
   * found.
   *
   * @param statisticId the id of the statistic to get
   * @return the statistic that matched the given ID
   */
  <V extends StatisticValue<R>, R> Statistic<V> getStatisticById(final StatisticId<V> statisticId);

  /**
   * This will write the statistic value to the underlying store. Note that this will overwrite
   * whatever the current persisted values are for the given statistic. Use incorporateStatistic to
   * aggregate the statistic value with any existing values. This method is not applicable to
   * statistics that use a binning strategy.
   *
   * @param statistic the statistic that the value belongs to
   * @param value the value to set
   */
  <V extends StatisticValue<R>, R> void setStatisticValue(Statistic<V> statistic, V value);

  /**
   * This will write the statistic value to the underlying store. Note that this will overwrite
   * whatever the current persisted values are for the given statistic. Use incorporateStatistic to
   * aggregate the statistic value with any existing values. This method is not applicable to
   * statistics that do not use a binning strategy.
   *
   * @param statistic the statistic that the value belongs to
   * @param value the value to set
   * @param bin the bin that the value belongs to
   */
  <V extends StatisticValue<R>, R> void setStatisticValue(
      Statistic<V> statistic,
      V value,
      ByteArray bin);

  /**
   * Add the statistic value to the store, preserving the existing value. This method is not
   * applicable to statistics that use a binning strategy.
   *
   * @param statistic the statistic to that the value belongs to
   * @param value the value to add
   */
  <V extends StatisticValue<R>, R> void incorporateStatisticValue(Statistic<V> statistic, V value);

  /**
   * Add the statistic value to the store, preserving the existing value. This method is not
   * applicable to statistics that do not use a binning strategy.
   *
   * @param statistic the statistic to that the value belongs to
   * @param value the value to add
   * @param bin the bin that the value belongs to
   */
  <V extends StatisticValue<R>, R> void incorporateStatisticValue(
      Statistic<V> statistic,
      V value,
      ByteArray bin);

  /**
   * Removes the value of the given statistic. This method is not applicable to statistics that use
   * a binning strategy.
   *
   * @param statistic the statistic to remove the value for
   * @return {@code true} if the value was removed
   */
  boolean removeStatisticValue(Statistic<? extends StatisticValue<?>> statistic);

  /**
   * Removes the value of the given statistic. This method is not applicable to statistics that do
   * not use a binning strategy.
   *
   * @param statistic the statistic to remove the value for
   * @param bin the bin of the statistic value to remove
   * @return {@code true} if the value was removed
   */
  boolean removeStatisticValue(Statistic<? extends StatisticValue<?>> statistic, ByteArray bin);

  /**
   * Removes all values associated with the given statistic. If the statistic uses a binning
   * strategy, all bins will be removed.
   *
   * @param statistic the statistic to remove values for
   * @return {@code true} if values were removed
   */
  boolean removeStatisticValues(Statistic<? extends StatisticValue<?>> statistic);

  /**
   * Remove all type-specific values from the given index statistic. If the statistic does not use a
   * {@link DataTypeBinningStrategy}, nothing will be removed.
   *
   * @param statistic
   * @param typeName
   * @return
   */
  boolean removeTypeSpecificStatisticValues(
      IndexStatistic<? extends StatisticValue<?>> statistic,
      String typeName);

  /**
   * Creates a writer that can be used to write values for a given statistic.
   *
   * @param statistic the statistic to write values for
   * @return a new statistic value writer
   */
  <V extends StatisticValue<R>, R> StatisticValueWriter<V> createStatisticValueWriter(
      Statistic<V> statistic);

  /**
   * Creates a callback that can be used to update statistics for the given index and adapter.
   *
   * @param index the index
   * @param type the data type
   * @param updateAdapterStats if {@code true} adapter statistics will be updated, otherwise only
   *        index statistics will be updated
   * @return a statistics update callback
   */
  <T> StatisticUpdateCallback<T> createUpdateCallback(
      Index index,
      AdapterToIndexMapping indexMapping,
      InternalDataAdapter<T> type,
      boolean updateAdapterStats);

  /**
   * Returns all values for each provided statistic. If a set of bins are provided, statistics that
   * use a binning strategy will only return values that match one of the given bins.
   *
   * @param statistics the statistics to get values for
   * @param binConstraints an optional bins filter
   * @param authorizations authorizations for the query
   * @return an iterator for all matching statistic values
   */
  CloseableIterator<? extends StatisticValue<?>> getStatisticValues(
      final Iterator<? extends Statistic<? extends StatisticValue<?>>> statistics,
      @Nullable final ByteArrayConstraints binConstraints,
      final String... authorizations);

  /**
   * Return the value of the given statistic. This method is not applicable to statistics that use a
   * binning strategy.
   *
   * @param statistic the statistic to get the value of
   * @param authorizations authorizations for the query
   * @return the value of the statistic, or {@code null} if it was not found
   */
  <V extends StatisticValue<R>, R> V getStatisticValue(
      final Statistic<V> statistic,
      String... authorizations);

  /**
   * Return the value of the given statistic. This method is not applicable to statistics that do
   * not use a binning strategy.
   *
   * @param statistic the statistic to get the value of
   * @param bin the bin of the value to get
   * @param authorizations authorizations for the query
   * @return the value of the statistic, or {@code null} if it was not found
   */
  <V extends StatisticValue<R>, R> V getStatisticValue(
      final Statistic<V> statistic,
      ByteArray bin,
      String... authorizations);

  /**
   * Return the values of the given statistic that have bins that match the given ranges. This
   * method is not applicable to statistics that do not use a binning strategy.
   *
   * @param statistic the statistic to get the value of
   * @param binRanges the ranges of bins to get values for
   * @param authorizations authorizations for the query
   * @return the value of the statistic, or {@code null} if it was not found
   */
  <V extends StatisticValue<R>, R> CloseableIterator<V> getStatisticValues(
      final Statistic<V> statistic,
      ByteArrayRange[] binRanges,
      String... authorizations);

  /**
   * Return the values of the given statistic that have bins that start with the given prefix. This
   * method is not applicable to statistics that do not use a binning strategy.
   *
   * @param statistic the statistic to get the value of
   * @param binPrefix the bin prefix to get values for
   * @param authorizations authorizations for the query
   * @return the matching values of the statistic
   */
  <V extends StatisticValue<R>, R> CloseableIterator<V> getStatisticValues(
      final Statistic<V> statistic,
      ByteArray binPrefix,
      String... authorizations);

  /**
   * Returns all of the values for a given statistic. If the statistic uses a binning strategy, each
   * bin will be returned as a separate value.
   *
   * @param statistic the statistic to get values for
   * @param authorizations authorizations for the query
   * @return the values for the statistic
   */
  <V extends StatisticValue<R>, R> CloseableIterator<V> getStatisticValues(
      final Statistic<V> statistic,
      String... authorizations);

  /**
   * Merges all statistic values that share the same key. Every separate write to a data type can
   * create new values for a statistic. Over time, this can result in a lot of values for a single
   * statistic. This function can be used to merge those values to improve statistic query
   * performance.
   *
   * @return {@code true} if the merge was successful
   */
  boolean mergeStats();

  /**
   * Remove all statistics from the data store.
   */
  void removeAll();
}
