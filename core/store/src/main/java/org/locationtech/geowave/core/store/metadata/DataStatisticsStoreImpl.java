/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataTypeStatistic;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IndexStatistic;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.StatisticId;
import org.locationtech.geowave.core.store.statistics.StatisticType;
import org.locationtech.geowave.core.store.statistics.StatisticUpdateCallback;
import org.locationtech.geowave.core.store.statistics.StatisticValueReader;
import org.locationtech.geowave.core.store.statistics.StatisticValueWriter;
import org.locationtech.geowave.core.store.statistics.StatisticsValueIterator;
import org.locationtech.geowave.core.store.statistics.binning.CompositeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.binning.DataTypeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.field.FieldStatisticId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

public class DataStatisticsStoreImpl extends
    AbstractGeoWavePersistence<Statistic<? extends StatisticValue<?>>> implements
    DataStatisticsStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataStatisticsStoreImpl.class);
  // this is fairly arbitrary at the moment because it is the only custom
  // server op added
  public static final int STATS_COMBINER_PRIORITY = 10;
  public static final String STATISTICS_COMBINER_NAME = "STATS_COMBINER";

  public DataStatisticsStoreImpl(
      final DataStoreOperations operations,
      final DataStoreOptions options) {
    super(operations, options, MetadataType.STATISTICS);
  }

  @Override
  protected ByteArray getPrimaryId(final Statistic<? extends StatisticValue<?>> persistedObject) {
    return persistedObject.getId().getUniqueId();
  }

  @Override
  protected ByteArray getSecondaryId(final Statistic<? extends StatisticValue<?>> persistedObject) {
    return persistedObject.getId().getGroupId();
  }

  @Override
  public boolean exists(final Statistic<? extends StatisticValue<?>> statistic) {
    return objectExists(getPrimaryId(statistic), getSecondaryId(statistic));
  }

  @Override
  public void addStatistic(final Statistic<? extends StatisticValue<?>> statistic) {
    this.addObject(statistic);
  }

  @Override
  public boolean removeStatistic(final Statistic<? extends StatisticValue<?>> statistic) {
    // Delete the statistic values
    removeStatisticValues(statistic);
    return deleteObject(getPrimaryId(statistic), getSecondaryId(statistic));
  }

  @Override
  public boolean removeStatistics(
      final Iterator<? extends Statistic<? extends StatisticValue<?>>> statistics) {
    boolean deleted = false;
    while (statistics.hasNext()) {
      final Statistic<? extends StatisticValue<?>> statistic = statistics.next();
      removeStatisticValues(statistic);
      deleted = deleteObject(getPrimaryId(statistic), getSecondaryId(statistic)) || deleted;
    }
    return deleted;
  }

  @Override
  public boolean removeStatistics(final Index index) {
    boolean removed = deleteObjects(IndexStatistic.generateGroupId(index.getName()));
    removed =
        deleteObjects(
            null,
            IndexStatistic.generateGroupId(index.getName()),
            operations,
            MetadataType.STATISTIC_VALUES,
            this) || removed;
    return removed;
  }

  @Override
  public boolean removeStatistics(final DataTypeAdapter<?> type, final Index... adapterIndices) {
    boolean removed = deleteObjects(DataTypeStatistic.generateGroupId(type.getTypeName()));
    removed =
        deleteObjects(
            null,
            DataTypeStatistic.generateGroupId(type.getTypeName()),
            operations,
            MetadataType.STATISTIC_VALUES,
            this) || removed;
    removed = deleteObjects(FieldStatistic.generateGroupId(type.getTypeName())) || removed;
    removed =
        deleteObjects(
            null,
            FieldStatistic.generateGroupId(type.getTypeName()),
            operations,
            MetadataType.STATISTIC_VALUES,
            this) || removed;
    for (final Index index : adapterIndices) {
      try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> statsIter =
          getIndexStatistics(index, null, null)) {
        while (statsIter.hasNext()) {
          final IndexStatistic<?> next = (IndexStatistic<?>) statsIter.next();
          removeTypeSpecificStatisticValues(next, type.getTypeName());
        }
      }
    }
    return removed;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean removeTypeSpecificStatisticValues(
      final IndexStatistic<?> indexStatistic,
      final String typeName) {
    if (indexStatistic.getBinningStrategy() == null) {
      return false;
    }
    final ByteArray adapterBin = DataTypeBinningStrategy.getBin(typeName);
    boolean removed = false;
    if (indexStatistic.getBinningStrategy() instanceof DataTypeBinningStrategy) {
      removed = removeStatisticValue(indexStatistic, adapterBin);
    } else if ((indexStatistic.getBinningStrategy() instanceof CompositeBinningStrategy)
        && ((CompositeBinningStrategy) indexStatistic.getBinningStrategy()).usesStrategy(
            DataTypeBinningStrategy.class)) {
      final CompositeBinningStrategy binningStrategy =
          (CompositeBinningStrategy) indexStatistic.getBinningStrategy();
      // TODO: The current metadata deleter only deletes exact values. One future optimization
      // could be to allow it to delete with a primary Id prefix. If the strategy index is 0,
      // a prefix delete could be used.
      final List<ByteArray> binsToRemove = Lists.newLinkedList();
      try (CloseableIterator<StatisticValue<Object>> valueIter =
          getStatisticValues((Statistic<StatisticValue<Object>>) indexStatistic)) {
        while (valueIter.hasNext()) {
          final ByteArray bin = valueIter.next().getBin();
          if (binningStrategy.binMatches(DataTypeBinningStrategy.class, bin, adapterBin)) {
            binsToRemove.add(bin);
          }
        }
      }
      for (final ByteArray bin : binsToRemove) {
        removed = removeStatisticValue(indexStatistic, bin) || removed;
      }
    }
    return removed;
  }

  @SuppressWarnings("unchecked")
  protected CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> getCachedObject(
      final ByteArray primaryId,
      final ByteArray secondaryId) {
    final Object cacheResult = getObjectFromCache(primaryId, secondaryId);

    // if there's an exact match in the cache return a singleton
    if (cacheResult != null) {
      return new CloseableIterator.Wrapper<>(
          Iterators.singletonIterator((Statistic<StatisticValue<Object>>) cacheResult));
    }
    return internalGetObjects(new MetadataQuery(primaryId.getBytes(), secondaryId.getBytes()));
  }

  protected CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> getBasicStatisticsInternal(
      final ByteArray secondaryId,
      final @Nullable StatisticType<? extends StatisticValue<?>> statisticType,
      final @Nullable String tag) {
    if (statisticType == null) {
      final CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> stats =
          getAllObjectsWithSecondaryId(secondaryId);
      if (tag == null) {
        return stats;
      }
      return new TagFilter(stats, tag);
    } else if (tag == null) {
      return internalGetObjects(
          new MetadataQuery(statisticType.getBytes(), secondaryId.getBytes(), true));
    }
    return getCachedObject(StatisticId.generateUniqueId(statisticType, tag), secondaryId);
  }

  protected CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> getFieldStatisticsInternal(
      final ByteArray secondaryId,
      final @Nullable StatisticType<? extends StatisticValue<?>> statisticType,
      final @Nullable String fieldName,
      final @Nullable String tag) {
    if (statisticType != null) {
      if (fieldName != null) {
        final ByteArray primaryId =
            FieldStatisticId.generateUniqueId(statisticType, fieldName, tag);
        if (tag != null) {
          return getCachedObject(primaryId, secondaryId);
        } else {
          return internalGetObjects(
              new MetadataQuery(primaryId.getBytes(), secondaryId.getBytes(), true));
        }
      } else {
        if (tag != null) {
          return new TagFilter(
              internalGetObjects(
                  new MetadataQuery(statisticType.getBytes(), secondaryId.getBytes(), true)),
              tag);
        } else {
          return internalGetObjects(
              new MetadataQuery(statisticType.getBytes(), secondaryId.getBytes(), true));
        }
      }
    }
    return new FieldStatisticFilter(getAllObjectsWithSecondaryId(secondaryId), fieldName, tag);
  }

  protected CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> getAllStatisticsInternal(
      final @Nullable StatisticType<? extends StatisticValue<?>> statisticType) {
    return internalGetObjects(
        new MetadataQuery(statisticType == null ? null : statisticType.getBytes(), null, true));
  }

  @SuppressWarnings("unchecked")
  @Override
  public CloseableIterator<? extends IndexStatistic<? extends StatisticValue<?>>> getIndexStatistics(
      final Index index,
      final @Nullable StatisticType<? extends StatisticValue<?>> statisticType,
      final @Nullable String tag) {
    return (CloseableIterator<? extends IndexStatistic<? extends StatisticValue<?>>>) getBasicStatisticsInternal(
        IndexStatistic.generateGroupId(index.getName()),
        statisticType,
        tag);
  }

  @SuppressWarnings("unchecked")
  @Override
  public CloseableIterator<? extends DataTypeStatistic<? extends StatisticValue<?>>> getDataTypeStatistics(
      final DataTypeAdapter<?> type,
      final @Nullable StatisticType<? extends StatisticValue<?>> statisticType,
      final @Nullable String tag) {
    return (CloseableIterator<? extends DataTypeStatistic<? extends StatisticValue<?>>>) getBasicStatisticsInternal(
        DataTypeStatistic.generateGroupId(type.getTypeName()),
        statisticType,
        tag);
  }

  @SuppressWarnings("unchecked")
  @Override
  public CloseableIterator<? extends FieldStatistic<? extends StatisticValue<?>>> getFieldStatistics(
      final DataTypeAdapter<?> type,
      final @Nullable StatisticType<? extends StatisticValue<?>> statisticType,
      final @Nullable String fieldName,
      final @Nullable String tag) {
    return (CloseableIterator<? extends FieldStatistic<? extends StatisticValue<?>>>) getFieldStatisticsInternal(
        FieldStatistic.generateGroupId(type.getTypeName()),
        statisticType,
        fieldName,
        tag);

  }

  @Override
  public CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> getAllStatistics(
      final @Nullable StatisticType<? extends StatisticValue<?>> statisticType) {
    return getAllStatisticsInternal(statisticType);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V extends StatisticValue<R>, R> Statistic<V> getStatisticById(
      final StatisticId<V> statisticId) {
    try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> iterator =
        getCachedObject(statisticId.getUniqueId(), statisticId.getGroupId())) {
      if (iterator.hasNext()) {
        return (Statistic<V>) iterator.next();
      }
    }
    return null;
  }


  @Override
  public <V extends StatisticValue<R>, R> void setStatisticValue(
      final Statistic<V> statistic,
      final V value) {
    if (statistic.getBinningStrategy() != null) {
      throw new UnsupportedOperationException(
          "The given statistic uses a binning strategy, but no bin was specified.");
    }
    removeStatisticValue(statistic);
    incorporateStatisticValue(statistic, value);
  }

  @Override
  public <V extends StatisticValue<R>, R> void setStatisticValue(
      final Statistic<V> statistic,
      final V value,
      final ByteArray bin) {
    if (statistic.getBinningStrategy() == null) {
      throw new UnsupportedOperationException(
          "The given statistic does not use a binning strategy, but a bin was specified.");
    }
    removeStatisticValue(statistic, bin);
    incorporateStatisticValue(statistic, value, bin);
  }

  @Override
  public <V extends StatisticValue<R>, R> void incorporateStatisticValue(
      final Statistic<V> statistic,
      final V value) {
    if (statistic.getBinningStrategy() != null) {
      throw new UnsupportedOperationException(
          "The given statistic uses a binning strategy, but no bin was specified.");
    }
    try (StatisticValueWriter<V> writer = createStatisticValueWriter(statistic)) {
      writer.writeStatisticValue(null, null, value);
    } catch (final Exception e) {
      LOGGER.error("Unable to write statistic value", e);
    }
  }

  @Override
  public <V extends StatisticValue<R>, R> void incorporateStatisticValue(
      final Statistic<V> statistic,
      final V value,
      final ByteArray bin) {
    if (statistic.getBinningStrategy() == null) {
      throw new UnsupportedOperationException(
          "The given statistic does not use a binning strategy, but a bin was specified.");
    }
    try (StatisticValueWriter<V> writer = createStatisticValueWriter(statistic)) {
      writer.writeStatisticValue(bin.getBytes(), null, value);
    } catch (final Exception e) {
      LOGGER.error("Unable to write statistic value", e);
    }
  }


  @Override
  public <V extends StatisticValue<R>, R> StatisticValueWriter<V> createStatisticValueWriter(
      final Statistic<V> statistic) {
    return new StatisticValueWriter<>(
        operations.createMetadataWriter(MetadataType.STATISTIC_VALUES),
        statistic);
  }

  private <V extends StatisticValue<R>, R> StatisticValueReader<V, R> createStatisticValueReader(
      final Statistic<V> statistic,
      final ByteArray bin,
      final boolean exact,
      final String... authorizations) {
    final byte[] primaryId;
    if ((bin == null) && !exact) {
      primaryId = StatisticValue.getValueId(statistic.getId(), new byte[0]);
    } else {
      primaryId = StatisticValue.getValueId(statistic.getId(), bin);
    }
    final MetadataQuery query =
        new MetadataQuery(
            primaryId,
            statistic.getId().getGroupId().getBytes(),
            !exact,
            authorizations);
    return new StatisticValueReader<>(
        operations.createMetadataReader(MetadataType.STATISTIC_VALUES).query(query),
        statistic);
  }

  private <V extends StatisticValue<R>, R> StatisticValueReader<V, R> createStatisticValueReader(
      final Statistic<V> statistic,
      final ByteArrayRange[] binRanges,
      final String... authorizations) {
    final MetadataQuery query =
        new MetadataQuery(
            Arrays.stream(binRanges).map(
                range -> new ByteArrayRange(
                    range.getStart() == null ? null
                        : StatisticValue.getValueId(statistic.getId(), range.getStart()),
                    range.getEnd() == null ? null
                        : StatisticValue.getValueId(statistic.getId(), range.getEnd()),
                    range.isSingleValue())).toArray(ByteArrayRange[]::new),
            statistic.getId().getGroupId().getBytes(),
            authorizations);
    return new StatisticValueReader<>(
        operations.createMetadataReader(MetadataType.STATISTIC_VALUES).query(query),
        statistic);
  }

  @Override
  public boolean removeStatisticValue(final Statistic<? extends StatisticValue<?>> statistic) {
    if (statistic.getBinningStrategy() != null) {
      throw new UnsupportedOperationException(
          "The given statistic uses a binning strategy, but no bin was specified.");
    }
    boolean deleted = false;
    try (
        MetadataDeleter deleter = operations.createMetadataDeleter(MetadataType.STATISTIC_VALUES)) {
      deleted =
          deleter.delete(
              new MetadataQuery(
                  statistic.getId().getUniqueId().getBytes(),
                  statistic.getId().getGroupId().getBytes()));
    } catch (final Exception e) {
      LOGGER.error("Unable to remove value for statistic", e);
    }
    return deleted;
  }

  @Override
  public boolean removeStatisticValue(
      final Statistic<? extends StatisticValue<?>> statistic,
      final ByteArray bin) {
    if (statistic.getBinningStrategy() == null) {
      throw new UnsupportedOperationException(
          "The given statistic does not use a binning strategy, but a bin was specified.");
    }
    boolean deleted = false;
    try (
        MetadataDeleter deleter = operations.createMetadataDeleter(MetadataType.STATISTIC_VALUES)) {
      deleted =
          deleter.delete(
              new MetadataQuery(
                  StatisticValue.getValueId(statistic.getId(), bin),
                  statistic.getId().getGroupId().getBytes()));
    } catch (final Exception e) {
      LOGGER.error("Unable to remove value for statistic", e);
    }
    return deleted;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean removeStatisticValues(final Statistic<? extends StatisticValue<?>> statistic) {
    if (statistic.getBinningStrategy() == null) {
      return removeStatisticValue(statistic);
    }
    // TODO: The performance of this operation could be improved if primary ID prefix queries were
    // allowed during delete.
    boolean deleted = false;
    final List<ByteArray> binsToRemove = Lists.newLinkedList();
    try (CloseableIterator<StatisticValue<Object>> valueIter =
        getStatisticValues((Statistic<StatisticValue<Object>>) statistic)) {
      while (valueIter.hasNext()) {
        final ByteArray bin = valueIter.next().getBin();
        binsToRemove.add(bin);
      }
    }
    for (final ByteArray bin : binsToRemove) {
      deleted = deleted || removeStatisticValue(statistic, bin);
    }
    return deleted;
  }

  @Override
  public CloseableIterator<? extends StatisticValue<?>> getStatisticValues(
      final Iterator<? extends Statistic<? extends StatisticValue<?>>> statistics,
      final ByteArrayConstraints binConstraints,
      final String... authorizations) {
    return new StatisticsValueIterator(this, statistics, binConstraints, authorizations);
  }

  @Override
  public <V extends StatisticValue<R>, R> V getStatisticValue(
      final Statistic<V> statistic,
      final String... authorizations) {
    if (statistic.getBinningStrategy() != null) {
      throw new UnsupportedOperationException(
          "The given statistic uses a binning strategy, but no bin was specified.");
    }
    try (StatisticValueReader<V, R> reader =
        createStatisticValueReader(statistic, null, true, authorizations)) {
      if (reader.hasNext()) {
        return reader.next();
      }
    }
    return null;
  }

  @Override
  public <V extends StatisticValue<R>, R> V getStatisticValue(
      final Statistic<V> statistic,
      final ByteArray bin,
      final String... authorizations) {
    if (statistic.getBinningStrategy() == null) {
      throw new UnsupportedOperationException(
          "The given statistic does not use a binning strategy, but a bin was specified.");
    }
    // allow for bin prefix scans
    try (StatisticValueReader<V, R> reader =
        createStatisticValueReader(statistic, bin, true, authorizations)) {
      if (reader.hasNext()) {
        return reader.next();
      }
    }
    return null;
  }

  @Override
  public <V extends StatisticValue<R>, R> CloseableIterator<V> getStatisticValues(
      final Statistic<V> statistic,
      final ByteArray binPrefix,
      final String... authorizations) {
    if (statistic.getBinningStrategy() == null) {
      throw new UnsupportedOperationException(
          "The given statistic does not use a binning strategy, but a bin was specified.");
    }
    return createStatisticValueReader(statistic, binPrefix, false, authorizations);
  }

  @Override
  public <V extends StatisticValue<R>, R> CloseableIterator<V> getStatisticValues(
      final Statistic<V> statistic,
      final ByteArrayRange[] binRanges,
      final String... authorizations) {
    return createStatisticValueReader(statistic, binRanges, authorizations);
  }

  @Override
  public <V extends StatisticValue<R>, R> CloseableIterator<V> getStatisticValues(
      final Statistic<V> statistic,
      final String... authorizations) {
    if (statistic.getBinningStrategy() != null) {
      return createStatisticValueReader(statistic, null, false, authorizations);
    }
    return createStatisticValueReader(statistic, null, true, authorizations);
  }

  @Override
  public <T> StatisticUpdateCallback<T> createUpdateCallback(
      final Index index,
      final AdapterToIndexMapping indexMapping,
      final InternalDataAdapter<T> adapter,
      final boolean updateAdapterStats) {
    final List<Statistic<? extends StatisticValue<?>>> statistics = Lists.newArrayList();
    if (index != null) {
      try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> indexStats =
          getIndexStatistics(index, null, null)) {
        while (indexStats.hasNext()) {
          statistics.add(indexStats.next());
        }
      }
    }
    if (updateAdapterStats) {
      try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> adapterStats =
          getDataTypeStatistics(adapter, null, null)) {
        while (adapterStats.hasNext()) {
          statistics.add(adapterStats.next());
        }
      }
      try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> fieldStats =
          getFieldStatistics(adapter, null, null, null)) {
        while (fieldStats.hasNext()) {
          statistics.add(fieldStats.next());
        }
      }
    }
    return new StatisticUpdateCallback<>(statistics, this, index, indexMapping, adapter);
  }

  @Override
  public void removeAll() {
    deleteObjects(null, null, operations, MetadataType.STATISTIC_VALUES, null);
    super.removeAll();
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean mergeStats() {
    final List<Statistic<StatisticValue<Object>>> statistics = new ArrayList<>();
    try (CloseableIterator<? extends Statistic<?>> it = getAllStatisticsInternal(null)) {
      while (it.hasNext()) {
        statistics.add((Statistic<StatisticValue<Object>>) it.next());
      }
    }
    for (final Statistic<StatisticValue<Object>> stat : statistics) {
      try (CloseableIterator<StatisticValue<Object>> it = this.getStatisticValues(stat)) {
        if (stat.getBinningStrategy() != null) {
          while (it.hasNext()) {
            final StatisticValue<Object> value = it.next();
            this.setStatisticValue(stat, value, value.getBin());
          }
        } else if (it.hasNext()) {
          this.setStatisticValue(stat, it.next());
        }
      }
    }
    return true;
  }

  protected static class TagFilter implements
      CloseableIterator<Statistic<? extends StatisticValue<?>>> {

    private final CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> source;
    private final String tag;

    private Statistic<? extends StatisticValue<?>> next = null;

    public TagFilter(
        final CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> source,
        final String tag) {
      this.source = source;
      this.tag = tag;
    }

    private void computeNext() {
      while (source.hasNext()) {
        final Statistic<? extends StatisticValue<?>> possibleNext = source.next();
        if (tag.equals(possibleNext.getTag())) {
          next = possibleNext;
          break;
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
    public Statistic<? extends StatisticValue<?>> next() {
      if (next == null) {
        computeNext();
      }
      final Statistic<? extends StatisticValue<?>> nextValue = next;
      next = null;
      return nextValue;
    }

    @Override
    public void close() {
      source.close();
    }

  }

  protected static class FieldStatisticFilter implements
      CloseableIterator<Statistic<? extends StatisticValue<?>>> {

    private final CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> source;
    private final String fieldName;
    private final String tag;

    private Statistic<? extends StatisticValue<?>> next = null;

    public FieldStatisticFilter(
        final CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> source,
        final String fieldName,
        final String tag) {
      this.source = source;
      this.fieldName = fieldName;
      this.tag = tag;
    }

    private void computeNext() {
      while (source.hasNext()) {
        final Statistic<? extends StatisticValue<?>> possibleNext = source.next();
        if (possibleNext instanceof FieldStatistic) {
          final FieldStatistic<? extends StatisticValue<?>> statistic =
              (FieldStatistic<? extends StatisticValue<?>>) possibleNext;
          if (((tag == null) || statistic.getTag().equals(tag))
              && ((fieldName == null) || statistic.getFieldName().equals(fieldName))) {
            next = possibleNext;
            break;
          }
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
    public Statistic<? extends StatisticValue<?>> next() {
      if (next == null) {
        computeNext();
      }
      final Statistic<? extends StatisticValue<?>> nextValue = next;
      next = null;
      return nextValue;
    }

    @Override
    public void close() {
      source.close();
    }
  }
}
