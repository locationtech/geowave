/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.constraints;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.index.text.ExplicitTextSearch;
import org.locationtech.geowave.core.index.text.TextIndexStrategy;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.AttributeIndex;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IndexFieldMapper;
import org.locationtech.geowave.core.store.api.IndexStatistic;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.base.BaseQueryOptions;
import org.locationtech.geowave.core.store.index.CustomIndex;
import org.locationtech.geowave.core.store.index.IndexFilter;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.index.TextAttributeIndexProvider.AdapterFieldTextIndexEntryConverter;
import org.locationtech.geowave.core.store.query.filter.ExpressionQueryFilter;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.locationtech.geowave.core.store.query.filter.expression.Filter;
import org.locationtech.geowave.core.store.query.filter.expression.FilterConstraints;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.StatisticId;
import org.locationtech.geowave.core.store.statistics.index.RowRangeHistogramStatistic;
import org.locationtech.geowave.core.store.statistics.index.RowRangeHistogramStatistic.RowRangeHistogramValue;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.internal.Sets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Determines the best index and provides constraints based on a given GeoWave filter.
 */
public class OptimalExpressionQuery implements
    AdapterAndIndexBasedQueryConstraints,
    QueryConstraints {
  private static final Logger LOGGER = LoggerFactory.getLogger(OptimalExpressionQuery.class);

  private Filter filter;
  private IndexFilter indexFilter;

  public OptimalExpressionQuery() {}

  public OptimalExpressionQuery(final Filter filter) {
    this(filter, null);
  }

  public OptimalExpressionQuery(final Filter filter, final IndexFilter indexFilter) {
    this.filter = filter;
    this.indexFilter = indexFilter;
  }

  private final Map<String, FilterConstraints<?>> constraintCache = Maps.newHashMap();

  @SuppressWarnings({"rawtypes", "unchecked"})
  public List<Pair<Index, List<InternalDataAdapter<?>>>> determineBestIndices(
      final BaseQueryOptions baseOptions,
      final InternalDataAdapter<?>[] adapters,
      final AdapterIndexMappingStore adapterIndexMappingStore,
      final IndexStore indexStore,
      final DataStatisticsStore statisticsStore) {
    final Map<Index, List<InternalDataAdapter<?>>> bestIndices = Maps.newHashMap();
    final Set<String> referencedFields = Sets.newHashSet();
    filter.addReferencedFields(referencedFields);
    for (final InternalDataAdapter<?> adapter : adapters) {
      if (!adapterMatchesFilter(adapter, referencedFields)) {
        continue;
      }
      final AdapterToIndexMapping[] adapterIndices =
          adapterIndexMappingStore.getIndicesForAdapter(adapter.getAdapterId());
      final Map<Index, FilterConstraints<?>> indexConstraints = Maps.newHashMap();
      Index bestIndex = null;
      for (final AdapterToIndexMapping mapping : adapterIndices) {
        if ((baseOptions.getIndexName() != null)
            && !baseOptions.getIndexName().equals(mapping.getIndexName())) {
          continue;
        }
        final Index index = mapping.getIndex(indexStore);
        if (indexFilter != null && !indexFilter.test(index)) {
          continue;
        }
        if ((bestIndex == null)
            || ((bestIndex instanceof AttributeIndex) && !(index instanceof AttributeIndex))) {
          bestIndex = index;
        }
        final Set<String> indexedFields = Sets.newHashSet();
        final Class<? extends Comparable> filterClass;
        if ((index instanceof CustomIndex)
            && (((CustomIndex<?, ?>) index).getCustomIndexStrategy() instanceof TextIndexStrategy)) {
          final TextIndexStrategy<?> indexStrategy =
              (TextIndexStrategy<?>) ((CustomIndex<?, ?>) index).getCustomIndexStrategy();
          if (!(indexStrategy.getEntryConverter() instanceof AdapterFieldTextIndexEntryConverter)) {
            continue;
          }
          indexedFields.add(
              ((AdapterFieldTextIndexEntryConverter<?>) indexStrategy.getEntryConverter()).getFieldName());
          filterClass = String.class;
        } else {
          for (final IndexFieldMapper<?, ?> mapper : mapping.getIndexFieldMappers()) {
            for (final String adapterField : mapper.getAdapterFields()) {
              indexedFields.add(adapterField);
            }
          }
          // Remove any fields that are part of the common index model, but not used in the index
          // strategy. They shouldn't be considered when trying to find a best match. In the future
          // it may be useful to consider an index that has extra common index dimensions that
          // contain filtered fields over one that only matches indexed dimensions. For example, if
          // I have a spatial index, and a spatial index that stores time, it should pick the one
          // that stores time if I supply a temporal constraint, even though it isn't part of the
          // index strategy.
          final int modelDimensions = index.getIndexModel().getDimensions().length;
          final int strategyDimensions =
              index.getIndexStrategy().getOrderedDimensionDefinitions().length;
          for (int i = modelDimensions - 1; i >= strategyDimensions; i--) {
            final IndexFieldMapper<?, ?> mapper =
                mapping.getMapperForIndexField(
                    index.getIndexModel().getDimensions()[i].getFieldName());
            for (final String adapterField : mapper.getAdapterFields()) {
              indexedFields.remove(adapterField);
            }
          }
          filterClass = Double.class;
        }
        if (referencedFields.containsAll(indexedFields)) {
          final FilterConstraints<?> constraints =
              filter.getConstraints(
                  filterClass,
                  statisticsStore,
                  adapter,
                  mapping,
                  index,
                  indexedFields);
          if (constraints.constrainsAllFields(indexedFields)) {
            indexConstraints.put(index, constraints);
          }
        }
      }
      if (indexConstraints.size() == 1) {
        final Entry<Index, FilterConstraints<?>> bestEntry =
            indexConstraints.entrySet().iterator().next();
        bestIndex = bestEntry.getKey();
        constraintCache.put(adapter.getTypeName(), bestEntry.getValue());
      } else if (indexConstraints.size() > 1) {
        // determine which constraint is the best
        double bestCardinality = Double.MAX_VALUE;
        Index bestConstrainedIndex = null;
        for (final Entry<Index, FilterConstraints<?>> entry : indexConstraints.entrySet()) {
          final QueryRanges ranges = entry.getValue().getQueryRanges(baseOptions, statisticsStore);
          if (ranges.isEmpty()) {
            continue;
          }
          // TODO: A future optimization would be to add a default numeric histogram for any numeric
          // index dimensions and just use the index data ranges to determine cardinality rather
          // than decomposing query ranges.
          final StatisticId<RowRangeHistogramValue> statisticId =
              IndexStatistic.generateStatisticId(
                  entry.getKey().getName(),
                  RowRangeHistogramStatistic.STATS_TYPE,
                  Statistic.INTERNAL_TAG);
          final RowRangeHistogramStatistic histogram =
              (RowRangeHistogramStatistic) statisticsStore.getStatisticById(statisticId);
          final double cardinality =
              DataStoreUtils.cardinality(
                  statisticsStore,
                  histogram,
                  adapter,
                  bestConstrainedIndex,
                  ranges);
          if ((bestConstrainedIndex == null) || (cardinality < bestCardinality)) {
            bestConstrainedIndex = entry.getKey();
            bestCardinality = cardinality;
          }
        }
        if (bestConstrainedIndex != null) {
          bestIndex = bestConstrainedIndex;
          constraintCache.put(adapter.getTypeName(), indexConstraints.get(bestIndex));
        }
      }
      if (bestIndex == null) {
        continue;
      }
      if (!bestIndices.containsKey(bestIndex)) {
        bestIndices.put(bestIndex, Lists.newArrayList());
      }
      bestIndices.get(bestIndex).add(adapter);
    }
    return bestIndices.entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue())).collect(
        Collectors.toList());
  }

  private boolean adapterMatchesFilter(
      final DataTypeAdapter<?> adapter,
      final Set<String> filteredFields) {
    for (final String field : filteredFields) {
      if (adapter.getFieldDescriptor(field) == null) {
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public QueryConstraints createQueryConstraints(
      final InternalDataAdapter<?> adapter,
      final Index index,
      final AdapterToIndexMapping indexMapping) {
    if (!constraintCache.containsKey(adapter.getTypeName())) {
      filter.prepare(adapter, indexMapping, index);
      return new FilteredEverythingQuery(
          Lists.newArrayList(new ExpressionQueryFilter<>(filter, adapter, indexMapping)));
    }
    final Filter reduced =
        filter.removePredicatesForFields(
            constraintCache.get(adapter.getTypeName()).getExactConstrainedFields());
    final List<QueryFilter> filterList;
    if (reduced != null) {
      reduced.prepare(adapter, indexMapping, index);
      filterList = Lists.newArrayList(new ExpressionQueryFilter<>(reduced, adapter, indexMapping));
    } else {
      filterList = Lists.newArrayList();
    }
    if (index instanceof CustomIndex) {
      return new CustomQueryConstraints(
          new ExplicitTextSearch((List) constraintCache.get(adapter.getTypeName()).getIndexData()),
          filterList);
    }
    return new ExplicitFilteredQuery(
        (List) constraintCache.get(adapter.getTypeName()).getIndexData(),
        filterList);
  }

  @Override
  public byte[] toBinary() {
    byte[] filterBytes;
    if (filter == null) {
      LOGGER.warn("Filter is null");
      filterBytes = new byte[] {};
    } else {
      filterBytes = PersistenceUtils.toBinary(filter);
    }
    byte[] indexFilterBytes;
    if (indexFilter == null) {
      indexFilterBytes = new byte[] {};
    } else {
      indexFilterBytes = PersistenceUtils.toBinary(indexFilter);
    }
    final ByteBuffer buffer =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(filterBytes.length)
                + filterBytes.length
                + indexFilterBytes.length);
    VarintUtils.writeUnsignedInt(filterBytes.length, buffer);
    buffer.put(filterBytes);
    buffer.put(indexFilterBytes);
    return buffer.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    byte[] filterBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(filterBytes);
    if (filterBytes.length > 0) {
      filter = (Filter) PersistenceUtils.fromBinary(filterBytes);
    } else {
      LOGGER.warn("CQL filter is empty bytes");
      filter = null;
    }
    if (buffer.hasRemaining()) {
      final byte[] indexFilterBytes = new byte[buffer.remaining()];
      buffer.get(indexFilterBytes);
      indexFilter = (IndexFilter) PersistenceUtils.fromBinary(indexFilterBytes);
    } else {
      indexFilter = null;
    }
  }
}
