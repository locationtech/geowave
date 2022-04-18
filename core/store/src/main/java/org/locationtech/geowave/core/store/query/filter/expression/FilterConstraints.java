/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.index.IndexMetaData;
import org.locationtech.geowave.core.index.MultiDimensionalIndexData;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.text.MultiDimensionalTextData;
import org.locationtech.geowave.core.index.text.TextIndexStrategy;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IndexFieldMapper;
import org.locationtech.geowave.core.store.base.BaseQueryOptions;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CustomIndex;
import org.locationtech.geowave.core.store.index.TextAttributeIndexProvider.AdapterFieldTextIndexEntryConverter;
import org.locationtech.geowave.core.store.query.filter.expression.IndexFieldConstraints.DimensionConstraints;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericFieldConstraints;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextFieldConstraints;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.InternalStatisticsHelper;
import org.locationtech.geowave.core.store.statistics.index.IndexMetaDataSetStatistic;
import org.locationtech.geowave.core.store.statistics.index.IndexMetaDataSetStatistic.IndexMetaDataSetValue;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Provides constraints for an adapter/index based on a GeoWave filter expression.
 */
public class FilterConstraints<V extends Comparable<V>> {

  private DataTypeAdapter<?> adapter;
  private AdapterToIndexMapping indexMapping;
  private Index index;
  private Map<String, IndexFieldConstraints<V>> fieldConstraints;
  private List<MultiDimensionalIndexData<V>> cachedIndexData = null;

  public FilterConstraints(
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index,
      final Map<String, IndexFieldConstraints<V>> fieldConstraints) {
    this.adapter = adapter;
    this.indexMapping = indexMapping;
    this.index = index;
    this.fieldConstraints = fieldConstraints;
  }

  /**
   * Get the constraints for the given field.
   * 
   * @param fieldName the field to get constraints for
   * @return the field constraints, or {@code null} if there weren't any
   */
  public IndexFieldConstraints<?> getFieldConstraints(final String fieldName) {
    return fieldConstraints.get(fieldName);
  }

  /**
   * @return the number of constrained fields
   */
  public int getFieldCount() {
    return fieldConstraints.size();
  }

  /**
   * Determines whether or not all of the provided fields are constrained.
   * 
   * @param fields the fields to check
   * @return {@code true} if all of the fields are constrained
   */
  public boolean constrainsAllFields(final Set<String> fields) {
    return fields.stream().allMatch(f -> fieldConstraints.containsKey(f));
  }

  /**
   * @return a set of fields that are exactly constrained, i.e. the ranges represent the predicate
   *         exactly
   */
  public Set<String> getExactConstrainedFields() {
    return fieldConstraints.entrySet().stream().filter(e -> e.getValue().isExact()).map(
        e -> e.getKey()).collect(Collectors.toSet());
  }

  private boolean isSingleDimension(
      final String indexFieldName,
      final NumericDimensionField<?>[] dimensions) {
    return Arrays.stream(dimensions).filter(
        dim -> dim.getFieldName().equals(indexFieldName)).count() == 1;
  }

  /**
   * Get the multi-dimensional index data from these constraints.
   * 
   * @return the multi-dimensional index data
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public List<MultiDimensionalIndexData<V>> getIndexData() {
    if (cachedIndexData == null) {
      if ((adapter == null) || (index == null) || (indexMapping == null)) {
        return Lists.newArrayList();
      }
      if (index instanceof CustomIndex) {
        final TextIndexStrategy indexStrategy =
            (TextIndexStrategy) ((CustomIndex) index).getCustomIndexStrategy();
        if (!(indexStrategy.getEntryConverter() instanceof AdapterFieldTextIndexEntryConverter)) {
          throw new RuntimeException("Unable to determine adapter field used by text index.");
        }
        final String fieldName =
            ((AdapterFieldTextIndexEntryConverter) indexStrategy.getEntryConverter()).getFieldName();
        final IndexFieldConstraints<?> fieldConstraint = fieldConstraints.get(fieldName);
        final List<DimensionConstraints<String>> dimensionConstraints = Lists.newArrayList();
        if (fieldConstraint == null) {
          dimensionConstraints.add(
              DimensionConstraints.of(
                  Lists.newArrayList(
                      FilterRange.of((String) null, (String) null, true, true, true))));
        } else if (fieldConstraint instanceof TextFieldConstraints) {
          final DimensionConstraints<String> dimensionConstraint =
              ((TextFieldConstraints) fieldConstraint).getDimensionRanges(0);
          if (dimensionConstraint == null) {
            dimensionConstraints.add(
                DimensionConstraints.of(
                    Lists.newArrayList(
                        FilterRange.of((String) null, (String) null, true, true, true))));
          } else {
            dimensionConstraints.add(dimensionConstraint);
          }
        } else {
          throw new RuntimeException("Non-text field constraints cannot be used for a text index.");
        }
        cachedIndexData = (List) TextFieldConstraints.toIndexData(dimensionConstraints);
      } else {
        // Right now all index strategies that aren't custom are numeric
        final CommonIndexModel indexModel = index.getIndexModel();
        final int numStrategyDimensions =
            index.getIndexStrategy().getOrderedDimensionDefinitions().length;
        final List<DimensionConstraints<Double>> dimensionConstraints =
            Lists.newArrayListWithCapacity(numStrategyDimensions);
        final Map<String, Integer> indexFieldDimensions = Maps.newHashMap();
        final NumericDimensionField<?>[] dimensions = indexModel.getDimensions();
        int dimensionIndex = 0;
        for (final NumericDimensionField<?> indexField : dimensions) {
          if (dimensionIndex >= numStrategyDimensions) {
            // Only build constraints for dimensions used by the index strategy.
            break;
          }
          dimensionIndex++;
          final String indexFieldName = indexField.getFieldName();
          if (!indexFieldDimensions.containsKey(indexFieldName)) {
            indexFieldDimensions.put(indexFieldName, 0);
          }
          final int indexFieldDimension = indexFieldDimensions.get(indexFieldName);
          final IndexFieldMapper<?, ?> mapper = indexMapping.getMapperForIndexField(indexFieldName);
          final String[] adapterFields = mapper.getIndexOrderedAdapterFields();
          IndexFieldConstraints<?> fieldConstraint = null;
          if (adapterFields.length > 1 && isSingleDimension(indexFieldName, dimensions)) {
            // If multiple fields are mapped to the same index dimension, combine all of their
            // constraints
            for (int i = 0; i < adapterFields.length; i++) {
              final IndexFieldConstraints<?> constraint = fieldConstraints.get(adapterFields[i]);
              if (fieldConstraint == null) {
                fieldConstraint = constraint;
              } else {
                fieldConstraint.and((IndexFieldConstraints) constraint);
              }
            }
          } else {
            fieldConstraint =
                fieldConstraints.get(adapterFields[indexFieldDimension % adapterFields.length]);
          }

          if (fieldConstraint == null) {
            dimensionConstraints.add(
                DimensionConstraints.of(
                    Lists.newArrayList(
                        FilterRange.of((Double) null, (Double) null, true, true, true))));
          } else if (fieldConstraint instanceof NumericFieldConstraints) {
            final DimensionConstraints<Double> dimensionConstraint =
                ((NumericFieldConstraints) fieldConstraint).getDimensionRanges(
                    indexFieldDimension % fieldConstraint.getDimensionCount());
            if (dimensionConstraint == null) {
              dimensionConstraints.add(
                  DimensionConstraints.of(
                      Lists.newArrayList(
                          FilterRange.of((Double) null, (Double) null, true, true, true))));
            } else {
              dimensionConstraints.add(dimensionConstraint);
            }
            indexFieldDimensions.put(indexFieldName, indexFieldDimension + 1);
          } else {
            throw new RuntimeException(
                "Non-numeric field constraints cannot be used for a numeric index.");
          }
        }
        cachedIndexData = (List) NumericFieldConstraints.toIndexData(dimensionConstraints);
      }
    }
    return cachedIndexData;
  }

  /**
   * Combine these constraints with another set of constraints using the OR operator.
   * 
   * @param other the constraints to combine
   */
  public void or(final FilterConstraints<V> other) {
    if (adapter == null) {
      adapter = other.adapter;
      index = other.index;
      indexMapping = other.indexMapping;
    }
    final Set<String> constrainedFields = getCombinedFields(other);
    for (final String field : constrainedFields) {
      final IndexFieldConstraints<V> fieldRanges1 = fieldConstraints.get(field);
      final IndexFieldConstraints<V> fieldRanges2 = other.fieldConstraints.get(field);
      if ((fieldRanges1 == null) || (fieldRanges2 == null)) {
        fieldConstraints.remove(field);
      } else {
        fieldRanges1.or(fieldRanges2);
      }
    }
  }

  /**
   * Combine these constraints with another set of constraints using the AND operator.
   * 
   * @param other the constraints to combine
   */
  public void and(final FilterConstraints<V> other) {
    if (adapter == null) {
      adapter = other.adapter;
      index = other.index;
      indexMapping = other.indexMapping;
      fieldConstraints = other.fieldConstraints;
    } else {
      final Set<String> constrainedFields = getCombinedFields(other);
      for (final String field : constrainedFields) {
        final IndexFieldConstraints<V> fieldRanges1 = fieldConstraints.get(field);
        final IndexFieldConstraints<V> fieldRanges2 = other.fieldConstraints.get(field);
        if (fieldRanges1 == null) {
          fieldConstraints.put(field, fieldRanges2);
        } else if (fieldRanges2 != null) {
          fieldRanges1.and(fieldRanges2);
        }
      }
    }
  }

  /**
   * Get the inverse of these constraints. Only 1-dimensional field constraints can be accurately
   * inverted, anything else will result in no constraints.
   */
  public void invert() {
    for (final IndexFieldConstraints<V> fieldConstraint : fieldConstraints.values()) {
      // Only invert if there is one constrained dimension, see Not#getConstraints for why this is.
      if (fieldConstraint.getDimensionCount() == 1) {
        fieldConstraint.invert();
      } else {
        fieldConstraints.clear();
        break;
      }
    }
  }

  /**
   * Get the raw query ranges represented by this filter's index data.
   * 
   * @param baseOptions the base query options
   * @param statisticsStore the data statistics store
   * @return the query ranges
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public QueryRanges getQueryRanges(
      final BaseQueryOptions baseOptions,
      final DataStatisticsStore statisticsStore) {
    if ((index instanceof CustomIndex)
        && (((CustomIndex<?, ?>) index).getCustomIndexStrategy() instanceof TextIndexStrategy)) {
      final List<MultiDimensionalTextData> indexData = (List) getIndexData();
      if (indexData.size() > 0) {
        final TextIndexStrategy<?> indexStrategy =
            (TextIndexStrategy<?>) ((CustomIndex<?, ?>) index).getCustomIndexStrategy();
        final List<QueryRanges> ranges =
            indexData.stream().map(data -> indexStrategy.getQueryRanges(data)).collect(
                Collectors.toList());
        if (ranges.size() == 1) {
          return ranges.get(0);
        }
        return new QueryRanges(ranges);
      }
    } else if (!(index instanceof CustomIndex)) {
      final List<MultiDimensionalNumericData> indexData = (List) getIndexData();
      if (indexData.size() > 0) {
        final IndexMetaData[] hints;
        final IndexMetaDataSetValue value =
            InternalStatisticsHelper.getIndexStatistic(
                statisticsStore,
                IndexMetaDataSetStatistic.STATS_TYPE,
                index.getName(),
                adapter.getTypeName(),
                null,
                baseOptions.getAuthorizations());
        if (value != null) {
          hints = value.getValue().toArray(new IndexMetaData[value.getValue().size()]);
        } else {
          hints = new IndexMetaData[0];
        }
        int maxRangeDecomposition =
            baseOptions.getMaxRangeDecomposition() != null ? baseOptions.getMaxRangeDecomposition()
                : 2000;
        return DataStoreUtils.constraintsToQueryRanges(
            indexData,
            index,
            baseOptions.getTargetResolutionPerDimensionForHierarchicalIndex(),
            maxRangeDecomposition,
            hints);
      }
    }
    return new QueryRanges();
  }

  private Set<String> getCombinedFields(final FilterConstraints<V> other) {
    final Set<String> constrainedFields = Sets.newHashSet(fieldConstraints.keySet());
    constrainedFields.addAll(other.fieldConstraints.keySet());
    return constrainedFields;
  }

  /**
   * Create a filter constraint for a single field.
   * 
   * @param <V> the constraint class
   * @param adapter the data type adapter
   * @param indexMapping the adapter to index mapping
   * @param index the index
   * @param fieldName the name of the constrained field
   * @param constraints the field constraints for the field
   * @return the constructed filter constraints
   */
  public static <V extends Comparable<V>> FilterConstraints<V> of(
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index,
      final String fieldName,
      final IndexFieldConstraints<V> constraints) {
    final Map<String, IndexFieldConstraints<V>> fieldConstraints = Maps.newHashMap();
    fieldConstraints.put(fieldName, constraints);
    return new FilterConstraints<>(adapter, indexMapping, index, fieldConstraints);
  }

  /**
   * Create a set of empty filter constraints. Empty filter constraints result in a full table scan.
   * 
   * @param <V> the constraint class
   * @return a set of empty filter constraints
   */
  public static <V extends Comparable<V>> FilterConstraints<V> empty() {
    return new FilterConstraints<>(null, null, null, Maps.newHashMap());
  }

}
