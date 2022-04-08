/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal;

import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.geotime.store.statistics.TimeRangeStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.TimeRangeStatistic.TimeRangeValue;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.index.simple.SimpleNumericIndexStrategy;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IndexFieldMapper;
import org.locationtech.geowave.core.store.query.filter.expression.BinaryPredicate;
import org.locationtech.geowave.core.store.query.filter.expression.FieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.FilterConstraints;
import org.locationtech.geowave.core.store.query.filter.expression.IndexFieldConstraints;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericFieldConstraints;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.InternalStatisticsHelper;
import org.threeten.extra.Interval;
import com.google.common.collect.Sets;

/**
 * Abstract class for comparing two temporal expressions.
 */
public abstract class BinaryTemporalPredicate extends BinaryPredicate<TemporalExpression> {

  public BinaryTemporalPredicate() {}

  public BinaryTemporalPredicate(
      final TemporalExpression expression1,
      final TemporalExpression expression2) {
    super(expression1, expression2);
  }

  @Override
  public void prepare(
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    if (expression1.isLiteral() && !(expression1 instanceof TemporalLiteral)) {
      expression1 = TemporalLiteral.of(expression1.evaluateValue(null));
    }
    if (expression2.isLiteral() && !(expression2 instanceof TemporalLiteral)) {
      expression2 = TemporalLiteral.of(expression2.evaluateValue(null));
    }
  }

  @Override
  public boolean evaluate(final Map<String, Object> fieldValues) {
    final Interval value1 = TimeUtils.getInterval(expression1.evaluateValue(fieldValues));
    final Interval value2 = TimeUtils.getInterval(expression2.evaluateValue(fieldValues));
    return evaluateInternal(value1, value2);
  }

  @Override
  public <T> boolean evaluate(final DataTypeAdapter<T> adapter, final T entry) {
    final Interval value1 = TimeUtils.getInterval(expression1.evaluateValue(adapter, entry));
    final Interval value2 = TimeUtils.getInterval(expression2.evaluateValue(adapter, entry));
    return evaluateInternal(value1, value2);
  }

  protected abstract boolean evaluateInternal(final Interval value1, final Interval value2);

  @Override
  public Set<String> getConstrainableFields() {
    if ((expression1 instanceof FieldValue) && expression2.isLiteral()) {
      return Sets.newHashSet(((FieldValue<?>) expression1).getFieldName());
    } else if ((expression2 instanceof FieldValue) && expression1.isLiteral()) {
      return Sets.newHashSet(((FieldValue<?>) expression2).getFieldName());
    }
    return Sets.newHashSet();
  }

  private boolean isPartOfRange(final String fieldName, final AdapterToIndexMapping indexMapping) {
    for (final IndexFieldMapper<?, ?> mapper : indexMapping.getIndexFieldMappers()) {
      final String[] adapterFields = mapper.getAdapterFields();
      for (int i = 0; i < adapterFields.length; i++) {
        if (adapterFields[i].equals(fieldName)) {
          return adapterFields.length > 1;
        }
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V extends Comparable<V>> FilterConstraints<V> getConstraints(
      final Class<V> constraintClass,
      final DataStatisticsStore statsStore,
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index,
      final Set<String> indexedFields) {
    if ((expression1 instanceof FieldValue)
        && indexedFields.contains(((FieldValue<?>) expression1).getFieldName())
        && expression2.isLiteral()
        && constraintClass.isAssignableFrom(Double.class)) {
      final Double minValue;
      final Double maxValue;
      if (index.getIndexStrategy() instanceof SimpleNumericIndexStrategy) {
        minValue = null;
        maxValue = null;
      } else {
        final TimeRangeValue timeRange =
            InternalStatisticsHelper.getFieldStatistic(
                statsStore,
                TimeRangeStatistic.STATS_TYPE,
                adapter.getTypeName(),
                ((FieldValue<?>) expression1).getFieldName());
        if (timeRange != null) {
          minValue = (double) timeRange.getMin();
          maxValue = (double) timeRange.getMax();
        } else {
          // We cannot determine the query range for the binned
          return FilterConstraints.empty();
        }
      }
      String fieldName = ((FieldValue<?>) expression1).getFieldName();
      final boolean partOfRange = isPartOfRange(fieldName, indexMapping);
      final Interval literal = expression2.evaluateValue(null, null);
      if (literal != null) {
        return FilterConstraints.of(
            adapter,
            indexMapping,
            index,
            fieldName,
            (IndexFieldConstraints<V>) getConstraints(
                literal,
                minValue,
                maxValue,
                false,
                !partOfRange && index.getIndexStrategy() instanceof SimpleNumericIndexStrategy));
      }
    } else if ((expression2 instanceof FieldValue)
        && indexedFields.contains(((FieldValue<?>) expression2).getFieldName())
        && expression1.isLiteral()
        && constraintClass.isAssignableFrom(Double.class)) {
      final Double minValue;
      final Double maxValue;
      if (index.getIndexStrategy() instanceof SimpleNumericIndexStrategy) {
        minValue = null;
        maxValue = null;
      } else {
        final TimeRangeValue timeRange =
            InternalStatisticsHelper.getFieldStatistic(
                statsStore,
                TimeRangeStatistic.STATS_TYPE,
                adapter.getTypeName(),
                ((FieldValue<?>) expression2).getFieldName());
        if (timeRange != null) {
          minValue = (double) timeRange.getMin();
          maxValue = (double) timeRange.getMax();
        } else {
          // We cannot determine the query range for the binned
          return FilterConstraints.empty();
        }
      }
      String fieldName = ((FieldValue<?>) expression2).getFieldName();
      final boolean partOfRange = isPartOfRange(fieldName, indexMapping);
      final Interval literal = expression1.evaluateValue(null, null);
      if (literal != null) {
        return FilterConstraints.of(
            adapter,
            indexMapping,
            index,
            fieldName,
            (IndexFieldConstraints<V>) getConstraints(
                literal,
                minValue,
                maxValue,
                true,
                !partOfRange && index.getIndexStrategy() instanceof SimpleNumericIndexStrategy));
      }
    }
    return FilterConstraints.empty();
  }

  protected abstract NumericFieldConstraints getConstraints(
      final Interval literal,
      final Double minRange,
      final Double maxRange,
      final boolean reversed,
      final boolean exact);

}
