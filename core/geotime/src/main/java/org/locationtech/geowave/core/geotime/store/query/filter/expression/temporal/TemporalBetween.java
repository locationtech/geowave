/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.index.simple.SimpleNumericIndexStrategy;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.filter.expression.FieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.Filter;
import org.locationtech.geowave.core.store.query.filter.expression.FilterConstraints;
import org.locationtech.geowave.core.store.query.filter.expression.IndexFieldConstraints;
import org.locationtech.geowave.core.store.query.filter.expression.Predicate;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericFieldConstraints;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.threeten.extra.Interval;
import com.google.common.collect.Sets;

/**
 * Predicate that passes when the first operand is between the provided lower and upper bound
 * operands. If the lower bound is a time range, the start value of the bound is used. If the upper
 * bound is a time range, the end value of the bound is used.
 */
public class TemporalBetween implements Predicate {
  private TemporalExpression valueExpr;
  private TemporalExpression lowerBoundExpr;
  private TemporalExpression upperBoundExpr;

  public TemporalBetween() {}

  public TemporalBetween(
      final TemporalExpression value,
      final TemporalExpression lowerBound,
      final TemporalExpression upperBound) {
    valueExpr = value;
    lowerBoundExpr = lowerBound;
    upperBoundExpr = upperBound;
  }

  public TemporalExpression getValue() {
    return valueExpr;
  }

  public TemporalExpression getLowerBound() {
    return lowerBoundExpr;
  }

  public TemporalExpression getUpperBound() {
    return upperBoundExpr;
  }

  @Override
  public void prepare(
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    if (valueExpr.isLiteral() && !(valueExpr instanceof TemporalLiteral)) {
      valueExpr = TemporalLiteral.of(valueExpr.evaluateValue(null));
    }
    if (lowerBoundExpr.isLiteral() && !(lowerBoundExpr instanceof TemporalLiteral)) {
      lowerBoundExpr = TemporalLiteral.of(lowerBoundExpr.evaluateValue(null));
    }
    if (upperBoundExpr.isLiteral() && !(upperBoundExpr instanceof TemporalLiteral)) {
      upperBoundExpr = TemporalLiteral.of(upperBoundExpr.evaluateValue(null));
    }
  }

  @Override
  public void addReferencedFields(final Set<String> fields) {
    if (valueExpr instanceof FieldValue) {
      fields.add(((FieldValue<?>) valueExpr).getFieldName());
    }
    if (lowerBoundExpr instanceof FieldValue) {
      fields.add(((FieldValue<?>) lowerBoundExpr).getFieldName());
    }
    if (upperBoundExpr instanceof FieldValue) {
      fields.add(((FieldValue<?>) upperBoundExpr).getFieldName());
    }
  }

  @Override
  public boolean evaluate(final Map<String, Object> fieldValues) {
    final Interval value = TimeUtils.getInterval(valueExpr.evaluateValue(fieldValues));
    final Interval lowerBound = TimeUtils.getInterval(lowerBoundExpr.evaluateValue(fieldValues));
    final Interval upperBound = TimeUtils.getInterval(upperBoundExpr.evaluateValue(fieldValues));
    return evaluate(value, lowerBound, upperBound);
  }

  @Override
  public <T> boolean evaluate(final DataTypeAdapter<T> adapter, final T entry) {
    final Interval value = TimeUtils.getInterval(valueExpr.evaluateValue(adapter, entry));
    final Interval lowerBound = TimeUtils.getInterval(lowerBoundExpr.evaluateValue(adapter, entry));
    final Interval upperBound = TimeUtils.getInterval(upperBoundExpr.evaluateValue(adapter, entry));
    return evaluate(value, lowerBound, upperBound);
  }

  private boolean evaluate(
      final Interval value,
      final Interval lowerBound,
      final Interval upperBound) {
    if ((value == null) || (lowerBound == null) || (upperBound == null)) {
      return false;
    }
    return value.getStart().compareTo(lowerBound.getStart()) >= 0
        && TimeUtils.getIntervalEnd(value).compareTo(TimeUtils.getIntervalEnd(upperBound)) <= 0;
  }

  @Override
  public Filter removePredicatesForFields(Set<String> fields) {
    final Set<String> referencedFields = Sets.newHashSet();
    valueExpr.addReferencedFields(referencedFields);
    lowerBoundExpr.addReferencedFields(referencedFields);
    upperBoundExpr.addReferencedFields(referencedFields);
    if (fields.containsAll(referencedFields)) {
      return null;
    }
    return this;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(valueExpr.toString());
    sb.append(" BETWEEN ");
    sb.append(lowerBoundExpr.toString());
    sb.append(" AND ");
    sb.append(upperBoundExpr.toString());
    return sb.toString();
  }

  @Override
  public byte[] toBinary() {
    return PersistenceUtils.toBinary(new Persistable[] {valueExpr, lowerBoundExpr, upperBoundExpr});
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final List<Persistable> expressions = PersistenceUtils.fromBinaryAsList(bytes);
    valueExpr = (TemporalExpression) expressions.get(0);
    lowerBoundExpr = (TemporalExpression) expressions.get(1);
    upperBoundExpr = (TemporalExpression) expressions.get(2);
  }

  @Override
  public Set<String> getConstrainableFields() {
    if ((valueExpr instanceof FieldValue)
        && lowerBoundExpr.isLiteral()
        && upperBoundExpr.isLiteral()) {
      return Sets.newHashSet(((FieldValue<?>) valueExpr).getFieldName());
    }
    return Sets.newHashSet();
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
    if ((valueExpr instanceof FieldValue)
        && indexedFields.contains(((FieldValue<?>) valueExpr).getFieldName())
        && lowerBoundExpr.isLiteral()
        && upperBoundExpr.isLiteral()
        && constraintClass.isAssignableFrom(Double.class)) {
      final Interval lowerBound = lowerBoundExpr.evaluateValue(null, null);
      final Interval upperBound = upperBoundExpr.evaluateValue(null, null);
      if ((lowerBound != null) && (upperBound != null)) {
        return FilterConstraints.of(
            adapter,
            indexMapping,
            index,
            ((FieldValue<?>) valueExpr).getFieldName(),
            (IndexFieldConstraints<V>) NumericFieldConstraints.of(
                (double) lowerBound.getStart().toEpochMilli(),
                (double) TimeUtils.getIntervalEnd(upperBound).toEpochMilli(),
                true,
                false,
                index.getIndexStrategy() instanceof SimpleNumericIndexStrategy));
      }
    }
    return FilterConstraints.empty();
  }
}
