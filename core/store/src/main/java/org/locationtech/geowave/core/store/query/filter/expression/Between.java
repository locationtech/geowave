/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import com.google.common.collect.Sets;

/**
 * An abstract between implementation for any comparable object.
 *
 * @param <E> the expression class
 * @param <C> the comparable class
 */
public abstract class Between<E extends Expression<C>, C extends Comparable<C>> implements
    Predicate {

  protected E valueExpr;
  protected E lowerBoundExpr;
  protected E upperBoundExpr;

  public Between() {}

  /**
   * Construct a new Between instance with the given value, lower bound, and upper bound
   * expressions.
   * 
   * @param value the expression that represents the value to compare
   * @param lowerBound the expression that represents the lower bound
   * @param upperBound the expression that represents the upper bound
   */
  public Between(final E value, final E lowerBound, final E upperBound) {
    valueExpr = value;
    lowerBoundExpr = lowerBound;
    upperBoundExpr = upperBound;
  }

  public E getValue() {
    return valueExpr;
  }

  public E getLowerBound() {
    return lowerBoundExpr;
  }

  public E getUpperBound() {
    return upperBoundExpr;
  }

  @Override
  public void addReferencedFields(final Set<String> fields) {
    valueExpr.addReferencedFields(fields);
    lowerBoundExpr.addReferencedFields(fields);
    upperBoundExpr.addReferencedFields(fields);
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
        && indexSupported(index)) {
      final C lowerBound = lowerBoundExpr.evaluateValue(null, null);
      final C upperBound = upperBoundExpr.evaluateValue(null, null);
      if ((lowerBound != null)
          && (upperBound != null)
          && constraintClass.isAssignableFrom(lowerBound.getClass())
          && constraintClass.isAssignableFrom(upperBound.getClass())) {
        return FilterConstraints.of(
            adapter,
            indexMapping,
            index,
            ((FieldValue<?>) valueExpr).getFieldName(),
            (IndexFieldConstraints<V>) toConstraints(lowerBound, upperBound));
      }
    }
    return FilterConstraints.empty();
  }

  protected abstract boolean indexSupported(final Index index);

  protected abstract IndexFieldConstraints<C> toConstraints(final C lowerBound, final C upperBound);

  @Override
  public boolean evaluate(final Map<String, Object> fieldValues) {
    final C value = valueExpr.evaluateValue(fieldValues);
    final C lowerBound = lowerBoundExpr.evaluateValue(fieldValues);
    final C upperBound = upperBoundExpr.evaluateValue(fieldValues);
    if ((value == null) || (lowerBound == null) || (upperBound == null)) {
      return false;
    }
    return evaluateInternal(value, lowerBound, upperBound);
  }

  @Override
  public <T> boolean evaluate(final DataTypeAdapter<T> adapter, final T entry) {
    final C value = valueExpr.evaluateValue(adapter, entry);
    final C lowerBound = lowerBoundExpr.evaluateValue(adapter, entry);
    final C upperBound = upperBoundExpr.evaluateValue(adapter, entry);
    if ((value == null) || (lowerBound == null) || (upperBound == null)) {
      return false;
    }
    return evaluateInternal(value, lowerBound, upperBound);
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

  protected abstract boolean evaluateInternal(
      final C value,
      final C lowerBound,
      final C upperBound);

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

  @SuppressWarnings("unchecked")
  @Override
  public void fromBinary(final byte[] bytes) {
    final List<Persistable> expressions = PersistenceUtils.fromBinaryAsList(bytes);
    valueExpr = (E) expressions.get(0);
    lowerBoundExpr = (E) expressions.get(1);
    upperBoundExpr = (E) expressions.get(2);
  }

}
