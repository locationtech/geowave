/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial;

import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.filter.expression.FilterConstraints;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;

/**
 * Predicate that passes when the first operand is not topologically equal to the second operand.
 */
public class SpatialNotEqualTo extends BinarySpatialPredicate {

  public SpatialNotEqualTo() {}

  public SpatialNotEqualTo(final SpatialExpression expr1, final SpatialExpression expr2) {
    super(expr1, expr2);
  }

  @Override
  public boolean evaluate(final Map<String, Object> fieldValues) {
    final Object value1 = getExpression1().evaluateValue(fieldValues);
    final Object value2 = getExpression2().evaluateValue(fieldValues);
    if (value1 == null) {
      return value2 != null;
    }
    if (value2 == null) {
      return true;
    }
    return evaluateInternal((FilterGeometry) value1, (FilterGeometry) value2);
  }

  @Override
  public <T> boolean evaluate(final DataTypeAdapter<T> adapter, final T entry) {
    final Object value1 = getExpression1().evaluateValue(adapter, entry);
    final Object value2 = getExpression2().evaluateValue(adapter, entry);
    if (value1 == null) {
      return value2 != null;
    }
    if (value2 == null) {
      return true;
    }
    return evaluateInternal((FilterGeometry) value1, (FilterGeometry) value2);
  }

  public <V extends Comparable<V>> FilterConstraints<V> getConstraints(
      final Class<V> constraintClass,
      final DataStatisticsStore statsStore,
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index,
      final Set<String> indexedFields) {
    return FilterConstraints.empty();
  }

  @Override
  protected boolean isExact() {
    return false;
  }

  @Override
  protected boolean evaluateInternal(final FilterGeometry value1, final FilterGeometry value2) {
    return !value1.isEqualTo(value2);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("NOT_EQUALS(");
    sb.append(expression1.toString());
    sb.append(",");
    sb.append(expression2.toString());
    sb.append(")");
    return sb.toString();
  }

}
