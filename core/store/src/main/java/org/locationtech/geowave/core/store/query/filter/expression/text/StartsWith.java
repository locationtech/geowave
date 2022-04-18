/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression.text;

import java.util.Set;
import org.locationtech.geowave.core.index.text.CaseSensitivity;
import org.locationtech.geowave.core.index.text.TextIndexStrategy;
import org.locationtech.geowave.core.index.text.TextSearchType;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.CustomIndex;
import org.locationtech.geowave.core.store.query.filter.expression.FieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.FilterConstraints;
import org.locationtech.geowave.core.store.query.filter.expression.IndexFieldConstraints;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import com.google.common.collect.Sets;

/**
 * Predicate that passes when the first operand starts with the second operand.
 */
public class StartsWith extends TextBinaryPredicate {

  public StartsWith() {}

  public StartsWith(final TextExpression expression1, final TextExpression expression2) {
    super(expression1, expression2);
  }

  public StartsWith(
      final TextExpression expression1,
      final TextExpression expression2,
      final boolean ignoreCase) {
    super(expression1, expression2, ignoreCase);
  }

  @Override
  public boolean evaluateInternal(final String value1, final String value2) {
    return value1.startsWith(value2);
  }

  @Override
  public Set<String> getConstrainableFields() {
    if ((expression1 instanceof FieldValue) && expression2.isLiteral()) {
      return Sets.newHashSet(((FieldValue<?>) expression1).getFieldName());
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
    if ((expression1 instanceof FieldValue)
        && indexedFields.contains(((FieldValue<?>) expression1).getFieldName())
        && expression2.isLiteral()
        && (index instanceof CustomIndex)
        && (((CustomIndex<?, ?>) index).getCustomIndexStrategy() instanceof TextIndexStrategy)
        && constraintClass.isAssignableFrom(String.class)) {
      final TextIndexStrategy<?> indexStrategy =
          (TextIndexStrategy<?>) ((CustomIndex<?, ?>) index).getCustomIndexStrategy();
      final String value = expression2.evaluateValue(null, null);
      if (value != null) {
        if ((ignoreCase
            && indexStrategy.isSupported(CaseSensitivity.CASE_INSENSITIVE)
            && indexStrategy.isSupported(TextSearchType.BEGINS_WITH))
            || (!ignoreCase
                && indexStrategy.isSupported(CaseSensitivity.CASE_SENSITIVE)
                && indexStrategy.isSupported(TextSearchType.BEGINS_WITH))) {
          return FilterConstraints.of(
              adapter,
              indexMapping,
              index,
              ((FieldValue<?>) expression1).getFieldName(),
              (IndexFieldConstraints<V>) TextFieldConstraints.of(
                  value,
                  value,
                  true,
                  true,
                  true,
                  !ignoreCase,
                  false));
        }
      }
    }
    return FilterConstraints.empty();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(expression1.toString());
    sb.append(" STARTS WITH ");
    sb.append(expression2.toString());
    return sb.toString();
  }

}
