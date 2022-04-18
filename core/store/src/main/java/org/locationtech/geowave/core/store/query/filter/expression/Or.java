/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import com.google.common.collect.Sets;
import com.google.common.base.Predicates;

/**
 * Combines multiple filters using the OR operator. The expression will evaluate to true if ANY of
 * the child filters resolve to true.
 */
public class Or extends MultiFilterOperator {

  public Or() {}

  public Or(final Filter... filters) {
    super(filters);
  }

  @Override
  public <V extends Comparable<V>> FilterConstraints<V> getConstraints(
      final Class<V> constraintClass,
      final DataStatisticsStore statsStore,
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index,
      final Set<String> indexedFields) {
    final Filter[] children = getChildren();
    if (children.length == 0) {
      return FilterConstraints.empty();
    }
    final FilterConstraints<V> finalConstraints =
        children[0].getConstraints(
            constraintClass,
            statsStore,
            adapter,
            indexMapping,
            index,
            indexedFields);
    for (int i = 1; i < children.length; i++) {
      finalConstraints.or(
          children[i].getConstraints(
              constraintClass,
              statsStore,
              adapter,
              indexMapping,
              index,
              indexedFields));
    }
    return finalConstraints;
  }

  @Override
  public boolean evaluate(final Map<String, Object> fieldValues) {
    return Arrays.stream(getChildren()).anyMatch(f -> f.evaluate(fieldValues));
  }

  @Override
  public <T> boolean evaluate(final DataTypeAdapter<T> adapter, final T entry) {
    return Arrays.stream(getChildren()).anyMatch(f -> f.evaluate(adapter, entry));
  }

  @Override
  public Set<String> getConstrainableFields() {
    final Filter[] children = getChildren();
    Set<String> constrainableFields = null;
    for (final Filter filter : children) {
      if (constrainableFields == null) {
        constrainableFields = filter.getConstrainableFields();
      } else {
        constrainableFields.retainAll(filter.getConstrainableFields());
      }
    }
    if (constrainableFields == null) {
      return Sets.newHashSet();
    }
    return constrainableFields;
  }

  @Override
  public Filter removePredicatesForFields(Set<String> fields) {
    // We can only remove predicates for fields that are on both sides of the
    final Set<String> removableFields =
        Arrays.stream(getChildren()).map(Filter::getConstrainableFields).collect(
            () -> new HashSet<>(fields),
            Set::retainAll,
            Set::retainAll);
    if (removableFields.size() == 0) {
      return this;
    }
    Filter[] updatedChildren =
        Arrays.stream(getChildren()).map(f -> f.removePredicatesForFields(removableFields)).filter(
            Predicates.notNull()).toArray(Filter[]::new);
    if (updatedChildren.length == 0) {
      return null;
    } else if (updatedChildren.length == 1) {
      return updatedChildren[0];
    }
    return new Or(updatedChildren);
  }

  @Override
  public String toString() {
    return Arrays.stream(getChildren()).map(
        f -> f instanceof MultiFilterOperator ? "(" + f.toString() + ")" : f.toString()).collect(
            Collectors.joining(" OR "));
  }

}
