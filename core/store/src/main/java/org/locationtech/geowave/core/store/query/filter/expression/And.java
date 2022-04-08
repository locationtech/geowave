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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import com.google.common.base.Predicates;
import com.google.common.collect.Sets;

/**
 * Combines multiple filters using the AND operator. The expression will only evaluate to true if
 * all child filters also resolve to true.
 */
public class And extends MultiFilterOperator {

  public And() {}

  public And(final Filter... children) {
    super(children);
  }

  @Override
  public boolean evaluate(final Map<String, Object> fieldValues) {
    return Arrays.stream(getChildren()).allMatch(f -> f.evaluate(fieldValues));
  }

  @Override
  public <T> boolean evaluate(final DataTypeAdapter<T> adapter, final T entry) {
    return Arrays.stream(getChildren()).allMatch(f -> f.evaluate(adapter, entry));
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
      finalConstraints.and(
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
  public Set<String> getConstrainableFields() {
    return Arrays.stream(getChildren()).map(Filter::getConstrainableFields).collect(
        () -> Sets.newHashSet(),
        Set::addAll,
        Set::addAll);
  }

  @Override
  public Filter removePredicatesForFields(Set<String> fields) {
    Filter[] updatedChildren =
        Arrays.stream(getChildren()).map(f -> f.removePredicatesForFields(fields)).filter(
            Predicates.notNull()).toArray(Filter[]::new);
    if (updatedChildren.length == 0) {
      return null;
    } else if (updatedChildren.length == 1) {
      return updatedChildren[0];
    }
    return new And(updatedChildren);
  }

  @Override
  public String toString() {
    return Arrays.stream(getChildren()).map(
        f -> f instanceof MultiFilterOperator ? "(" + f.toString() + ")" : f.toString()).collect(
            Collectors.joining(" AND "));
  }

}
