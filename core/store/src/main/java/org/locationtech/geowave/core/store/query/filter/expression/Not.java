/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression;

import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;

/**
 * A filter that inverts the result of another filter.
 */
public class Not implements Filter {

  private Filter baseCondition;

  public Not() {}

  public Not(final Filter baseCondition) {
    this.baseCondition = baseCondition;
  }

  public Filter getFilter() {
    return baseCondition;
  }

  @Override
  public void prepare(
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    baseCondition.prepare(adapter, indexMapping, index);
  }

  @Override
  public void addReferencedFields(final Set<String> fields) {
    baseCondition.addReferencedFields(fields);
  }

  @Override
  public Set<String> getConstrainableFields() {
    return baseCondition.getConstrainableFields();
  }

  @Override
  public <V extends Comparable<V>> FilterConstraints<V> getConstraints(
      final Class<V> constraintClass,
      final DataStatisticsStore statsStore,
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index,
      final Set<String> indexedFields) {
    final FilterConstraints<V> constraints =
        baseCondition.getConstraints(
            constraintClass,
            statsStore,
            adapter,
            indexMapping,
            index,
            indexedFields);
    // TODO: There is room for improvement here in the future. To properly handle the constraints
    // for Not, all operators need to be inverted, not just the ranges. For example, if you
    // had A > 5 && B < 5, inverting just the ranges would result in a filter of A <= 5 && B >=5
    // which is incorrect, it should really be A <= 5 || B >=5 which becomes unconstrainable on
    // either A or B attribute indexes. On the other hand if the underlying filter was using ||
    // instead of &&, then the filter would become constrainable where it previously wasn't. For now
    // we can say that if only one field and one dimension are being constrained, inverting the
    // ranges produces an accurate constraint.
    if (constraints.getFieldCount() == 1) {
      constraints.invert();
      return constraints;
    }
    return FilterConstraints.empty();
  }

  @Override
  public boolean evaluate(final Map<String, Object> fieldValues) {
    return !baseCondition.evaluate(fieldValues);
  }

  @Override
  public <T> boolean evaluate(final DataTypeAdapter<T> adapter, final T entry) {
    return !baseCondition.evaluate(adapter, entry);
  }

  @Override
  public Filter removePredicatesForFields(Set<String> fields) {
    final Filter updated = baseCondition.removePredicatesForFields(fields);
    if (updated == null) {
      return null;
    }
    return new Not(updated);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("NOT(");
    sb.append(baseCondition.toString());
    sb.append(")");
    return sb.toString();
  }

  @Override
  public byte[] toBinary() {
    return PersistenceUtils.toBinary(baseCondition);
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    baseCondition = (Filter) PersistenceUtils.fromBinary(bytes);
  }

}
