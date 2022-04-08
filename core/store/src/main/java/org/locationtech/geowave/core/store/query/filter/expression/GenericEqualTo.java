/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression;

import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import com.google.common.collect.Sets;

/**
 * A generic predicate to compare two expressions using {@code Object.equals}.
 */
public class GenericEqualTo extends BinaryPredicate<Expression<? extends Object>> {

  public GenericEqualTo() {}

  public GenericEqualTo(
      final Expression<? extends Object> expression1,
      final Expression<? extends Object> expression2) {
    super(expression1, expression2);
  }

  @Override
  public boolean evaluate(final Map<String, Object> fieldValues) {
    final Object value1 = expression1.evaluateValue(fieldValues);
    final Object value2 = expression2.evaluateValue(fieldValues);
    if (value1 == null) {
      return value2 == null;
    }
    if (value2 == null) {
      return false;
    }
    return value1.equals(value2);
  }

  @Override
  public <T> boolean evaluate(final DataTypeAdapter<T> adapter, final T entry) {
    final Object value1 = expression1.evaluateValue(adapter, entry);
    final Object value2 = expression2.evaluateValue(adapter, entry);
    if (value1 == null) {
      return value2 == null;
    }
    if (value2 == null) {
      return false;
    }
    return value1.equals(value2);
  }

  @Override
  public Set<String> getConstrainableFields() {
    return Sets.newHashSet();
  }

  @Override
  public void prepare(
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {}

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(expression1.toString());
    sb.append(" = ");
    sb.append(expression2.toString());
    return sb.toString();
  }

}
