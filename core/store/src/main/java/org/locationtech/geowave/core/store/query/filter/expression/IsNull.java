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
import com.beust.jcommander.internal.Sets;

/**
 * Predicate that passes when the underlying expression evaluates to {@code null}.
 */
public class IsNull implements Predicate {

  private Expression<?> expression;

  public IsNull() {}

  public IsNull(final Expression<?> expression) {
    this.expression = expression;
  }

  public Expression<?> getExpression() {
    return expression;
  }

  @Override
  public void prepare(
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {}

  @Override
  public void addReferencedFields(final Set<String> fields) {
    expression.addReferencedFields(fields);
  }

  @Override
  public boolean evaluate(final Map<String, Object> fieldValues) {
    return expression.evaluateValue(fieldValues) == null;
  }

  @Override
  public <T> boolean evaluate(final DataTypeAdapter<T> adapter, final T entry) {
    return expression.evaluateValue(adapter, entry) == null;
  }

  @Override
  public Filter removePredicatesForFields(Set<String> fields) {
    final Set<String> referencedFields = Sets.newHashSet();
    expression.addReferencedFields(referencedFields);
    if (fields.containsAll(referencedFields)) {
      return null;
    }
    return this;
  }

  @Override
  public Set<String> getConstrainableFields() {
    return Sets.newHashSet();
  }

  @Override
  public String toString() {
    return expression.toString() + " IS NULL";
  }

  @Override
  public byte[] toBinary() {
    return PersistenceUtils.toBinary(expression);
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    expression = (Expression<?>) PersistenceUtils.fromBinary(bytes);
  }

}
