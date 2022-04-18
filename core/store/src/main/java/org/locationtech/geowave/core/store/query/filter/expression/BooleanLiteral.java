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
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import com.google.common.collect.Sets;

/**
 * A literal implementation that evaluates to either {@code true} or {@code false}.
 */
public class BooleanLiteral extends Literal<Object> implements BooleanExpression, Predicate {

  public BooleanLiteral() {}

  public BooleanLiteral(final Object literal) {
    super(literal);
  }

  @Override
  public void prepare(
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {}

  @Override
  public void addReferencedFields(final Set<String> fields) {}

  @Override
  public Boolean evaluateValue(final Map<String, Object> fieldValues) {
    return BooleanExpression.evaluateObject(literal);
  }

  @Override
  public <T> Boolean evaluateValue(final DataTypeAdapter<T> adapter, final T entry) {
    return BooleanExpression.evaluateObject(literal);
  }

  @Override
  public Filter removePredicatesForFields(Set<String> fields) {
    return this;
  }

  @Override
  public Set<String> getConstrainableFields() {
    return Sets.newHashSet();
  }

  @Override
  public String toString() {
    return BooleanExpression.evaluateObject(literal) ? "TRUE" : "FALSE";
  }

  public static BooleanLiteral of(final Object object) {
    return new BooleanLiteral(object);
  }

}
