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
 * A field value implementation for interpreting any adapter field as a boolean. Non-boolean field
 * values will evaluate to {@code true} if they are non-null.
 */
public class BooleanFieldValue extends FieldValue<Object> implements BooleanExpression {

  public BooleanFieldValue() {}

  public BooleanFieldValue(final String fieldName) {
    super(fieldName);
  }

  @Override
  public void addReferencedFields(final Set<String> fields) {
    fields.add(fieldName);
  }

  @Override
  public Boolean evaluateValue(final Map<String, Object> fieldValues) {
    return BooleanExpression.evaluateObject(fieldValues.get(fieldName));
  }

  @Override
  public <T> Boolean evaluateValue(final DataTypeAdapter<T> adapter, final T entry) {
    return BooleanExpression.evaluateObject(adapter.getFieldValue(entry, fieldName));
  }

  @Override
  protected Object evaluateValueInternal(final Object value) {
    return BooleanExpression.evaluateObject(value);
  }

  public static BooleanFieldValue of(final String fieldName) {
    return new BooleanFieldValue(fieldName);
  }

  @Override
  public void prepare(
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {}

  @Override
  public Set<String> getConstrainableFields() {
    return Sets.newHashSet(fieldName);
  }

  @Override
  public Filter removePredicatesForFields(Set<String> fields) {
    if (fields.contains(fieldName)) {
      return null;
    }
    return this;
  }

}
