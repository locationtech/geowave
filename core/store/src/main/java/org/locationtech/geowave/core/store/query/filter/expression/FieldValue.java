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
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

/**
 * An abstract expression implementation for turning fields values from an adapter entry into an
 * object to be used by the expression.
 *
 * @param <V> the class of the resolved field value
 */
public abstract class FieldValue<V> implements Expression<V> {

  protected String fieldName;

  public FieldValue() {}

  public FieldValue(final String fieldName) {
    this.fieldName = fieldName;
  }

  public String getFieldName() {
    return fieldName;
  }

  @Override
  public void addReferencedFields(final Set<String> fields) {
    fields.add(fieldName);
  }

  @Override
  public boolean isLiteral() {
    return false;
  }

  @Override
  public V evaluateValue(final Map<String, Object> fieldValues) {
    final Object value = fieldValues.get(fieldName);
    if (value == null) {
      return null;
    }
    return evaluateValueInternal(value);
  }

  @Override
  public <T> V evaluateValue(final DataTypeAdapter<T> adapter, final T entry) {
    final Object value = adapter.getFieldValue(entry, fieldName);
    if (value == null) {
      return null;
    }
    return evaluateValueInternal(value);
  }

  protected abstract V evaluateValueInternal(final Object value);

  @Override
  public String toString() {
    return fieldName;
  }

  @Override
  public byte[] toBinary() {
    return StringUtils.stringToBinary(fieldName);
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    fieldName = StringUtils.stringFromBinary(bytes);
  }

}
