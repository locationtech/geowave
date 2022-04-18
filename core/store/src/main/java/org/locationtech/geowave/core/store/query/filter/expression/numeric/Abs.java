/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression.numeric;

import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

/**
 * An expression that takes the absolute value of the evaluated value of another numeric expression.
 */
public class Abs implements NumericExpression {

  private NumericExpression baseExpression;

  public Abs() {}

  public Abs(final NumericExpression baseExpression) {
    this.baseExpression = baseExpression;
  }

  public NumericExpression getExpression() {
    return baseExpression;
  }

  @Override
  public void addReferencedFields(final Set<String> fields) {
    baseExpression.addReferencedFields(fields);
  }

  @Override
  public boolean isLiteral() {
    return baseExpression.isLiteral();
  }

  @Override
  public Double evaluateValue(final Map<String, Object> fieldValues) {
    final Double value = baseExpression.evaluateValue(fieldValues);
    if (value == null) {
      return null;
    }
    return Math.abs(value);
  }

  @Override
  public <T> Double evaluateValue(final DataTypeAdapter<T> adapter, final T entry) {
    final Double value = baseExpression.evaluateValue(adapter, entry);
    if (value == null) {
      return null;
    }
    return Math.abs(value);
  }

  @Override
  public String toString() {
    return "abs(" + baseExpression.toString() + ")";
  }

  @Override
  public byte[] toBinary() {
    return PersistenceUtils.toBinary(baseExpression);
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    baseExpression = (NumericExpression) PersistenceUtils.fromBinary(bytes);
  }

}
