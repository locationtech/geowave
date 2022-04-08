/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression.numeric;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

/**
 * Abstract implementation for performing math operations on two numeric expressions.
 */
public abstract class MathExpression implements NumericExpression {

  private NumericExpression expression1;
  private NumericExpression expression2;

  public MathExpression() {}

  public MathExpression(final NumericExpression expr1, final NumericExpression expr2) {
    expression1 = expr1;
    expression2 = expr2;
  }

  public NumericExpression getExpression1() {
    return expression1;
  }

  public NumericExpression getExpression2() {
    return expression2;
  }

  @Override
  public void addReferencedFields(final Set<String> fields) {
    expression1.addReferencedFields(fields);
    expression2.addReferencedFields(fields);
  }

  @Override
  public boolean isLiteral() {
    return expression1.isLiteral() && expression2.isLiteral();
  }

  @Override
  public Double evaluateValue(final Map<String, Object> fieldValues) {
    final Double value1 = expression1.evaluateValue(fieldValues);
    final Double value2 = expression2.evaluateValue(fieldValues);
    if ((value1 == null) || (value2 == null)) {
      return null;
    }
    return doOperation(value1, value2);
  }

  @Override
  public <T> Double evaluateValue(final DataTypeAdapter<T> adapter, final T entry) {
    final Double value1 = expression1.evaluateValue(adapter, entry);
    final Double value2 = expression2.evaluateValue(adapter, entry);
    if ((value1 == null) || (value2 == null)) {
      return null;
    }
    return doOperation(value1, value2);
  }

  protected abstract double doOperation(final double value1, final double value2);

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    if (expression1 instanceof MathExpression) {
      sb.append("(");
      sb.append(expression1.toString());
      sb.append(")");
    } else {
      sb.append(expression1.toString());
    }
    sb.append(" ");
    sb.append(getOperatorString());
    sb.append(" ");
    if (expression2 instanceof MathExpression) {
      sb.append("(");
      sb.append(expression2.toString());
      sb.append(")");
    } else {
      sb.append(expression2.toString());
    }
    return sb.toString();
  }

  protected abstract String getOperatorString();

  @Override
  public byte[] toBinary() {
    return PersistenceUtils.toBinary(new Persistable[] {expression1, expression2});
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final List<Persistable> expressions = PersistenceUtils.fromBinaryAsList(bytes);
    expression1 = (NumericExpression) expressions.get(0);
    expression2 = (NumericExpression) expressions.get(1);
  }
}
