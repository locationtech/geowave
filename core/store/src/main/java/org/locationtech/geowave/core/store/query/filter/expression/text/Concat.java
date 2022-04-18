/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression.text;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

/**
 * An expression that concatenates two text expressions into a single text expression.
 */
public class Concat implements TextExpression {

  private TextExpression expression1;
  private TextExpression expression2;

  public Concat() {}

  public Concat(final TextExpression expr1, final TextExpression expr2) {
    expression1 = expr1;
    expression2 = expr2;
  }

  public TextExpression getExpression1() {
    return expression1;
  }

  public TextExpression getExpression2() {
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
  public String evaluateValue(final Map<String, Object> fieldValues) {
    final String value1 = expression1.evaluateValue(fieldValues);
    final String value2 = expression2.evaluateValue(fieldValues);
    if (value1 == null) {
      return value2;
    }
    if (value2 == null) {
      return value1;
    }
    return value1.concat(value2);
  }

  @Override
  public <T> String evaluateValue(final DataTypeAdapter<T> adapter, final T entry) {
    final String value1 = expression1.evaluateValue(adapter, entry);
    final String value2 = expression2.evaluateValue(adapter, entry);
    if (value1 == null) {
      return value2;
    }
    if (value2 == null) {
      return value1;
    }
    return value1.concat(value2);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("concat(");
    sb.append(expression1.toString());
    sb.append(",");
    sb.append(expression2.toString());
    sb.append(")");
    return sb.toString();
  }

  @Override
  public byte[] toBinary() {
    return PersistenceUtils.toBinary(new Persistable[] {expression1, expression2});
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final List<Persistable> expressions = PersistenceUtils.fromBinaryAsList(bytes);
    expression1 = (TextExpression) expressions.get(0);
    expression2 = (TextExpression) expressions.get(1);
  }

}
