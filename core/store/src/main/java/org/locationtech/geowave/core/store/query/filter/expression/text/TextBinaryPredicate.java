/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression.text;

import java.nio.ByteBuffer;
import java.util.Map;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.filter.expression.BinaryPredicate;

/**
 * Abstract class for comparing two text expressions.
 */
public abstract class TextBinaryPredicate extends BinaryPredicate<TextExpression> {

  protected boolean ignoreCase;

  public TextBinaryPredicate() {}

  public TextBinaryPredicate(final TextExpression expression1, final TextExpression expression2) {
    this(expression1, expression2, false);
  }

  public TextBinaryPredicate(
      final TextExpression expression1,
      final TextExpression expression2,
      final boolean ignoreCase) {
    super(expression1, expression2);
    this.ignoreCase = ignoreCase;
  }

  @Override
  public void prepare(
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    if (expression1.isLiteral() && !(expression1 instanceof TextLiteral)) {
      expression1 = TextLiteral.of(expression1.evaluateValue(null));
    }
    if (expression2.isLiteral() && !(expression2 instanceof TextLiteral)) {
      expression2 = TextLiteral.of(expression2.evaluateValue(null));
    }
  }

  @Override
  public boolean evaluate(final Map<String, Object> fieldValues) {
    final Object value1 = expression1.evaluateValue(fieldValues);
    final Object value2 = expression2.evaluateValue(fieldValues);
    return evaluateValues(value1, value2);
  }

  @Override
  public <T> boolean evaluate(final DataTypeAdapter<T> adapter, final T entry) {
    final Object value1 = expression1.evaluateValue(adapter, entry);
    final Object value2 = expression2.evaluateValue(adapter, entry);
    return evaluateValues(value1, value2);
  }

  private boolean evaluateValues(final Object value1, final Object value2) {
    if ((value1 == null) || (value2 == null)) {
      return false;
    }
    if (ignoreCase) {
      return evaluateInternal(value1.toString().toLowerCase(), value2.toString().toLowerCase());
    }
    return evaluateInternal(value1.toString(), value2.toString());
  }

  protected abstract boolean evaluateInternal(String value1, String value2);

  public boolean isIgnoreCase() {
    return ignoreCase;
  }

  @Override
  public byte[] toBinary() {
    final byte[] superBinary = super.toBinary();
    final ByteBuffer buffer = ByteBuffer.allocate(1 + superBinary.length);
    buffer.put(ignoreCase ? (byte) 1 : (byte) 0);
    buffer.put(superBinary);
    return buffer.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    ignoreCase = buffer.get() == (byte) 1;
    final byte[] superBinary = new byte[buffer.remaining()];
    buffer.get(superBinary);
    super.fromBinary(superBinary);
  }

}
