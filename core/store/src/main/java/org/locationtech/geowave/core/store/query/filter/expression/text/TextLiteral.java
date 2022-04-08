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
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.query.filter.expression.Expression;
import org.locationtech.geowave.core.store.query.filter.expression.Literal;

/**
 * A text implementation of literal, representing text literal objects.
 */
public class TextLiteral extends Literal<String> implements TextExpression {

  public TextLiteral() {}

  public TextLiteral(final String literal) {
    super(literal);
  }

  public static TextLiteral of(Object literal) {
    if (literal == null) {
      return new TextLiteral(null);
    }
    if (literal instanceof TextLiteral) {
      return (TextLiteral) literal;
    }
    if (literal instanceof Expression && ((Expression<?>) literal).isLiteral()) {
      literal = ((Expression<?>) literal).evaluateValue(null);
    }
    return new TextLiteral(literal.toString());
  }

  @Override
  public String toString() {
    return literal == null ? "null" : "'" + literal + "'";
  }

  @Override
  public byte[] toBinary() {
    if (literal == null) {
      return new byte[] {(byte) 0};
    }
    final byte[] literalBytes = StringUtils.stringToBinary(literal);
    final ByteBuffer buffer = ByteBuffer.allocate(1 + literalBytes.length);
    buffer.put((byte) 1);
    buffer.put(literalBytes);
    return buffer.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    final byte nullByte = buffer.get();
    if (nullByte == 0) {
      literal = null;
      return;
    }
    final byte[] literalBytes = new byte[buffer.remaining()];
    buffer.get(literalBytes);
    literal = StringUtils.stringFromBinary(literalBytes);
  }

}
