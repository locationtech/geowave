/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression.text;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.text.CaseSensitivity;
import org.locationtech.geowave.core.index.text.TextIndexStrategy;
import org.locationtech.geowave.core.index.text.TextSearchType;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.CustomIndex;
import org.locationtech.geowave.core.store.query.filter.expression.Between;
import org.locationtech.geowave.core.store.query.filter.expression.IndexFieldConstraints;

/**
 * Implementation of between for text data.
 */
public class TextBetween extends Between<TextExpression, String> {

  private boolean ignoreCase;

  public TextBetween() {}

  public TextBetween(
      final TextExpression valueExpr,
      final TextExpression lowerBoundExpr,
      final TextExpression upperBoundExpr) {
    this(valueExpr, lowerBoundExpr, upperBoundExpr, false);
  }

  public TextBetween(
      final TextExpression valueExpr,
      final TextExpression lowerBoundExpr,
      final TextExpression upperBoundExpr,
      final boolean ignoreCase) {
    super(valueExpr, lowerBoundExpr, upperBoundExpr);
    this.ignoreCase = ignoreCase;
  }

  @Override
  protected boolean indexSupported(final Index index) {
    if ((index instanceof CustomIndex)
        && (((CustomIndex<?, ?>) index).getCustomIndexStrategy() instanceof TextIndexStrategy)) {
      final TextIndexStrategy<?> indexStrategy =
          (TextIndexStrategy<?>) ((CustomIndex<?, ?>) index).getCustomIndexStrategy();
      return (indexStrategy.isSupported(TextSearchType.BEGINS_WITH)
          && indexStrategy.isSupported(
              ignoreCase ? CaseSensitivity.CASE_INSENSITIVE : CaseSensitivity.CASE_SENSITIVE));
    }
    return false;
  }

  @Override
  public void prepare(
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    if (valueExpr.isLiteral() && !(valueExpr instanceof TextLiteral)) {
      valueExpr = TextLiteral.of(valueExpr.evaluateValue(null));
    }
    if (lowerBoundExpr.isLiteral() && !(lowerBoundExpr instanceof TextLiteral)) {
      lowerBoundExpr = TextLiteral.of(lowerBoundExpr.evaluateValue(null));
    }
    if (upperBoundExpr.isLiteral() && !(upperBoundExpr instanceof TextLiteral)) {
      upperBoundExpr = TextLiteral.of(upperBoundExpr.evaluateValue(null));
    }
  }

  @Override
  protected boolean evaluateInternal(
      final String value,
      final String lowerBound,
      final String upperBound) {
    if (ignoreCase) {
      final String valueLower = value.toLowerCase();
      return (valueLower.compareTo(lowerBound.toLowerCase()) >= 0)
          && (valueLower.compareTo(upperBound.toLowerCase()) <= 0);
    }
    return (value.compareTo(lowerBound) >= 0) && (value.compareTo(upperBound) <= 0);
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

  @Override
  protected IndexFieldConstraints<String> toConstraints(
      final String lowerBound,
      final String upperBound) {
    // It's not exact because strings with the upper bound prefix may be greater than the upper
    // bound
    return TextFieldConstraints.of(lowerBound, upperBound, true, true, false, !ignoreCase, false);
  }

}
