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
import java.util.List;
import org.locationtech.geowave.core.index.text.CaseSensitivity;
import org.locationtech.geowave.core.index.text.TextIndexStrategy;
import org.locationtech.geowave.core.index.text.TextSearchType;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.CustomIndex;
import org.locationtech.geowave.core.store.query.filter.expression.ComparisonOperator;
import org.locationtech.geowave.core.store.query.filter.expression.FilterRange;
import org.locationtech.geowave.core.store.query.filter.expression.IndexFieldConstraints;

/**
 * Implementation of comparison operators for text data.
 */
public class TextComparisonOperator extends ComparisonOperator<TextExpression, String> {

  private boolean ignoreCase;

  public TextComparisonOperator() {}

  public TextComparisonOperator(
      final TextExpression expression1,
      final TextExpression expression2,
      final CompareOp compareOperator) {
    this(expression1, expression2, compareOperator, false);
  }

  public TextComparisonOperator(
      final TextExpression expression1,
      final TextExpression expression2,
      final CompareOp compareOperator,
      final boolean ignoreCase) {
    super(expression1, expression2, compareOperator);
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
  public boolean isExact() {
    // TODO: This should really be dependent on the index strategy, but for now, the text index
    // strategy will only be exact if the operator is >= or < due to the way the prefix range scans
    // work
    switch (compareOperator) {
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected boolean equalTo(final String value1, final String value2) {
    if (ignoreCase) {
      return value1.equalsIgnoreCase(value2);
    }
    return value1.equals(value2);
  }

  @Override
  protected boolean notEqualTo(final String value1, final String value2) {
    if (ignoreCase) {
      return !value1.equalsIgnoreCase(value2);
    }
    return !value1.equals(value2);
  }

  @Override
  protected boolean lessThan(final String value1, final String value2) {
    if (ignoreCase) {
      return value1.toLowerCase().compareTo(value2.toLowerCase()) < 0;
    }
    return value1.compareTo(value2) < 0;
  }

  @Override
  protected boolean lessThanOrEqual(final String value1, final String value2) {
    if (ignoreCase) {
      return value1.toLowerCase().compareTo(value2.toLowerCase()) <= 0;
    }
    return value1.compareTo(value2) <= 0;
  }

  @Override
  protected boolean greaterThan(final String value1, final String value2) {
    if (ignoreCase) {
      return value1.toLowerCase().compareTo(value2.toLowerCase()) > 0;
    }
    return value1.compareTo(value2) > 0;
  }

  @Override
  protected boolean greaterThanOrEqual(final String value1, final String value2) {
    if (ignoreCase) {
      return value1.toLowerCase().compareTo(value2.toLowerCase()) >= 0;
    }
    return value1.compareTo(value2) >= 0;
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
  protected IndexFieldConstraints<String> toFieldConstraints(
      final List<FilterRange<String>> ranges) {
    return TextFieldConstraints.of(ranges);
  }

  @Override
  protected FilterRange<String> toFilterRange(
      final String start,
      final String end,
      boolean startInclusive,
      final boolean endInclusive) {
    // Entries with the same prefix may be greater than the prefix or not equal to it, so these
    // operators need to include those prefixes in the scan
    switch (compareOperator) {
      case GREATER_THAN:
      case NOT_EQUAL_TO:
        startInclusive = true;
        break;
      default:
        break;

    }
    return TextFilterRange.of(
        start,
        end,
        startInclusive,
        endInclusive,
        isExact(),
        !ignoreCase,
        false);
  }

}
