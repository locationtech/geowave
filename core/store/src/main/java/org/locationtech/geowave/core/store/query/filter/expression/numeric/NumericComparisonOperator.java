/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression.numeric;

import java.util.List;
import org.locationtech.geowave.core.index.FloatCompareUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.CustomIndex;
import org.locationtech.geowave.core.store.query.filter.expression.ComparisonOperator;
import org.locationtech.geowave.core.store.query.filter.expression.FilterRange;
import org.locationtech.geowave.core.store.query.filter.expression.IndexFieldConstraints;

/**
 * Implementation of comparison operators for numeric data.
 */
public class NumericComparisonOperator extends ComparisonOperator<NumericExpression, Double> {

  public NumericComparisonOperator() {}

  public NumericComparisonOperator(
      final NumericExpression expression1,
      final NumericExpression expression2,
      final CompareOp compareOperator) {
    super(expression1, expression2, compareOperator);
  }

  @Override
  public void prepare(
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    if (expression1.isLiteral() && !(expression1 instanceof NumericLiteral)) {
      expression1 = NumericLiteral.of(expression1.evaluateValue(null));
    }
    if (expression2.isLiteral() && !(expression2 instanceof NumericLiteral)) {
      expression2 = NumericLiteral.of(expression2.evaluateValue(null));
    }
  }

  @Override
  protected boolean equalTo(final Double value1, final Double value2) {
    return FloatCompareUtils.checkDoublesEqual(value1, value2);
  }

  @Override
  protected boolean notEqualTo(final Double value1, final Double value2) {
    return !FloatCompareUtils.checkDoublesEqual(value1, value2);
  }

  @Override
  protected boolean lessThan(final Double value1, final Double value2) {
    return value1 < value2;
  }

  @Override
  protected boolean lessThanOrEqual(final Double value1, final Double value2) {
    return value1 <= value2;
  }

  @Override
  protected boolean greaterThan(final Double value1, final Double value2) {
    return value1 > value2;
  }

  @Override
  protected boolean greaterThanOrEqual(final Double value1, final Double value2) {
    return value1 >= value2;
  }

  @Override
  protected boolean indexSupported(final Index index) {
    return !(index instanceof CustomIndex);
  }


  @Override
  protected IndexFieldConstraints<Double> toFieldConstraints(
      final List<FilterRange<Double>> ranges) {
    return NumericFieldConstraints.of(ranges);
  }
}
