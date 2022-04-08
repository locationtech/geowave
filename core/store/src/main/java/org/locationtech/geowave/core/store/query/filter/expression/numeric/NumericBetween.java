/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression.numeric;

import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.CustomIndex;
import org.locationtech.geowave.core.store.query.filter.expression.Between;
import org.locationtech.geowave.core.store.query.filter.expression.IndexFieldConstraints;

/**
 * Implementation of between for numeric data.
 */
public class NumericBetween extends Between<NumericExpression, Double> {

  public NumericBetween() {}

  public NumericBetween(
      final NumericExpression valueExpr,
      final NumericExpression lowerBoundExpr,
      final NumericExpression upperBoundExpr) {
    super(valueExpr, lowerBoundExpr, upperBoundExpr);
  }

  @Override
  protected boolean evaluateInternal(
      final Double value,
      final Double lowerBound,
      final Double upperBound) {
    return (value >= lowerBound) && (value <= upperBound);
  }

  @Override
  protected IndexFieldConstraints<Double> toConstraints(
      final Double lowerBound,
      final Double upperBound) {
    return NumericFieldConstraints.of(lowerBound, upperBound, true, true, true);
  }

  @Override
  protected boolean indexSupported(final Index index) {
    return !(index instanceof CustomIndex);
  }

  @Override
  public void prepare(
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    if (valueExpr.isLiteral() && !(valueExpr instanceof NumericLiteral)) {
      valueExpr = NumericLiteral.of(valueExpr.evaluateValue(null));
    }
    if (lowerBoundExpr.isLiteral() && !(lowerBoundExpr instanceof NumericLiteral)) {
      lowerBoundExpr = NumericLiteral.of(lowerBoundExpr.evaluateValue(null));
    }
    if (upperBoundExpr.isLiteral() && !(upperBoundExpr instanceof NumericLiteral)) {
      upperBoundExpr = NumericLiteral.of(upperBoundExpr.evaluateValue(null));
    }
  }
}
