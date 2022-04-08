/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql.function.expression;

import java.util.List;
import org.locationtech.geowave.core.store.query.filter.expression.Expression;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericExpression;
import org.locationtech.geowave.core.store.query.gwql.GWQLParseException;

public class AbsFunction implements ExpressionFunction<Double> {

  @Override
  public String getName() {
    return "ABS";
  }

  @Override
  public Class<Double> getReturnType() {
    return Double.class;
  }

  @Override
  public Expression<Double> create(List<Expression<?>> arguments) {
    if (arguments.size() == 1 && arguments.get(0) instanceof NumericExpression) {
      return ((NumericExpression) arguments.get(0)).abs();
    }
    throw new GWQLParseException("ABS expects exactly 1 numeric expression.");
  }

}
