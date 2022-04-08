/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql.type;

import org.locationtech.geowave.core.store.query.filter.expression.Expression;
import org.locationtech.geowave.core.store.query.filter.expression.InvalidFilterException;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericExpression;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericLiteral;
import org.locationtech.geowave.core.store.query.gwql.CastableType;
import org.locationtech.geowave.core.store.query.gwql.GWQLParseException;

public class NumberCastableType implements CastableType<Double> {
  @Override
  public String getName() {
    return "number";
  }

  @Override
  public NumericExpression cast(Object objectOrExpression) {
    return toNumericExpression(objectOrExpression);
  }

  public static NumericExpression toNumericExpression(Object objectOrExpression) {
    if (objectOrExpression instanceof NumericExpression) {
      return (NumericExpression) objectOrExpression;
    }
    if (objectOrExpression instanceof Expression
        && ((Expression<?>) objectOrExpression).isLiteral()) {
      objectOrExpression = ((Expression<?>) objectOrExpression).evaluateValue(null);
    }
    if (objectOrExpression instanceof Expression) {
      throw new GWQLParseException("Unable to cast expression to number");
    } else {
      try {
        return NumericLiteral.of(objectOrExpression);
      } catch (InvalidFilterException e) {
        throw new GWQLParseException("Unable to cast literal to date", e);
      }
    }
  }
}
