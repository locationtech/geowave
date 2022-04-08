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
import org.locationtech.geowave.core.store.query.filter.expression.text.TextExpression;
import org.locationtech.geowave.core.store.query.gwql.GWQLParseException;

public class ConcatFunction implements ExpressionFunction<String> {

  @Override
  public String getName() {
    return "CONCAT";
  }

  @Override
  public Class<String> getReturnType() {
    return String.class;
  }

  @Override
  public Expression<String> create(List<Expression<?>> arguments) {
    if (arguments.size() == 2 && arguments.stream().allMatch(a -> a instanceof TextExpression)) {
      return ((TextExpression) arguments.get(0)).concat(arguments.get(1));
    }
    throw new GWQLParseException("CONCAT expects 2 text expressions.");
  }

}
