/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.gwql;

import java.util.List;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalExpression;
import org.locationtech.geowave.core.store.query.filter.expression.Expression;
import org.locationtech.geowave.core.store.query.filter.expression.Predicate;
import org.locationtech.geowave.core.store.query.gwql.GWQLParseException;
import org.locationtech.geowave.core.store.query.gwql.function.predicate.PredicateFunction;

public class TemporalPredicates {

  private static abstract class TemporalPredicateFunction implements PredicateFunction {
    @Override
    public Predicate create(List<Expression<?>> arguments) {
      if (arguments.size() == 2) {
        final TemporalExpression expression1 =
            DateCastableType.toTemporalExpression(arguments.get(0));
        final TemporalExpression expression2 =
            DateCastableType.toTemporalExpression(arguments.get(1));
        return createInternal(expression1, expression2);
      }
      throw new GWQLParseException("Function expects 2 arguments, got " + arguments.size());
    }

    protected abstract Predicate createInternal(
        final TemporalExpression expression1,
        final TemporalExpression expression2);
  }

  public static class ContainsFunction extends TemporalPredicateFunction {
    @Override
    public String getName() {
      return "TCONTAINS";
    }

    @Override
    protected Predicate createInternal(
        final TemporalExpression expression1,
        final TemporalExpression expression2) {
      return expression1.contains(expression2);
    }
  }

  public static class OverlapsFunction extends TemporalPredicateFunction {
    @Override
    public String getName() {
      return "TOVERLAPS";
    }

    @Override
    protected Predicate createInternal(
        final TemporalExpression expression1,
        final TemporalExpression expression2) {
      return expression1.overlaps(expression2);
    }
  }

}
