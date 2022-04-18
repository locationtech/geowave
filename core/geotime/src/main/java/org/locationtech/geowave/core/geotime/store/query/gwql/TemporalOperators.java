/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.gwql;

import org.locationtech.geowave.core.store.query.filter.expression.Expression;
import org.locationtech.geowave.core.store.query.filter.expression.Predicate;
import org.locationtech.geowave.core.store.query.gwql.function.operator.OperatorFunction;

public class TemporalOperators {
  public static class BeforeOperator implements OperatorFunction {
    @Override
    public String getName() {
      return "BEFORE";
    }

    @Override
    public Predicate create(Expression<?> expression1, Expression<?> expression2) {
      return DateCastableType.toTemporalExpression(expression1).isBefore(
          DateCastableType.toTemporalExpression(expression2));
    }
  }

  public static class BeforeOrDuringOperator implements OperatorFunction {
    @Override
    public String getName() {
      return "BEFORE_OR_DURING";
    }

    @Override
    public Predicate create(Expression<?> expression1, Expression<?> expression2) {
      return DateCastableType.toTemporalExpression(expression1).isBeforeOrDuring(
          DateCastableType.toTemporalExpression(expression2));
    }
  }

  public static class DuringOperator implements OperatorFunction {
    @Override
    public String getName() {
      return "DURING";
    }

    @Override
    public Predicate create(Expression<?> expression1, Expression<?> expression2) {
      return DateCastableType.toTemporalExpression(expression1).isDuring(
          DateCastableType.toTemporalExpression(expression2));
    }
  }

  public static class DuringOrAfterOperator implements OperatorFunction {
    @Override
    public String getName() {
      return "DURING_OR_AFTER";
    }

    @Override
    public Predicate create(Expression<?> expression1, Expression<?> expression2) {
      return DateCastableType.toTemporalExpression(expression1).isDuringOrAfter(
          DateCastableType.toTemporalExpression(expression2));
    }
  }

  public static class AfterOperator implements OperatorFunction {
    @Override
    public String getName() {
      return "AFTER";
    }

    @Override
    public Predicate create(Expression<?> expression1, Expression<?> expression2) {
      return DateCastableType.toTemporalExpression(expression1).isAfter(
          DateCastableType.toTemporalExpression(expression2));
    }
  }
}
