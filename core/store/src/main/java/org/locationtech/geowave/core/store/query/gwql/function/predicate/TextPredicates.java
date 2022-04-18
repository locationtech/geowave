/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql.function.predicate;

import java.util.List;
import org.locationtech.geowave.core.store.query.filter.expression.BooleanLiteral;
import org.locationtech.geowave.core.store.query.filter.expression.Expression;
import org.locationtech.geowave.core.store.query.filter.expression.Predicate;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextExpression;
import org.locationtech.geowave.core.store.query.gwql.GWQLParseException;
import org.locationtech.geowave.core.store.query.gwql.type.TextCastableType;

public class TextPredicates {
  private static abstract class TextPredicateFunction implements PredicateFunction {
    @Override
    public Predicate create(List<Expression<?>> arguments) {
      if (arguments.size() < 2 || arguments.size() > 3) {
        throw new GWQLParseException("Function expects 2 or 3 arguments, got " + arguments.size());
      }
      final TextExpression expression1 = TextCastableType.toTextExpression(arguments.get(0));
      final TextExpression expression2 = TextCastableType.toTextExpression(arguments.get(1));
      final boolean ignoreCase;
      if (arguments.size() == 3) {
        if (arguments.get(2) instanceof BooleanLiteral) {
          ignoreCase = ((BooleanLiteral) arguments.get(2)).evaluateValue(null);
        } else {
          throw new GWQLParseException(
              "Function expects a boolean literal for the third argument.");
        }
      } else {
        ignoreCase = false;
      }
      return createInternal(expression1, expression2, ignoreCase);
    }

    protected abstract Predicate createInternal(
        final TextExpression expression1,
        final TextExpression expression2,
        final boolean ignoreCase);
  }

  public static class StrStartsWithFunction extends TextPredicateFunction {
    @Override
    public String getName() {
      return "STRSTARTSWITH";
    }

    @Override
    protected Predicate createInternal(
        TextExpression expression1,
        TextExpression expression2,
        final boolean ignoreCase) {
      return expression1.startsWith(expression2, ignoreCase);
    }
  }

  public static class StrEndsWithFunction extends TextPredicateFunction {
    @Override
    public String getName() {
      return "STRENDSWITH";
    }

    @Override
    protected Predicate createInternal(
        TextExpression expression1,
        TextExpression expression2,
        final boolean ignoreCase) {
      return expression1.endsWith(expression2, ignoreCase);
    }
  }

  public static class StrContainsFunction extends TextPredicateFunction {
    @Override
    public String getName() {
      return "STRCONTAINS";
    }

    @Override
    protected Predicate createInternal(
        TextExpression expression1,
        TextExpression expression2,
        final boolean ignoreCase) {
      return expression1.contains(expression2, ignoreCase);
    }
  }
}
