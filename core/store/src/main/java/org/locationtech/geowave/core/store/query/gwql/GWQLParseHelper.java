/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql;

import java.util.List;
import org.apache.commons.text.StringEscapeUtils;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.query.filter.expression.ComparableExpression;
import org.locationtech.geowave.core.store.query.filter.expression.Expression;
import org.locationtech.geowave.core.store.query.filter.expression.FieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.GenericFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.InvalidFilterException;
import org.locationtech.geowave.core.store.query.filter.expression.Literal;
import org.locationtech.geowave.core.store.query.filter.expression.Predicate;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericExpression;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextLiteral;
import org.locationtech.geowave.core.store.query.gwql.function.expression.ExpressionFunction;
import org.locationtech.geowave.core.store.query.gwql.function.operator.OperatorFunction;
import org.locationtech.geowave.core.store.query.gwql.function.predicate.PredicateFunction;

/**
 * Helper functions for transforming GWQL into GeoWave objects.
 */
public class GWQLParseHelper {

  /**
   * Convert a GWQL text literal to a {@link TextLiteral} expression.
   * 
   * @param literal the GWQL literal
   * @return a {@code TextLiteral} that contains the literal string
   */
  public static TextLiteral evaluateTextLiteral(final String literal) {
    final String text =
        literal.substring(1, literal.length() - 1).replace("''", "'").replace("\\'", "'");
    return TextLiteral.of(StringEscapeUtils.unescapeJava(text));
  }

  /**
   * Gets a {@link FieldValue} expression from an adapter for the given field name.
   * 
   * @param adapter the data type adapter
   * @param fieldName the field name
   * @return the field value expression for the field
   */
  public static FieldValue<?> getFieldValue(
      final DataTypeAdapter<?> adapter,
      final String fieldName) {
    final FieldDescriptor<?> descriptor = adapter.getFieldDescriptor(fieldName);
    if (descriptor != null) {
      final FieldValue<?> fieldValue =
          GWQLExtensionRegistry.instance().createFieldValue(descriptor.bindingClass(), fieldName);
      if (fieldValue == null) {
        return GenericFieldValue.of(fieldName);
      }
      return fieldValue;
    }
    throw new GWQLParseException("Field " + fieldName + " did not exist in the specified type.");
  }

  /**
   * Gets an expression representing the sum of two input expressions.
   * 
   * @param expression1 the first expression
   * @param expression2 the expression to add
   * @return the added expressions
   */
  public static Expression<?> getAddExpression(
      final Expression<?> expression1,
      final Expression<?> expression2) {
    if (expression1 instanceof NumericExpression && expression2 instanceof NumericExpression) {
      return ((NumericExpression) expression1).add(expression2);
    }
    throw new GWQLParseException("Math operations require numeric expressions.");
  }

  /**
   * Gets an expression that represents one expression subtracted from another expression.
   * 
   * @param expression1 the first expression
   * @param expression2 the expression to subtract
   * @return the subtracted expressions
   */
  public static Expression<?> getSubtractExpression(
      final Expression<?> expression1,
      final Expression<?> expression2) {
    if (expression1 instanceof NumericExpression && expression2 instanceof NumericExpression) {
      return ((NumericExpression) expression1).subtract(expression2);
    }
    throw new GWQLParseException("Math operations require numeric expressions.");
  }

  /**
   * Gets an expression that represents the one expression multiplied by another expression.
   * 
   * @param expression1 the first expression
   * @param expression2 the expression to multiply by
   * @return the multiplied expressions
   */
  public static Expression<?> getMultiplyExpression(
      final Expression<?> expression1,
      final Expression<?> expression2) {
    if (expression1 instanceof NumericExpression && expression2 instanceof NumericExpression) {
      return ((NumericExpression) expression1).multiplyBy(expression2);
    }
    throw new GWQLParseException("Math operations require numeric expressions.");
  }

  /**
   * Gets an expression that represents one expression divided by another expression.
   * 
   * @param expression1 the first expression
   * @param expression2 the expression to divide by
   * @return the divided expressions
   */
  public static Expression<?> getDivideExpression(
      final Expression<?> expression1,
      final Expression<?> expression2) {
    if (expression1 instanceof NumericExpression && expression2 instanceof NumericExpression) {
      return ((NumericExpression) expression1).divideBy(expression2);
    }
    throw new GWQLParseException("Math operations require numeric expressions.");
  }

  /**
   * Gets a between predicate for the given comparable expression.
   * 
   * @param value the expression to evaluate
   * @param lowerBound the lower bound
   * @param upperBound the upper bound
   * @return a between predicate
   */
  public static Predicate getBetweenPredicate(
      final Expression<?> value,
      final Expression<?> lowerBound,
      final Expression<?> upperBound) {
    try {
      if (value instanceof ComparableExpression
          && lowerBound instanceof ComparableExpression
          && upperBound instanceof ComparableExpression) {
        return ((ComparableExpression<?>) value).isBetween(lowerBound, upperBound);
      }
    } catch (InvalidFilterException e) {
      // operands were incompatible
    }
    throw new GWQLParseException(
        "The BETWEEN operation is only supported for comparable expressions.");
  }

  /**
   * Gets an equals predicate for the given expressions.
   * 
   * @param expression1 the first expression
   * @param expression2 the second expression
   * @return the equals predicate
   */
  public static Predicate getEqualsPredicate(
      final Expression<?> expression1,
      final Expression<?> expression2) {
    return expression1.isEqualTo(expression2);
  }

  /**
   * Gets a not equals predicate for the given expressions.
   * 
   * @param expression1 the first expression
   * @param expression2 the second expression
   * @return the not equals predicate
   */
  public static Predicate getNotEqualsPredicate(
      final Expression<?> expression1,
      final Expression<?> expression2) {
    return expression1.isNotEqualTo(expression2);
  }

  /**
   * Gets a less than predicate for the given expressions.
   * 
   * @param expression1 the first expression
   * @param expression2 the second expression
   * @return the less than predicate
   */
  public static Predicate getLessThanPredicate(
      final Expression<?> expression1,
      final Expression<?> expression2) {
    try {
      if (expression1 instanceof ComparableExpression
          && expression2 instanceof ComparableExpression) {
        return ((ComparableExpression<?>) expression1).isLessThan(expression2);
      }
    } catch (InvalidFilterException e) {
      // operand was incompatible
    }
    throw new GWQLParseException(
        "Comparison operators can only be used on comparable expressions.");
  }

  /**
   * Gets a less than or equals predicate for the given expressions.
   * 
   * @param expression1 the first expression
   * @param expression2 the second expression
   * @return the less than or equals predicate
   */
  public static Predicate getLessThanOrEqualsPredicate(
      final Expression<?> expression1,
      final Expression<?> expression2) {
    try {
      if (expression1 instanceof ComparableExpression
          && expression2 instanceof ComparableExpression) {
        return ((ComparableExpression<?>) expression1).isLessThanOrEqualTo(expression2);
      }
    } catch (InvalidFilterException e) {
      // operand was incompatible
    }
    throw new GWQLParseException(
        "Comparison operators can only be used on comparable expressions.");
  }

  /**
   * Gets a greater than predicate for the given expressions.
   * 
   * @param expression1 the first expression
   * @param expression2 the second expression
   * @return the greater than predicate
   */
  public static Predicate getGreaterThanPredicate(
      final Expression<?> expression1,
      final Expression<?> expression2) {
    try {
      if (expression1 instanceof ComparableExpression
          && expression2 instanceof ComparableExpression) {
        return ((ComparableExpression<?>) expression1).isGreaterThan(expression2);
      }
    } catch (InvalidFilterException e) {
      // operand was incompatible
    }
    throw new GWQLParseException(
        "Comparison operators can only be used on comparable expressions.");
  }

  /**
   * Gets a greater than or equals predicate for the given expressions.
   * 
   * @param expression1 the first expression
   * @param expression2 the second expression
   * @return the greater than or equals predicate
   */
  public static Predicate getGreaterThanOrEqualsPredicate(
      final Expression<?> expression1,
      final Expression<?> expression2) {
    try {
      if (expression1 instanceof ComparableExpression
          && expression2 instanceof ComparableExpression) {
        return ((ComparableExpression<?>) expression1).isGreaterThanOrEqualTo(expression2);
      }
    } catch (InvalidFilterException e) {
      // operand was incompatible
    }
    throw new GWQLParseException(
        "Comparison operators can only be used on comparable expressions.");
  }

  /**
   * Gets an expression that matches the given function name and arguments.
   * 
   * @param functionName the name of the expression function
   * @param arguments the arguments of the function
   * @return the expression function
   */
  public static Expression<?> getExpressionFunction(
      final String functionName,
      final List<Expression<?>> arguments) {
    final ExpressionFunction<?> function =
        GWQLExtensionRegistry.instance().getExpressionFunction(functionName);
    if (function != null) {
      return function.create(arguments);
    }
    throw new GWQLParseException("No expression function was found with the name: " + functionName);
  }

  /**
   * Gets a predicate that matches the given function name and arguments.
   * 
   * @param functionName the name of the predicate function
   * @param arguments the arguments of the function
   * @return the predicate function
   */
  public static Predicate getPredicateFunction(
      final String functionName,
      final List<Expression<?>> arguments) {
    final PredicateFunction function =
        GWQLExtensionRegistry.instance().getPredicateFunction(functionName);
    if (function != null) {
      return function.create(arguments);
    }
    throw new GWQLParseException("No predicate function was found with the name: " + functionName);
  }

  /**
   * Gets the operator predicate that matches the given operator.
   * 
   * @param operator the operator
   * @param expression1 the first operand
   * @param expression2 the second operand
   * @return the operator predicate
   */
  public static Predicate getOperatorPredicate(
      final String operator,
      final Expression<?> expression1,
      final Expression<?> expression2) {
    final OperatorFunction function =
        GWQLExtensionRegistry.instance().getOperatorFunction(operator);
    if (function != null) {
      return function.create(expression1, expression2);
    }
    throw new GWQLParseException("No '" + operator + "' operator was found");
  }

  /**
   * Casts the given expression to the target type.
   * 
   * @param targetType the type to cast to
   * @param expression the base expression
   * @return the casted expression
   */
  public static Expression<?> castExpression(
      final String targetType,
      final Expression<?> expression) {
    final CastableType<?> type = GWQLExtensionRegistry.instance().getCastableType(targetType);
    if (type != null) {
      return type.cast(
          expression.isLiteral() ? ((Literal<?>) expression).evaluateValue(null) : expression);
    }
    throw new GWQLParseException("Type '" + targetType + "' is undefined");
  }
}
