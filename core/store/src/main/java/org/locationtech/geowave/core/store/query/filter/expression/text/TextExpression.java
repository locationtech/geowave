/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression.text;

import org.locationtech.geowave.core.store.query.filter.expression.ComparableExpression;
import org.locationtech.geowave.core.store.query.filter.expression.ComparisonOperator.CompareOp;
import org.locationtech.geowave.core.store.query.filter.expression.FieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.Predicate;

/**
 * An expression that evaluates to a text (string) value.
 */
public interface TextExpression extends ComparableExpression<String> {

  /**
   * Create a new expression by concatenating this expression and a given operand. The operand can
   * be either another text expression or should evaluate to a text literal.
   * 
   * @param other the object to concatenate
   * @return an expression representing the concatenated values
   */
  default TextExpression concat(final Object other) {
    return new Concat(this, toTextExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression ends with the provided object. The
   * operand can be either another text expression, or any object that can be converted to a text
   * literal.
   * 
   * @param other the text object to test against
   * @return the ends with predicate
   */
  default Predicate endsWith(final Object other) {
    return new EndsWith(this, toTextExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression ends with the provided object. The
   * operand can be either another text expression, or any object that can be converted to a text
   * literal.
   * 
   * @param other the text object to test against
   * @param ignoreCase whether or not to ignore the casing of the expressions
   * @return the ends with predicate
   */
  default Predicate endsWith(final Object other, final boolean ignoreCase) {
    return new EndsWith(this, toTextExpression(other), ignoreCase);
  }

  /**
   * Create a predicate that tests to see if this expression starts with the provided object. The
   * operand can be either another text expression, or any object that can be converted to a text
   * literal.
   * 
   * @param other the text object to test against
   * @return the starts with predicate
   */
  default Predicate startsWith(final Object other) {
    return new StartsWith(this, toTextExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression starts with the provided object. The
   * operand can be either another text expression, or any object that can be converted to a text
   * literal.
   * 
   * @param other the text object to test against
   * @param ignoreCase whether or not to ignore the casing of the expressions
   * @return the starts with predicate
   */
  default Predicate startsWith(final Object other, final boolean ignoreCase) {
    return new StartsWith(this, toTextExpression(other), ignoreCase);
  }

  /**
   * Create a predicate that tests to see if this expression contains the provided object. The
   * operand can be either another text expression, or any object that can be converted to a text
   * literal.
   * 
   * @param other the text object to test against
   * @return the contains predicate
   */
  default Predicate contains(final Object other) {
    return new Contains(this, toTextExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression contains the provided object. The
   * operand can be either another text expression, or any object that can be converted to a text
   * literal.
   * 
   * @param other the text object to test against
   * @param ignoreCase whether or not to ignore the casing of the expressions
   * @return the contains predicate
   */
  default Predicate contains(final Object other, final boolean ignoreCase) {
    return new Contains(this, toTextExpression(other), ignoreCase);
  }

  /**
   * Create a predicate that tests to see if this expression is less than the provided object. The
   * operand can be either another text expression, or any object that can be converted to a text
   * literal.
   * 
   * @param other the text object to test against
   * @return the less than predicate
   */
  @Override
  default Predicate isLessThan(final Object other) {
    return new TextComparisonOperator(this, toTextExpression(other), CompareOp.LESS_THAN);
  }

  /**
   * Create a predicate that tests to see if this expression is less than the provided object. The
   * operand can be either another text expression, or any object that can be converted to a text
   * literal.
   * 
   * @param other the text object to test against
   * @param ignoreCase whether or not to ignore the casing of the expressions
   * @return the less than predicate
   */
  default Predicate isLessThan(final Object other, final boolean ignoreCase) {
    return new TextComparisonOperator(
        this,
        toTextExpression(other),
        CompareOp.LESS_THAN,
        ignoreCase);
  }

  /**
   * Create a predicate that tests to see if this expression is less than or equal to the provided
   * object. The operand can be either another text expression, or any object that can be converted
   * to a text literal.
   * 
   * @param other the text object to test against
   * @return the less than or equal to predicate
   */
  @Override
  default Predicate isLessThanOrEqualTo(final Object other) {
    return new TextComparisonOperator(this, toTextExpression(other), CompareOp.LESS_THAN_OR_EQUAL);
  }

  /**
   * Create a predicate that tests to see if this expression is less than or equal to the provided
   * object. The operand can be either another text expression, or any object that can be converted
   * to a text literal.
   * 
   * @param other the text object to test against
   * @param ignoreCase whether or not to ignore the casing of the expressions
   * @return the less than or equal to predicate
   */
  default Predicate isLessThanOrEqualTo(final Object other, final boolean ignoreCase) {
    return new TextComparisonOperator(
        this,
        toTextExpression(other),
        CompareOp.LESS_THAN_OR_EQUAL,
        ignoreCase);
  }

  /**
   * Create a predicate that tests to see if this expression is greater than the provided object.
   * The operand can be either another text expression, or any object that can be converted to a
   * text literal.
   * 
   * @param other the text object to test against
   * @return the greater than predicate
   */
  @Override
  default Predicate isGreaterThan(final Object other) {
    return new TextComparisonOperator(this, toTextExpression(other), CompareOp.GREATER_THAN);
  }

  /**
   * Create a predicate that tests to see if this expression is greater than the provided object.
   * The operand can be either another text expression, or any object that can be converted to a
   * text literal.
   * 
   * @param other the text object to test against
   * @param ignoreCase whether or not to ignore the casing of the expressions
   * @return the greater than predicate
   */
  default Predicate isGreaterThan(final Object other, final boolean ignoreCase) {
    return new TextComparisonOperator(
        this,
        toTextExpression(other),
        CompareOp.GREATER_THAN,
        ignoreCase);
  }

  /**
   * Create a predicate that tests to see if this expression is greater than or equal to the
   * provided object. The operand can be either another text expression, or any object that can be
   * converted to a text literal.
   * 
   * @param other the text object to test against
   * @return the greater than or equal to predicate
   */
  @Override
  default Predicate isGreaterThanOrEqualTo(final Object other) {
    return new TextComparisonOperator(
        this,
        toTextExpression(other),
        CompareOp.GREATER_THAN_OR_EQUAL);
  }

  /**
   * Create a predicate that tests to see if this expression is greater than or equal to the
   * provided object. The operand can be either another text expression, or any object that can be
   * converted to a text literal.
   * 
   * @param other the text object to test against
   * @param ignoreCase whether or not to ignore the casing of the expressions
   * @return the greater than or equal to predicate
   */
  default Predicate isGreaterThanOrEqualTo(final Object other, final boolean ignoreCase) {
    return new TextComparisonOperator(
        this,
        toTextExpression(other),
        CompareOp.GREATER_THAN_OR_EQUAL,
        ignoreCase);
  }

  /**
   * Create a predicate that tests to see if this expression is between the provided lower and upper
   * bounds. The operands can be either other text expressions, or any objects that can be converted
   * to text literals.
   * 
   * @param lowerBound the lower bound text object to test against
   * @param upperBound the upper bound text object to test against
   * @return the between predicate
   */
  @Override
  default Predicate isBetween(final Object lowerBound, final Object upperBound) {
    return new TextBetween(this, toTextExpression(lowerBound), toTextExpression(upperBound));
  }

  /**
   * Create a predicate that tests to see if this expression is between the provided lower and upper
   * bounds. The operands can be either other text expressions, or any objects that can be converted
   * to text literals.
   * 
   * @param lowerBound the lower bound text object to test against
   * @param upperBound the upper bound text object to test against
   * @param ignoreCase whether or not to ignore the casing of the expressions
   * @return the between predicate
   */
  default Predicate isBetween(
      final Object lowerBound,
      final Object upperBound,
      final boolean ignoreCase) {
    return new TextBetween(
        this,
        toTextExpression(lowerBound),
        toTextExpression(upperBound),
        ignoreCase);
  }

  /**
   * Create a predicate that tests to see if this expression is equal to the provided object. The
   * operand can be either another text expression, or any object that can be converted to a text
   * literal.
   * 
   * @param other the text object to test against
   * @return the equals predicate
   */
  @Override
  default Predicate isEqualTo(final Object other) {
    return new TextComparisonOperator(this, toTextExpression(other), CompareOp.EQUAL_TO);
  }

  /**
   * Create a predicate that tests to see if this expression is equal to the provided object. The
   * operand can be either another text expression, or any object that can be converted to a text
   * literal.
   * 
   * @param other the text object to test against
   * @param ignoreCase whether or not to ignore the casing of the expressions
   * @return the equals predicate
   */
  default Predicate isEqualTo(final Object other, final boolean ignoreCase) {
    return new TextComparisonOperator(
        this,
        toTextExpression(other),
        CompareOp.EQUAL_TO,
        ignoreCase);
  }

  /**
   * Create a predicate that tests to see if this expression is not equal to the provided object.
   * The operand can be either another text expression, or any object that can be converted to a
   * text literal.
   * 
   * @param other the text object to test against
   * @return the not equals predicate
   */
  @Override
  default Predicate isNotEqualTo(final Object other) {
    return new TextComparisonOperator(this, toTextExpression(other), CompareOp.NOT_EQUAL_TO);
  }

  /**
   * Create a predicate that tests to see if this expression is not equal to the provided object.
   * The operand can be either another text expression, or any object that can be converted to a
   * text literal.
   * 
   * @param other the text object to test against
   * @param ignoreCase whether or not to ignore the casing of the expressions
   * @return the not equals predicate
   */
  default Predicate isNotEqualTo(final Object other, final boolean ignoreCase) {
    return new TextComparisonOperator(
        this,
        toTextExpression(other),
        CompareOp.NOT_EQUAL_TO,
        ignoreCase);
  }

  /**
   * Convert the given object to a text expression, if it isn't one already.
   * 
   * @param obj the object to convert
   * @return the text expression
   */
  default TextExpression toTextExpression(final Object obj) {
    if (obj instanceof TextExpression) {
      return (TextExpression) obj;
    } else if (obj instanceof FieldValue) {
      return TextFieldValue.of(((FieldValue<?>) obj).getFieldName());
    }
    return TextLiteral.of(obj);
  }

}
