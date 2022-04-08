/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression.numeric;

import org.locationtech.geowave.core.store.query.filter.expression.ComparableExpression;
import org.locationtech.geowave.core.store.query.filter.expression.ComparisonOperator.CompareOp;
import org.locationtech.geowave.core.store.query.filter.expression.Predicate;

/**
 * An expression that evaluates to a numeric (double) value.
 */
public interface NumericExpression extends ComparableExpression<Double> {

  /**
   * Create a new expression by adding the given operand to this expression. The operand can be
   * either another numeric expression or should evaluate to a numeric literal.
   * 
   * @param other the object to add
   * @return an expression representing the added values
   */
  default NumericExpression add(final Object other) {
    return new Add(this, toNumericExpression(other));
  }

  /**
   * Create a new expression by subtracting the given operand from this expression. The operand can
   * be either another numeric expression or should evaluate to a numeric literal.
   * 
   * @param other the object to subtract
   * @return an expression representing the subtracted values
   */
  default NumericExpression subtract(final Object other) {
    return new Subtract(this, toNumericExpression(other));
  }

  /**
   * Create a new expression by multiplying this expression by the given operand. The operand can be
   * either another numeric expression or should evaluate to a numeric literal.
   * 
   * @param other the object to multiply by
   * @return an expression representing the multiplied values
   */
  default NumericExpression multiplyBy(final Object other) {
    return new Multiply(this, toNumericExpression(other));
  }

  /**
   * Create a new expression by dividing this expression by the given operand. The operand can be
   * either another numeric expression or should evaluate to a numeric literal.
   * 
   * @param other the object to divide by
   * @return an expression representing the divided values
   */
  default NumericExpression divideBy(final Object other) {
    return new Divide(this, toNumericExpression(other));
  }

  /**
   * Create a new expression by taking the absolute value of this expression.
   * 
   * @return an expression representing the absolute value of this expression
   */
  default NumericExpression abs() {
    return new Abs(this);
  }

  /**
   * Create a predicate that tests to see if this expression is less than the provided object. The
   * operand can be either another numeric expression, or any object that can be converted to a
   * numeric literal.
   * 
   * @param other the numeric object to test against
   * @return the less than predicate
   */
  @Override
  default Predicate isLessThan(final Object other) {
    return new NumericComparisonOperator(this, toNumericExpression(other), CompareOp.LESS_THAN);
  }

  /**
   * Create a predicate that tests to see if this expression is less than or equal to the provided
   * object. The operand can be either another numeric expression, or any object that can be
   * converted to a numeric literal.
   * 
   * @param other the numeric object to test against
   * @return the less than or equal to predicate
   */
  @Override
  default Predicate isLessThanOrEqualTo(final Object other) {
    return new NumericComparisonOperator(
        this,
        toNumericExpression(other),
        CompareOp.LESS_THAN_OR_EQUAL);
  }

  /**
   * Create a predicate that tests to see if this expression is greater than the provided object.
   * The operand can be either another numeric expression, or any object that can be converted to a
   * numeric literal.
   * 
   * @param other the numeric object to test against
   * @return the greater than predicate
   */
  @Override
  default Predicate isGreaterThan(final Object other) {
    return new NumericComparisonOperator(this, toNumericExpression(other), CompareOp.GREATER_THAN);
  }

  /**
   * Create a predicate that tests to see if this expression is greater than or equal to the
   * provided object. The operand can be either another numeric expression, or any object that can
   * be converted to a numeric literal.
   * 
   * @param other the numeric object to test against
   * @return the greater than or equal to predicate
   */
  @Override
  default Predicate isGreaterThanOrEqualTo(final Object other) {
    return new NumericComparisonOperator(
        this,
        toNumericExpression(other),
        CompareOp.GREATER_THAN_OR_EQUAL);
  }

  /**
   * Create a predicate that tests to see if this expression is equal to the provided object. The
   * operand can be either another numeric expression, or any object that can be converted to a
   * numeric literal.
   * 
   * @param other the numeric object to test against
   * @return the equals predicate
   */
  @Override
  default Predicate isEqualTo(final Object other) {
    return new NumericComparisonOperator(this, toNumericExpression(other), CompareOp.EQUAL_TO);
  }

  /**
   * Create a predicate that tests to see if this expression is not equal to the provided object.
   * The operand can be either another numeric expression, or any object that can be converted to a
   * numeric literal.
   * 
   * @param other the numeric object to test against
   * @return the not equals predicate
   */
  @Override
  default Predicate isNotEqualTo(final Object other) {
    return new NumericComparisonOperator(this, toNumericExpression(other), CompareOp.NOT_EQUAL_TO);
  }

  /**
   * Create a predicate that tests to see if this expression is between the provided lower and upper
   * bounds. The operands can be either numeric expressions, or any objects that can be converted to
   * numeric literals.
   * 
   * @param lowerBound the lower bound to test against
   * @param upperBound the upper bound to test against
   * @return the between predicate
   */
  @Override
  default Predicate isBetween(final Object lowerBound, final Object upperBound) {
    return new NumericBetween(
        this,
        toNumericExpression(lowerBound),
        toNumericExpression(upperBound));
  }

  /**
   * Convert the given object to a numeric expression, if it isn't one already.
   * 
   * @param obj the object to convert
   * @return the numeric expression
   */
  default NumericExpression toNumericExpression(final Object obj) {
    if (obj instanceof NumericExpression) {
      return (NumericExpression) obj;
    }
    return NumericLiteral.of(obj);
  }

}
