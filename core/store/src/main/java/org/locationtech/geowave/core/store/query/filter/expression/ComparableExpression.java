/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression;

/**
 * An extension of the expression interface for comparable expression types.
 * 
 * @param <V> the comparable class
 */
public interface ComparableExpression<V> extends Expression<V> {

  /**
   * Create a predicate that tests to see if this expression is less than the provided object. The
   * operand can be either another expression or should evaluate to a literal of the same type.
   * 
   * @param other the object to test against
   * @return the less than predicate
   */
  Predicate isLessThan(final Object other);

  /**
   * Create a predicate that tests to see if this expression is less than or equal to the provided
   * object. The operand can be either another expression or should evaluate to a literal of the
   * same type.
   * 
   * @param other the object to test against
   * @return the less than or equal to predicate
   */
  Predicate isLessThanOrEqualTo(final Object other);

  /**
   * Create a predicate that tests to see if this expression is greater than the provided object.
   * The operand can be either another expression or should evaluate to a literal of the same type.
   * 
   * @param other the object to test against
   * @return the greater than predicate
   */
  Predicate isGreaterThan(final Object other);

  /**
   * Create a predicate that tests to see if this expression is greater than or equal to the
   * provided object. The operand can be either another expression or should evaluate to a literal
   * of the same type.
   * 
   * @param other the object to test against
   * @return the greater than or equal to predicate
   */
  Predicate isGreaterThanOrEqualTo(final Object other);

  /**
   * Create a predicate that tests to see if this expression is between the provided lower and upper
   * bounds. The operands can be either other expressions or should evaluate to literals of the same
   * type.
   * 
   * @param lowerBound the lower bound to test against
   * @param upperBound the upper bound to test against
   * @return the between predicate
   */
  Predicate isBetween(final Object lowerBound, final Object upperBound);

}
