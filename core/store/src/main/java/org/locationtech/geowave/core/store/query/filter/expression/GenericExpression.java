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
 * A generic expression for doing basic comparisons of field values and literals that are not
 * represented by other expression implementations.
 */
public interface GenericExpression extends Expression<Object> {

  @Override
  default Predicate isEqualTo(final Object other) {
    return new GenericEqualTo(this, toExpression(other));
  }

  @Override
  default Predicate isNotEqualTo(final Object other) {
    return new GenericNotEqualTo(this, toExpression(other));
  }

  @SuppressWarnings("unchecked")
  public static Expression<Object> toExpression(final Object object) {
    if (object instanceof Expression) {
      return (Expression<Object>) object;
    }
    return GenericLiteral.of(object);
  }
}
