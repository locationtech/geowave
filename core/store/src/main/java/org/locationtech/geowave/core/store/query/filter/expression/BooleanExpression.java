/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression;

import java.util.Map;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

/**
 * An expression representing a boolean value. Also acts as a predicate since the expression itself
 * can be interpreted as either true or false. Any non-boolean object will evaluate to {@code true}
 * if it is non-null.
 */
public interface BooleanExpression extends GenericExpression, Predicate {

  @Override
  default boolean evaluate(final Map<String, Object> fieldValues) {
    return (Boolean) evaluateValue(fieldValues);
  }

  @Override
  default <T> boolean evaluate(final DataTypeAdapter<T> adapter, final T entry) {
    return (Boolean) evaluateValue(adapter, entry);
  }

  /**
   * Evaluate an object to determine if it should be interpreted as {@code true} or {@code false}.
   * 
   * @param value the object to evaluate
   * @return the evaluated boolean
   */
  public static boolean evaluateObject(final Object value) {
    if (value == null) {
      return false;
    }
    if (value instanceof Boolean) {
      return value.equals(true);
    }
    if (value instanceof Number) {
      return ((Number) value).longValue() != 0;
    }
    // Any non-null value should be considered true
    return true;
  }

}
