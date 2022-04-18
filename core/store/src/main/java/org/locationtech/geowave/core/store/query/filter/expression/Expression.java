/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression;

import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

/**
 * Base interface for any expression that evaluates to some value to be used by a predicate.
 *
 * @param <V> the evaluated value class
 */
public interface Expression<V> extends Persistable {

  /**
   * Evaluate the expression using the provided field values.
   * 
   * @param fieldValues the field values to use
   * @return the evaluated expression value
   */
  V evaluateValue(Map<String, Object> fieldValues);

  /**
   * Evaluate the expression using the provided adapter and entry.
   * 
   * @param <T> the data type of the adapter
   * @param adapter the data type adapter
   * @param entry the entry
   * @return the evaluated expression value
   */
  <T> V evaluateValue(DataTypeAdapter<T> adapter, T entry);

  /**
   * @return {@code true} if this expression does not require any adapter field values to compute
   */
  boolean isLiteral();

  /**
   * Adds any fields referenced by this expression to the provided set.
   * 
   * @param fields the set to add any referenced fields to
   */
  void addReferencedFields(final Set<String> fields);

  /**
   * Create a predicate that tests to see if this expression is equal ton the provided object. The
   * operand can be either another expression or should evaluate to a literal of the same type.
   * 
   * @param other the object to test against
   * @return the equals predicate
   */
  Predicate isEqualTo(final Object other);

  /**
   * Create a predicate that tests to see if this expression is not equal ton the provided object.
   * The operand can be either another expression or should evaluate to a literal of the same type.
   * 
   * @param other the object to test against
   * @return the not equals predicate
   */
  Predicate isNotEqualTo(final Object other);

  /**
   * Create a predicate that tests to see if this expression is null.
   * 
   * @return the is null predicate
   */
  default Predicate isNull() {
    return new IsNull(this);
  }

  /**
   * Create a predicate that tests to see if this expression is not null.
   * 
   * @return the not null predicate
   */
  default Predicate isNotNull() {
    return new IsNotNull(this);
  }

}
