/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression.numeric;

import org.locationtech.geowave.core.store.query.filter.expression.FieldValue;

/**
 * A field value implementation for numeric adapter fields.
 */
public class NumericFieldValue extends FieldValue<Double> implements NumericExpression {

  public NumericFieldValue() {}

  public NumericFieldValue(final String fieldName) {
    super(fieldName);
  }

  @Override
  protected Double evaluateValueInternal(final Object value) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    throw new RuntimeException(
        "Field value did not evaluate to a number: " + value.getClass().toString());
  }

  public static NumericFieldValue of(final String fieldName) {
    return new NumericFieldValue(fieldName);
  }

}
