/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression.numeric;

import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.query.filter.expression.Literal;

/**
 * A numeric implementation of literal, representing numeric literal objects.
 */
public class NumericLiteral extends Literal<Double> implements NumericExpression {

  public NumericLiteral() {}

  public NumericLiteral(final Number literal) {
    super(literal == null ? null : literal.doubleValue());
  }

  @Override
  public <T> Double evaluateValue(final DataTypeAdapter<T> adapter, final T entry) {
    final Number value = super.evaluateValue(adapter, entry);
    if (value == null) {
      return null;
    }
    return value.doubleValue();
  }

  public static NumericLiteral of(final Number literal) {
    return new NumericLiteral(literal);
  }

}
