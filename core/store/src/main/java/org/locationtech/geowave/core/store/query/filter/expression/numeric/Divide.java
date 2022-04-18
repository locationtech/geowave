/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression.numeric;

/**
 * An expression that divides the values of two numeric expressions.
 */
public class Divide extends MathExpression {

  public Divide() {}

  public Divide(final NumericExpression expr1, final NumericExpression expr2) {
    super(expr1, expr2);
  }

  @Override
  protected double doOperation(final double value1, final double value2) {
    return value1 / value2;
  }

  @Override
  protected String getOperatorString() {
    return "/";
  }

}
