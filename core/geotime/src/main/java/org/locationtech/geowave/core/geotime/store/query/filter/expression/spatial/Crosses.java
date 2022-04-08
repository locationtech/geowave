/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial;

/**
 * Predicate that passes when the first operand crosses the second operand.
 */
public class Crosses extends BinarySpatialPredicate {

  public Crosses() {}

  public Crosses(final SpatialExpression expression1, final SpatialExpression expression2) {
    super(expression1, expression2);
  }

  @Override
  public boolean evaluateInternal(final FilterGeometry value1, final FilterGeometry value2) {
    return value1.crosses(value2);
  }

  @Override
  protected boolean isExact() {
    return false;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("CROSSES(");
    sb.append(expression1.toString());
    sb.append(",");
    sb.append(expression2.toString());
    sb.append(")");
    return sb.toString();
  }

}
