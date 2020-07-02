/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.aggregation;

import java.math.BigDecimal;
import org.locationtech.geowave.core.geotime.store.query.aggregate.FieldNameParam;

/**
 * Calculates the sum of all value of a given numeric attribute. Ignores null attribute values.
 */
public class VectorSumAggregation extends VectorMathAggregation {

  public VectorSumAggregation() {
    this(null);
  }

  public VectorSumAggregation(final FieldNameParam fieldNameParam) {
    super(fieldNameParam);
  }

  @Override
  protected BigDecimal agg(BigDecimal a, BigDecimal b) {
    if (a == null) {
      return b;
    } else if (b == null) {
      return a;
    }
    return a.add(b);
  }
}
