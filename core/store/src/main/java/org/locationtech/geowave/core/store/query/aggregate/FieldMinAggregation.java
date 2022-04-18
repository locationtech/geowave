/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.aggregate;

import java.math.BigDecimal;

/**
 * Aggregates to find the minimum value of a given numeric attribute. Ignores null attribute values.
 */
public class FieldMinAggregation<T> extends FieldMathAggregation<T> {

  public FieldMinAggregation() {
    this(null);
  }

  public FieldMinAggregation(final FieldNameParam fieldNameParam) {
    super(fieldNameParam);
  }

  @Override
  protected BigDecimal agg(final BigDecimal a, final BigDecimal b) {
    if (a == null) {
      return b;
    } else if (b == null) {
      return a;
    }
    return a.min(b);
  }

}
