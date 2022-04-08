/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql.function.aggregation;

import java.math.BigDecimal;
import org.locationtech.geowave.core.store.query.aggregate.FieldSumAggregation;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.query.aggregate.FieldNameParam;

/**
 * Aggregation function that sums all non-null numeric values of a given column.
 */
public class SumFunction extends MathAggregationFunction {

  @Override
  public String getName() {
    return "SUM";
  }

  @Override
  protected <T> Aggregation<?, BigDecimal, T> aggregation(FieldNameParam columnName) {
    return new FieldSumAggregation<>(columnName);
  }

}
