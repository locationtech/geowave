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
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.query.aggregate.FieldNameParam;

/**
 * Base aggregation function for performing math aggregations on numeric columns.
 */
public abstract class MathAggregationFunction implements AggregationFunction<BigDecimal> {

  @Override
  public Class<BigDecimal> getReturnType() {
    return BigDecimal.class;
  }

  @Override
  public <T> Aggregation<?, BigDecimal, T> getAggregation(
      final DataTypeAdapter<T> adapter,
      final String[] functionArgs) {
    if (functionArgs == null || functionArgs.length != 1) {
      throw new RuntimeException(getName() + " takes exactly 1 parameter");
    }
    if (functionArgs[0].equals("*")) {
      throw new RuntimeException(getName() + " expects a numeric column.");
    }
    final FieldNameParam columnName = new FieldNameParam(functionArgs[0]);
    FieldDescriptor<?> descriptor = adapter.getFieldDescriptor(columnName.getFieldName());
    if (descriptor == null) {
      throw new RuntimeException(
          "No attribute called '" + columnName.getFieldName() + "' was found in the given type.");
    }
    if (!Number.class.isAssignableFrom(descriptor.bindingClass())) {
      throw new RuntimeException(
          getName()
              + " aggregation only works on numeric fields, given field was of type "
              + descriptor.bindingClass().getName()
              + ".");
    }
    return aggregation(columnName);
  }

  protected abstract <T> Aggregation<?, BigDecimal, T> aggregation(final FieldNameParam columnName);
}
