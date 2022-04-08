/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql.function.aggregation;

import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.store.query.aggregate.OptimalCountAggregation.FieldCountAggregation;

/**
 * Count aggregation function that accepts a single argument. If `*` is passed to the function, all
 * simple features will be counted. Otherwise, all non-null values of the given column will be
 * counted.
 */
public class CountFunction implements AggregationFunction<Long> {

  @Override
  public String getName() {
    return "COUNT";
  }

  @Override
  public Class<Long> getReturnType() {
    return Long.class;
  }

  @Override
  public <T> Aggregation<?, Long, T> getAggregation(
      final DataTypeAdapter<T> adapter,
      final String[] functionArgs) {
    if (functionArgs == null || functionArgs.length != 1) {
      throw new RuntimeException("COUNT takes exactly 1 parameter");
    }
    final FieldNameParam columnName =
        functionArgs[0].equals("*") ? null : new FieldNameParam(functionArgs[0]);
    if (columnName != null && adapter.getFieldDescriptor(columnName.getFieldName()) == null) {
      throw new RuntimeException(
          "No attribute called '" + columnName.getFieldName() + "' was found in the given type.");
    }
    return new FieldCountAggregation<>(columnName);
  }
}
