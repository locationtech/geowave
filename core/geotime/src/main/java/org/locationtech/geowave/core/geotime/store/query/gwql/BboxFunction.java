/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.gwql;

import org.locationtech.geowave.core.geotime.store.query.aggregate.VectorBoundingBoxAggregation;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.store.query.gwql.function.aggregation.AggregationFunction;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * Bounding box aggregation function that accepts a single argument. If `*` is passed to the
 * function, the default geometry of the feature will be used for the calculation, otherwise, the
 * supplied geometry column name will be used.
 */
public class BboxFunction implements AggregationFunction<Envelope> {

  @Override
  public String getName() {
    return "BBOX";
  }

  @Override
  public Class<Envelope> getReturnType() {
    return Envelope.class;
  }

  @Override
  public <T> Aggregation<?, Envelope, T> getAggregation(
      final DataTypeAdapter<T> adapter,
      final String[] functionArgs) {
    if (functionArgs == null || functionArgs.length != 1) {
      throw new RuntimeException("BBOX takes exactly 1 parameter");
    }
    final FieldNameParam columnName =
        functionArgs[0].equals("*") ? null : new FieldNameParam(functionArgs[0]);
    if (columnName != null) {
      FieldDescriptor<?> descriptor = adapter.getFieldDescriptor(columnName.getFieldName());
      if (descriptor == null) {
        throw new RuntimeException(
            "No attribute called '" + columnName.getFieldName() + "' was found in the given type.");
      }
      if (!Geometry.class.isAssignableFrom(descriptor.bindingClass())) {
        throw new RuntimeException(
            "BBOX aggregation only works on geometry fields, given field was of type "
                + descriptor.bindingClass().getName()
                + ".");
      }
    }
    return new VectorBoundingBoxAggregation<>(columnName);
  }

}
