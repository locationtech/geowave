/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.gwql.function;

import org.locationtech.geowave.core.geotime.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.geotime.store.query.aggregate.VectorBoundingBoxAggregation;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

/**
 * Bounding box aggregation function that accepts a single argument. If `*` is passed to the
 * function, the default geometry of the feature will be used for the calculation, otherwise, the
 * supplied geometry column name will be used.
 */
public class BboxFunction implements QLVectorAggregationFunction {

  @Override
  public Class<?> returnType() {
    return Envelope.class;
  }

  @Override
  public Aggregation<?, ?, SimpleFeature> getAggregation(
      final SimpleFeatureType featureType,
      final String[] functionArgs) {
    if (functionArgs == null || functionArgs.length != 1) {
      throw new RuntimeException("BBOX takes exactly 1 parameter");
    }
    final FieldNameParam columnName =
        functionArgs[0].equals("*") ? null : new FieldNameParam(functionArgs[0]);
    if (columnName != null) {
      AttributeDescriptor descriptor = featureType.getDescriptor(columnName.getFieldName());
      if (descriptor == null) {
        throw new RuntimeException(
            "No attribute called '" + columnName.getFieldName() + "' was found in the given type.");
      }
      if (!Geometry.class.isAssignableFrom(descriptor.getType().getBinding())) {
        throw new RuntimeException(
            "BBOX aggregation only works on geometry fields, given field was of type "
                + descriptor.getType().getBinding().getName()
                + ".");
      }
    }
    return new VectorBoundingBoxAggregation(columnName);
  }

}
