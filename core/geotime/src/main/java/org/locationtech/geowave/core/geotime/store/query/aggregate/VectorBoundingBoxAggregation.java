/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.aggregate;

import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.query.aggregate.FieldNameParam;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

public class VectorBoundingBoxAggregation<T> extends BoundingBoxAggregation<FieldNameParam, T> {
  private FieldNameParam fieldNameParam;
  private String spatialField = null;

  public VectorBoundingBoxAggregation() {
    this(null);
  }

  public VectorBoundingBoxAggregation(final FieldNameParam fieldNameParam) {
    super();
    this.fieldNameParam = fieldNameParam;
  }

  @Override
  public FieldNameParam getParameters() {
    return fieldNameParam;
  }

  @Override
  public void setParameters(final FieldNameParam fieldNameParam) {
    this.fieldNameParam = fieldNameParam;
  }

  @Override
  protected Envelope getEnvelope(final DataTypeAdapter<T> adapter, final T entry) {
    if ((fieldNameParam != null) && !fieldNameParam.isEmpty()) {
      final Object o = adapter.getFieldValue(entry, fieldNameParam.getFieldName());
      if (o instanceof Geometry) {
        final Geometry geometry = (Geometry) o;
        return geometry.getEnvelopeInternal();
      }
    } else {
      if (spatialField == null) {
        for (final FieldDescriptor<?> descriptor : adapter.getFieldDescriptors()) {
          if (Geometry.class.isAssignableFrom(descriptor.bindingClass())) {
            spatialField = descriptor.fieldName();
            break;
          }
        }
      }
      if (spatialField != null) {
        return ((Geometry) adapter.getFieldValue(entry, spatialField)).getEnvelopeInternal();
      }
    }
    return null;
  }
}
