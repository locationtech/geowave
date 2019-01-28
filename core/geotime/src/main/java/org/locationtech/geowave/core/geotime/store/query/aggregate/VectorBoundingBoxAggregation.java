/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.aggregate;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;

public class VectorBoundingBoxAggregation extends
    BoundingBoxAggregation<FieldNameParam, SimpleFeature> {
  private FieldNameParam fieldNameParam;

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
  protected Envelope getEnvelope(final SimpleFeature entry) {
    Object o;
    if ((fieldNameParam != null) && !fieldNameParam.isEmpty()) {
      o = entry.getAttribute(fieldNameParam.getFieldName());
    } else {
      o = entry.getDefaultGeometry();
    }
    if ((o != null) && (o instanceof Geometry)) {
      final Geometry geometry = (Geometry) o;
      if (!geometry.isEmpty()) {
        return geometry.getEnvelopeInternal();
      }
    }
    return null;
  }
}
