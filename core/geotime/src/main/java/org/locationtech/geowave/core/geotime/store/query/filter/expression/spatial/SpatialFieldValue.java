/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial;

import org.locationtech.geowave.core.geotime.adapter.SpatialFieldDescriptor;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.query.filter.expression.FieldValue;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * A field value implementation for spatial adapter fields.
 */
public class SpatialFieldValue extends FieldValue<FilterGeometry> implements SpatialExpression {

  public SpatialFieldValue() {}

  public SpatialFieldValue(final String fieldName) {
    super(fieldName);
  }

  @Override
  public CoordinateReferenceSystem getCRS(final DataTypeAdapter<?> adapter) {
    final FieldDescriptor<?> fieldDescriptor = adapter.getFieldDescriptor(fieldName);
    if ((fieldDescriptor != null) && (fieldDescriptor instanceof SpatialFieldDescriptor)) {
      return ((SpatialFieldDescriptor<?>) fieldDescriptor).crs();
    }
    return GeometryUtils.getDefaultCRS();
  }

  public static SpatialFieldValue of(final String fieldName) {
    return new SpatialFieldValue(fieldName);
  }

  @Override
  protected FilterGeometry evaluateValueInternal(final Object value) {
    return new UnpreparedFilterGeometry((Geometry) value);
  }

}
