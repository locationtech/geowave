/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.dimension;

import javax.annotation.Nullable;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.data.NumericData;

public class CustomCRSSpatialField extends SpatialField {
  public CustomCRSSpatialField() {}

  public CustomCRSSpatialField(final @Nullable Integer geometryPrecision) {
    super(geometryPrecision);
  }

  public CustomCRSSpatialField(
      final CustomCRSSpatialDimension baseDefinition,
      final @Nullable Integer geometryPrecision) {
    this(baseDefinition, geometryPrecision, GeometryWrapper.DEFAULT_GEOMETRY_FIELD_NAME);
  }

  public CustomCRSSpatialField(
      final NumericDimensionDefinition baseDefinition,
      final @Nullable Integer geometryPrecision,
      final String fieldName) {
    super(baseDefinition, geometryPrecision, fieldName);
  }

  @Override
  public NumericData getNumericData(final GeometryWrapper geometry) {
    // TODO if this can be generalized to n-dimensional that would be better
    if (((CustomCRSSpatialDimension) baseDefinition).getAxis() == 0) {
      return GeometryUtils.xRangeFromGeometry(geometry.getGeometry());
    }
    return GeometryUtils.yRangeFromGeometry(geometry.getGeometry());
  }
}
