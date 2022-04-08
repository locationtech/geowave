/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.dimension;

import java.util.Set;
import javax.annotation.Nullable;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.IndexDimensionHint;
import org.locationtech.geowave.core.index.numeric.NumericData;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import com.google.common.collect.Sets;

public class CustomCRSSpatialField extends SpatialField {
  public CustomCRSSpatialField() {}

  public CustomCRSSpatialField(
      final CustomCRSSpatialDimension baseDefinition,
      final @Nullable Integer geometryPrecision,
      final @Nullable CoordinateReferenceSystem crs) {
    super(baseDefinition, geometryPrecision, crs);
  }

  @Override
  public NumericData getNumericData(final Geometry geometry) {
    // TODO if this can be generalized to n-dimensional that would be better
    if (((CustomCRSSpatialDimension) baseDefinition).getAxis() == 0) {
      return GeometryUtils.xRangeFromGeometry(geometry);
    }
    return GeometryUtils.yRangeFromGeometry(geometry);
  }

  @Override
  public Set<IndexDimensionHint> getDimensionHints() {
    if (((CustomCRSSpatialDimension) baseDefinition).getAxis() == 0) {
      return Sets.newHashSet(SpatialField.LONGITUDE_DIMENSION_HINT);
    }
    return Sets.newHashSet(SpatialField.LATITUDE_DIMENSION_HINT);
  }
}
