/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.dimension;

import javax.annotation.Nullable;
import org.locationtech.geowave.core.geotime.index.dimension.LongitudeDefinition;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.data.NumericData;

/**
 * This field can be used as a EPSG:4326 longitude dimension within GeoWave. It can utilize JTS
 * geometry as the underlying spatial object for this dimension.
 */
public class LongitudeField extends SpatialField {
  public LongitudeField() {}

  public LongitudeField(final @Nullable Integer geometryPrecision) {
    this(geometryPrecision, GeometryWrapper.DEFAULT_GEOMETRY_FIELD_NAME);
  }

  public LongitudeField(final @Nullable Integer geometryPrecision, final String fieldName) {
    this(new LongitudeDefinition(), geometryPrecision, fieldName);
  }

  public LongitudeField(
      final NumericDimensionDefinition baseDefinition,
      final @Nullable Integer geometryPrecision,
      final String fieldName) {
    super(baseDefinition, geometryPrecision, fieldName);
  }

  @Override
  public NumericData getNumericData(final GeometryWrapper geometry) {
    return GeometryUtils.xRangeFromGeometry(geometry.getGeometry());
  }
}
