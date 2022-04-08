/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.adapter;

import org.locationtech.geowave.core.geotime.store.dimension.SpatialField;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.adapter.FieldDescriptorBuilder;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * A field descriptor builder that includes helper functions for spatial indexing hints and
 * `CoordinateReferenceSystem`.
 *
 * @param <T> the adapter field type
 */
public class SpatialFieldDescriptorBuilder<T> extends
    FieldDescriptorBuilder<T, SpatialFieldDescriptor<T>, SpatialFieldDescriptorBuilder<T>> {

  protected CoordinateReferenceSystem crs = GeometryUtils.getDefaultCRS();

  public SpatialFieldDescriptorBuilder(final Class<T> bindingClass) {
    super(bindingClass);
  }

  /**
   * Hint that the field contains both latitude and longitude information and should be used in
   * spatial indexing.
   * 
   * @return the spatial field descriptor builder
   */
  public SpatialFieldDescriptorBuilder<T> spatialIndexHint() {
    return this.indexHint(SpatialField.LONGITUDE_DIMENSION_HINT).indexHint(
        SpatialField.LATITUDE_DIMENSION_HINT);
  }

  /**
   * Hint that the field contains latitude information and should be used in spatial indexing.
   * 
   * @return the spatial field descriptor builder
   */
  public SpatialFieldDescriptorBuilder<T> latitudeIndexHint() {
    return this.indexHint(SpatialField.LATITUDE_DIMENSION_HINT);
  }

  /**
   * Hint that the field contains longitude information and should be used in spatial indexing.
   * 
   * @return the spatial field descriptor builder
   */
  public SpatialFieldDescriptorBuilder<T> longitudeIndexHint() {
    return this.indexHint(SpatialField.LONGITUDE_DIMENSION_HINT);
  }

  /**
   * Specify the coordinate reference system of the spatial field.
   * 
   * @return the spatial field descriptor builder
   */
  public SpatialFieldDescriptorBuilder<T> crs(final CoordinateReferenceSystem crs) {
    this.crs = crs;
    return this;
  }

  @Override
  public SpatialFieldDescriptor<T> build() {
    return new SpatialFieldDescriptor<>(bindingClass, fieldName, indexHints, crs);
  }
}
