/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.aggregate;

import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.jts.geom.Geometry;

public class SpatialFieldBinningStrategy<T> extends SpatialBinningStrategy<T> {

  /**
   * Create a binning strategy using a small number of bins. Usage of this method is not
   * recommended, if you are to use this, it should be through serialization.
   */
  public SpatialFieldBinningStrategy() {
    this(SpatialBinningType.S2, 3, true, null);
  }

  /**
   * @param type S2, H3, or GeoHash
   * @param precision the resolution/length of the hash
   * @param useCentroidOnly desired behavior for complex geometry such as lines and polygons whether
   *        to just aggregate one hash value based on the centroid or to apply the aggregation to
   *        all overlapping centroids
   * @param geometryFieldName the geometry field to bin on
   */
  public SpatialFieldBinningStrategy(
      final SpatialBinningType type,
      final int precision,
      final boolean useCentroidOnly,
      final String geometryFieldName) {
    super(type, precision, useCentroidOnly, geometryFieldName);
  }

  @Override
  Geometry getGeometry(DataTypeAdapter<T> adapter, T entry) {
    final Object obj = adapter.getFieldValue(entry, geometryFieldName);
    if (obj != null && obj instanceof Geometry) {
      return (Geometry) obj;
    }
    return null;
  }

}
