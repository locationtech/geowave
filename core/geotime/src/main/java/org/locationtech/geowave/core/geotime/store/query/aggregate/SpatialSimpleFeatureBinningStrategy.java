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
import org.opengis.feature.simple.SimpleFeature;


/**
 * A GeohashBinningStrategy that bins SimpleFeature values.
 *
 * @see SpatialBinningStrategy
 */
public class SpatialSimpleFeatureBinningStrategy extends SpatialBinningStrategy<SimpleFeature> {

  /**
   * Create a binning strategy using a small number of bins. Usage of this method is not
   * recommended, if you are to use this, it should be through serialization.
   */
  public SpatialSimpleFeatureBinningStrategy() {
    this(SpatialBinningType.S2, 3, true);
  }

  /**
   * @param type S2, H3, or GeoHash
   * @param precision the resolution/length of the hash
   * @param useCentroidOnly desired behavior for complex geometry such as lines and polygons whether
   *        to just aggregate one hash value based on the centroid or to apply the aggregation to
   *        all overlapping centroids
   */
  public SpatialSimpleFeatureBinningStrategy(
      final SpatialBinningType type,
      final int precision,
      final boolean useCentroidOnly) {
    super(type, precision, useCentroidOnly, null);
  }

  @Override
  public Geometry getGeometry(
      final DataTypeAdapter<SimpleFeature> adapter,
      final SimpleFeature entry) {
    return (Geometry) entry.getDefaultGeometry();
  }
}
