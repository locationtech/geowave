/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.aggregate;

import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;


/**
 * A GeohashBinningStrategy that bins SimpleFeature values.
 *
 * @see GeohashBinningStrategy
 */
public class GeohashSimpleFeatureBinningStrategy extends GeohashBinningStrategy<SimpleFeature> {

  /**
   * Create a binning strategy using a small number of bins. Usage of this method is not
   * recommended, if you are to use this, it should be through serialization.
   */
  public GeohashSimpleFeatureBinningStrategy() {
    this(2);
  }

  /**
   * @see GeohashBinningStrategy#GeohashBinningStrategy(int)
   */
  public GeohashSimpleFeatureBinningStrategy(int precision) {
    super(precision);
  }

  @Override
  public Geometry getGeometry(SimpleFeature entry) {
    return (Geometry) entry.getDefaultGeometry();
  }
}
