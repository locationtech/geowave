/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.distance;

import org.opengis.feature.simple.SimpleFeature;

public class FeatureCentroidOrthodromicDistanceFn extends FeatureCentroidDistanceFn implements
    DistanceFn<SimpleFeature> {

  private static final long serialVersionUID = -9077135292765517738L;

  public FeatureCentroidOrthodromicDistanceFn() {
    super(new CoordinateCircleDistanceFn());
  }
}
