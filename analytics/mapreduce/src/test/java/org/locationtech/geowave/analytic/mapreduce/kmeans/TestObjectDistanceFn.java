/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.kmeans;

import org.locationtech.geowave.analytic.distance.CoordinateEuclideanDistanceFn;
import org.locationtech.geowave.analytic.distance.DistanceFn;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

public class TestObjectDistanceFn implements DistanceFn<TestObject> {
  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private final DistanceFn<Coordinate> coordinateDistanceFunction =
      new CoordinateEuclideanDistanceFn();

  private Geometry getGeometry(final TestObject x) {
    return x.geo;
  }

  @Override
  public double measure(final TestObject x, final TestObject y) {

    return coordinateDistanceFunction.measure(
        getGeometry(x).getCentroid().getCoordinate(),
        getGeometry(y).getCentroid().getCoordinate());
  }
}
