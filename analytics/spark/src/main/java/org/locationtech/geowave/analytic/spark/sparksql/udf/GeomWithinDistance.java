/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.sparksql.udf;

import org.locationtech.jts.geom.Geometry;

public class GeomWithinDistance extends GeomFunction {

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private double radius;

  public GeomWithinDistance() {
    radius = 0.01;
  }

  public GeomWithinDistance(final double radius) {
    this.radius = radius;
  }

  @Override
  public double getBufferAmount() {
    return radius;
  }

  public double getRadius() {
    return radius;
  }

  public void setRadius(final double radius) {
    this.radius = radius;
  }

  @Override
  public boolean apply(final Geometry geom1, final Geometry geom2) {
    return geom1.distance(geom2) <= radius;
  }
}
