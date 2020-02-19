/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.sparksql.udf;

import org.apache.spark.sql.api.java.UDF2;
import org.locationtech.jts.geom.Geometry;

public class GeomDistance implements UDF2<Geometry, Geometry, Double> {
  /**
   *
   */
  private static final long serialVersionUID = 1L;

  @Override
  public Double call(final Geometry leftGeom, final Geometry rightGeom) throws Exception {
    return leftGeom.distance(rightGeom);
  }
}
