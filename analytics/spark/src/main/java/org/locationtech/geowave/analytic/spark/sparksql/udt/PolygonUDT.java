/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.sparksql.udt;

import org.locationtech.jts.geom.Polygon;

/** Created by jwileczek on 7/20/18. */
public class PolygonUDT extends AbstractGeometryUDT<Polygon> {
  /**
   *
   */
  private static final long serialVersionUID = 1L;

  @Override
  public Class<Polygon> userClass() {
    return Polygon.class;
  }
}
