/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.sparksql.udt;

import org.locationtech.jts.geom.Point;

/** Created by jwileczek on 7/20/18. */
public class PointUDT extends AbstractGeometryUDT<Point> {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  @Override
  public Class<Point> userClass() {
    return Point.class;
  }
}
