/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.sparksql.udt;

import org.apache.spark.sql.types.DataType;
import org.locationtech.jts.geom.Geometry;

/** Created by jwileczek on 7/20/18. */
public class GeometryUDT extends AbstractGeometryUDT<Geometry> {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  @Override
  public boolean acceptsType(final DataType dataType) {
    return super.acceptsType(dataType)
        || (dataType.getClass() == GeometryUDT.class)
        || (dataType.getClass() == PointUDT.class)
        || (dataType.getClass() == LineStringUDT.class)
        || (dataType.getClass() == PolygonUDT.class)
        || (dataType.getClass() == MultiLineStringUDT.class)
        || (dataType.getClass() == MultiPointUDT.class)
        || (dataType.getClass() == MultiPolygonUDT.class);
  }

  @Override
  public Class<Geometry> userClass() {
    return Geometry.class;
  }
}
