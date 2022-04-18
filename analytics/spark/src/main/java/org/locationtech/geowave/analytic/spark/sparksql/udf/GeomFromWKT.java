/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.sparksql.udf;

import org.apache.spark.sql.api.java.UDF1;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;

/** Created by jwileczek on 8/16/18. */
public class GeomFromWKT implements UDF1<String, Geometry> {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  @Override
  public Geometry call(final String o) throws Exception {
    return new WKTReader().read(o);
  }
}
