/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.util;

import static org.junit.Assert.assertEquals;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.referencing.operation.MathTransform;
import org.geotools.referencing.CRS;
import org.geotools.referencing.CRS.AxisOrder;
import org.junit.Test;

/**
 * GeoServer's gs-main is on this module's classpath, and it registers an operation factory that
 * needs an EPSG operation authority, which core/geotime supplies.
 */
public class EpsgTransformTest {

  @Test
  public void testTransformBetweenEpsgCodes() throws Exception {
    final CoordinateReferenceSystem wgs84 = CRS.decode("EPSG:4326", true);
    final CoordinateReferenceSystem webMercator = CRS.decode("EPSG:3857", true);
    final MathTransform transform = CRS.findMathTransform(wgs84, webMercator, true);
    final double[] projected = new double[2];
    transform.transform(new double[] {-77.0365, 38.8977}, 0, projected, 0, 1);
    assertEquals(-8575663.9525, projected[0], 1e-3);
    assertEquals(4707028.5508, projected[1], 1e-3);
  }

  @Test
  public void testEpsg4326IsLongitudeFirst() throws Exception {
    assertEquals(AxisOrder.EAST_NORTH, CRS.getAxisOrder(CRS.decode("EPSG:4326")));
    assertEquals(AxisOrder.EAST_NORTH, CRS.getAxisOrder(CRS.decode("EPSG:4326", true)));
  }
}
