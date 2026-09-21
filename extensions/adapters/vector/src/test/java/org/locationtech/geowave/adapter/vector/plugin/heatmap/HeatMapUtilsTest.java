/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin.heatmap;

import static org.junit.Assert.assertEquals;
import org.geotools.filter.FilterFactoryImpl;
import org.junit.Ignore;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.spatial.BBOX;
import org.locationtech.geowave.adapter.vector.plugin.GeoWaveFeatureCollection;

/**
 * Unit tests for HeatMapUtils. <br>
 * 
 * @author M. Zagorski <br>
 * @apiNote Date: 5-10-2022 <br>
 * 
 * @apiNote Changelog: <br>
 *
 */

public class HeatMapUtilsTest {
  final GeometryFactory factory = new GeometryFactory(new PrecisionModel(PrecisionModel.FIXED));

  @Test
  public void getGeohashPrecisionTest() {
    assertEquals(1, HeatMapUtils.getGeohashPrecision(10000000));
    assertEquals(2, HeatMapUtils.getGeohashPrecision(500000));
    assertEquals(3, HeatMapUtils.getGeohashPrecision(15000));
    assertEquals(4, HeatMapUtils.getGeohashPrecision(500));
    assertEquals(5, HeatMapUtils.getGeohashPrecision(15));
    assertEquals(6, HeatMapUtils.getGeohashPrecision(1));
    assertEquals(7, HeatMapUtils.getGeohashPrecision(0.01));
    assertEquals(8, HeatMapUtils.getGeohashPrecision(0.0005));
    assertEquals(9, HeatMapUtils.getGeohashPrecision(0.00002));
    assertEquals(10, HeatMapUtils.getGeohashPrecision(0.000005));
    assertEquals(11, HeatMapUtils.getGeohashPrecision(0.00000002));
    assertEquals(12, HeatMapUtils.getGeohashPrecision(0.00000001));
  }

  @Test
  public void getExtentCellCountTest() {
    assertEquals(3312, HeatMapUtils.getExtentCellCount(920, 360, 10));
  }

  @Test
  public void getCellAreaTest() {
    assertEquals(200, Math.round(HeatMapUtils.getCellArea(20000, 100)));
  }

  @SuppressWarnings("static-access")
  @Ignore
  @Test
  public void buildSimpleFeatureTest() {

    final FilterFactoryImpl factory = new FilterFactoryImpl();
    BBOX bb = factory.bbox("geometry", 28, 41, 28.5, 41.5, "EPSG:4326");

    String geohashID = "9trg";
    byte[] geohashId = geohashID.getBytes();
    final Double weightVal = 2.0;
    final Integer geohashPrec = 4;
    final String weightAttr = "SIZE";

    ByteArray geohashIdB = new ByteArray();
    geohashIdB.fromBytes(geohashId);

    SimpleFeature simpFeature =
        HeatMapUtils.buildSimpleFeature(
            GeoWaveFeatureCollection.getHeatmapFeatureType(),
            geohashIdB,
            weightVal,
            geohashPrec,
            weightAttr,
            "CNT_STATS");
  }

}
