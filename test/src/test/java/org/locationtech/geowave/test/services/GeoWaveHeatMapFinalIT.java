/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 * 
 * @author Milla Zagorski
 *
 *         <p> See the NOTICE file distributed with this work for additional information regarding
 *         copyright ownership. All rights reserved. This program and the accompanying materials are
 *         made available under the terms of the Apache License, Version 2.0 which accompanies this
 *         distribution and is available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.services;


import static org.junit.Assert.assertTrue;
import java.awt.geom.Point2D;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.vector.HeatmapProcess;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.plugin.GeoWaveHeatMap;
import org.locationtech.geowave.adapter.vector.plugin.GeoWaveHeatMapFinal;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.util.ProgressListener;

public class GeoWaveHeatMapFinalIT {

  /**
   * A test of a simple surface, validating that the process can be invoked and return a reasonable
   * result in a simple situation.
   *
   * <p>Test includes data which lies outside the heatmap buffer area, to check that it is filtered
   * correctly (i.e. does not cause out-of-range errors, and does not affect generated surface).
   * 
   * @author Milla Zagorski
   * @apiNode Note: based on the GeoTools version of HeatmapProcess integration test by Martin Davis
   *          - OpenGeo.
   * @apiNote Date: 3-25-2022 <br>
   *
   * @apiNote Changelog: <br>
   * 
   * 
   */
  @Test
  public void testSimpleSurface() {
    System.out.println("STARTING SIMPLE SURFACE TEST - GeoWaveHeatMapFinalIT.java");

    ReferencedEnvelope bounds = new ReferencedEnvelope(0, 10, 0, 10, DefaultGeographicCRS.WGS84);
    Coordinate[] data =
        new Coordinate[] {
            new Coordinate(4, 4),
            new Coordinate(4, 6),
            // include a coordinate outside the heatmap buffer bounds, to ensure it is
            // filtered correctly
            new Coordinate(100, 100)};
    SimpleFeatureCollection fc = createPoints(data, bounds);

    ProgressListener monitor = null;

    // HeatmapProcess process = new HeatmapProcess(); // changed this to the GeoWaveHeatMap
    // GeoWaveHeatMap process = new GeoWaveHeatMap(); //Baseline tests pass
    GeoWaveHeatMapFinal process = new GeoWaveHeatMapFinal(); // Baseline tests pass

    GridCoverage2D cov =
        process.execute(
            fc, // data
            20, // radius
            null, // weightAttr
            1, // pixelsPerCell
            bounds, // outputEnv
            100, // outputWidth
            100, // outputHeight
            "CNT_AGGR", // queryType
            false, // createStats
            monitor // monitor)
        );

    // following tests are checking for an appropriate shape for the surface

    float center1 = coverageValue(cov, 4, 4);
    float center2 = coverageValue(cov, 4, 6);
    float midway = coverageValue(cov, 4, 5);
    float far = coverageValue(cov, 9, 9);

    // peaks are roughly equal
    float peakDiff = Math.abs(center1 - center2);
    assert (peakDiff < center1 / 10);

    // dip between peaks
    assertTrue(midway > center1 / 2);

    // surface is flat far away
    assertTrue(far < center1 / 1000);
  }

  private float coverageValue(GridCoverage2D cov, double x, double y) {
    System.out.println("STARTING COVERAGE VALUE");

    float[] covVal = new float[1];
    Point2D worldPos = new Point2D.Double(x, y);
    cov.evaluate(worldPos, covVal);
    return covVal[0];
  }

  private SimpleFeatureCollection createPoints(Coordinate[] pts, ReferencedEnvelope bounds) {
    System.out.println("STARTING CREATE POINTS");

    SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
    tb.setName("data");
    tb.setCRS(bounds.getCoordinateReferenceSystem());
    tb.add("shape", MultiPoint.class);
    tb.add("value", Double.class);

    SimpleFeatureType type = tb.buildFeatureType();
    SimpleFeatureBuilder fb = new SimpleFeatureBuilder(type);
    DefaultFeatureCollection fc = new DefaultFeatureCollection();

    GeometryFactory factory = new GeometryFactory(new PackedCoordinateSequenceFactory());

    for (Coordinate p : pts) {
      Geometry point = factory.createPoint(p);
      fb.add(point);
      fb.add(p.getZ());
      fc.add(fb.buildFeature(null));
    }

    return fc;
  }
}
