/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.filter;

import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter.GeometryImage;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.util.GeometricShapeFactory;

/**
 * SpatialQueryFilter interns one PreparedGeometry for every filter deserialized from the same
 * bytes, so a query's threads can all make its first, index-building call at once. In JTS 1.18.1 a
 * thread that checked the lazily built interval index while another thread was building it could
 * skip the index and place an interior point outside the polygon.
 */
public class SharedPreparedGeometryTest {
  private static final int THREADS = 8;
  private static final int ROUNDS = 2000;

  @Test
  public void testFirstUseFromManyThreadsAgrees() throws Exception {
    final GeometricShapeFactory shapes = new GeometricShapeFactory(GeometryUtils.GEOMETRY_FACTORY);
    shapes.setNumPoints(500);
    shapes.setCentre(new Coordinate(0, 0));
    shapes.setSize(10);
    final Polygon circle = shapes.createCircle();
    final Point[] interiorPoints = new Point[THREADS];
    for (int t = 0; t < THREADS; t++) {
      interiorPoints[t] =
          GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(0.1 * t, 0.05 * t));
    }
    final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    try {
      int outside = 0;
      for (int r = 0; r < ROUNDS; r++) {
        // a geometry no earlier round interned, so its index is not built yet
        final GeometryImage image =
            new GeometryImage(
                GeometryUtils.geometryToBinary(
                    AffineTransformation.translationInstance(r * 1e-6, 0).transform(circle),
                    null));
        image.init();
        final PreparedGeometry shared = image.getGeometry();
        final CyclicBarrier barrier = new CyclicBarrier(THREADS);
        final List<Future<Boolean>> results = new ArrayList<>();
        for (final Point point : interiorPoints) {
          results.add(pool.submit(() -> {
            barrier.await();
            return shared.intersects(point);
          }));
        }
        for (final Future<Boolean> result : results) {
          if (!result.get(30, TimeUnit.SECONDS)) {
            outside++;
          }
        }
      }
      assertEquals("interior points found outside the polygon", 0, outside);
    } finally {
      pool.shutdownNow();
    }
  }
}
