/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.binning;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;

/**
 * Covers the properties that H3 binning is expected to hold, independent of the H3 library version.
 *
 * <p> Under h3 3.x these could not all be satisfied: {@code polyfill} returned only cells whose
 * centre fell inside the polygon, so coverage was an under-approximation that was compensated for by
 * buffering the input by an approximate edge length. h3 4.x's
 * {@code polygonToCellsExperimental(..., containment_overlapping)} returns every cell the polygon
 * touches, so binning is now both sound and complete and the buffering is gone.
 */
public class H3BinningHelperTest {

  // The Washington Monument, a convenient fixed point well away from any cell boundary.
  private static final Point POINT =
      GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(-77.0365, 38.8977));

  private static final Geometry TRIANGLE =
      GeometryUtils.GEOMETRY_FACTORY.createPolygon(
          new Coordinate[] {
              new Coordinate(33, 33),
              new Coordinate(34, 34),
              new Coordinate(33, 34),
              new Coordinate(33, 33)});

  private static final Geometry WITH_HOLE =
      GeometryUtils.GEOMETRY_FACTORY.createPolygon(
          GeometryUtils.GEOMETRY_FACTORY.createLinearRing(
              new Coordinate[] {
                  new Coordinate(33, 33),
                  new Coordinate(33, 34),
                  new Coordinate(34, 34),
                  new Coordinate(34, 33),
                  new Coordinate(33, 33)}),
          new LinearRing[] {
              GeometryUtils.GEOMETRY_FACTORY.createLinearRing(
                  new Coordinate[] {
                      new Coordinate(33.25, 33.25),
                      new Coordinate(33.75, 33.25),
                      new Coordinate(33.75, 33.75),
                      new Coordinate(33.25, 33.75),
                      new Coordinate(33.25, 33.25)})});

  private static final Geometry LINE =
      GeometryUtils.GEOMETRY_FACTORY.createLineString(
          new Coordinate[] {
              new Coordinate(33, 33),
              new Coordinate(33, 34),
              new Coordinate(34, 34),
              new Coordinate(34, 33)});

  /**
   * Bin ids are the on-disk key space for every stored statistic, so a change here silently
   * invalidates existing data rather than failing loudly. These are the canonical H3 indexes for the
   * fixture point and must not drift across library upgrades.
   */
  @Test
  public void cellIdsAreStable() {
    assertEquals(0x802bfffffffffffL, singleCell(POINT, 0));
    assertEquals(0x852aa847fffffffL, singleCell(POINT, 5));
    assertEquals(0x892aa845a1bffffL, singleCell(POINT, 9));
  }

  /** A point falls in exactly one cell, and that cell's geometry contains it. */
  @Test
  public void pointBinsToTheCellContainingIt() {
    for (final int precision : new int[] {0, 5, 9}) {
      final ByteArray[] bins = SpatialBinningType.H3.getSpatialBins(POINT, precision);
      assertEquals("a point should produce exactly one bin", 1, bins.length);
      assertTrue(
          "the bin geometry should contain the point it was derived from",
          SpatialBinningType.H3.getBinGeometry(bins[0], precision).contains(POINT));
    }
  }

  /**
   * getBinGeometry closes the ring by appending the first vertex, which requires the boundary list
   * returned by the H3 library to be mutable. A library that returned an immutable list would throw
   * here rather than fail subtly later.
   */
  @Test
  public void binGeometryIsAClosedHexagon() {
    final ByteArray bin = SpatialBinningType.H3.getSpatialBins(POINT, 9)[0];
    final Geometry geom = SpatialBinningType.H3.getBinGeometry(bin, 9);
    assertTrue("bin geometry should be valid", geom.isValid());
    assertEquals("six vertices plus the closing point", 7, geom.getCoordinates().length);
  }

  /** Soundness: no bin is returned that the input does not actually touch. */
  @Test
  public void everyBinIntersectsTheInput() {
    for (final Geometry geom : new Geometry[] {TRIANGLE, WITH_HOLE, LINE}) {
      for (int precision = 1; precision < 7; precision++) {
        for (final ByteArray bin : SpatialBinningType.H3.getSpatialBins(geom, precision)) {
          assertTrue(
              String.format(
                  "bin does not intersect input at precision %d for %s",
                  precision,
                  geom.getGeometryType()),
              SpatialBinningType.H3.getBinGeometry(bin, precision).intersects(geom));
        }
      }
    }
  }

  /**
   * Completeness: the bins together cover the whole input. This is the property h3 3.x's polyfill
   * did not provide -- it returned only cells whose centre was inside the polygon, leaving edges
   * uncovered.
   */
  @Test
  public void binsFullyCoverTheInput() {
    for (final Geometry geom : new Geometry[] {TRIANGLE, WITH_HOLE, LINE}) {
      for (int precision = 1; precision < 7; precision++) {
        Geometry union = null;
        for (final ByteArray bin : SpatialBinningType.H3.getSpatialBins(geom, precision)) {
          final Geometry binGeom = SpatialBinningType.H3.getBinGeometry(bin, precision);
          union = (union == null) ? binGeom : union.union(binGeom);
        }
        final double uncovered =
            (geom.getDimension() == 2) ? geom.difference(union).getArea()
                : geom.difference(union).getLength();
        assertEquals(
            String.format(
                "input not fully covered at precision %d for %s",
                precision,
                geom.getGeometryType()),
            0d,
            uncovered,
            1E-9);
      }
    }
  }

  private static long singleCell(final Geometry geom, final int precision) {
    final ByteArray[] bins = SpatialBinningType.H3.getSpatialBins(geom, precision);
    assertEquals(1, bins.length);
    return Lexicoders.LONG.fromByteArray(bins[0].getBytes());
  }
}
