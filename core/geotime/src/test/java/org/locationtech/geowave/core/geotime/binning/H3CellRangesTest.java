/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.binning;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.store.statistics.binning.SpatialFieldValueBinningStrategy;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.geom.util.GeometryFixer;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import com.uber.h3core.AreaUnit;
import com.uber.h3core.H3Core;
import com.uber.h3core.LengthUnit;
import com.uber.h3core.util.LatLng;

public class H3CellRangesTest {
  private static final H3Core H3 = H3CellRanges.h3();
  private static final Comparator<byte[]> BYTE_ORDER = UnsignedBytes.lexicographicalComparator();
  private static final long SEED = 20261027L;
  // H3's earth radius, in km per degree of arc
  private static final double KM_PER_DEGREE = Math.toRadians(6371.007180918475);

  private static final LatLng[] PLACES =
      new LatLng[] {
          new LatLng(38.8977, -77.0365),
          new LatLng(-33.8568, 151.2153),
          new LatLng(51.5007, -0.1246),
          new LatLng(0.0001, 0.0001),
          new LatLng(-54.8019, -68.303)};

  private static final Geometry TRIANGLE =
      polygon(new double[][] {{33, 33}, {34, 34}, {33, 34}, {33, 33}});

  private static final Geometry SMALL_QUAD =
      polygon(new double[][] {{0.5, 0.6}, {0.7, 0.8}, {1, 0.9}, {0.8, 0.7}, {0.5, 0.6}});

  private static final Geometry WITH_HOLE =
      GeometryUtils.GEOMETRY_FACTORY.createPolygon(
          ring(new double[][] {{33, 33}, {33, 34}, {34, 34}, {34, 33}, {33, 33}}),
          new LinearRing[] {
              ring(
                  new double[][] {
                      {33.25, 33.25},
                      {33.75, 33.25},
                      {33.75, 33.75},
                      {33.25, 33.75},
                      {33.25, 33.25}})});

  private static final Geometry LINE =
      GeometryUtils.GEOMETRY_FACTORY.createLineString(
          new Coordinate[] {
              new Coordinate(33, 33),
              new Coordinate(33, 34),
              new Coordinate(34, 34),
              new Coordinate(34, 33)});

  // far smaller than a resolution 3 cell, so the polyfill is empty and the centroid is used
  private static final Geometry TINY =
      polygon(
          new double[][] {
              {10, 10},
              {10.000001, 10},
              {10.000001, 10.000001},
              {10, 10.000001},
              {10, 10}});

  // split at the antimeridian, as a geometry crossing it has to be in planar coordinates
  private static final Geometry ANTIMERIDIAN =
      GeometryUtils.GEOMETRY_FACTORY.createMultiPolygon(
          new Polygon[] {
              polygon(
                  new double[][] {
                      {179.5, -0.5},
                      {180, -0.5},
                      {180, 0.5},
                      {179.5, 0.5},
                      {179.5, -0.5}}),
              polygon(
                  new double[][] {
                      {-180, -0.5},
                      {-179.5, -0.5},
                      {-179.5, 0.5},
                      {-180, 0.5},
                      {-180, -0.5}})});

  private static final Geometry NORTH_CAP =
      polygon(new double[][] {{-10, 89}, {10, 89}, {10, 90}, {-10, 90}, {-10, 89}});

  private static final Geometry SOUTH_CAP =
      polygon(new double[][] {{-10, -90}, {10, -90}, {10, -89}, {-10, -89}, {-10, -90}});

  private static final DataTypeAdapter<Shape> SHAPES =
      BasicDataTypeAdapter.newAdapter("Shape", Shape.class, "id");

  public static class Shape {
    private String id;
    private Geometry geometry;

    public Shape() {}

    public Shape(final Geometry geometry) {
      id = "shape";
      this.geometry = geometry;
    }

    public String getId() {
      return id;
    }

    public void setId(final String id) {
      this.id = id;
    }

    public Geometry getGeometry() {
      return geometry;
    }

    public void setGeometry(final Geometry geometry) {
      this.geometry = geometry;
    }
  }

  @Test
  public void descendantBoundsAreTheChildIdExtremes() {
    final int[][] resolutionPairs =
        new int[][] {{0, 0}, {0, 1}, {0, 3}, {2, 2}, {2, 5}, {5, 8}, {9, 9}, {12, 15}, {15, 15}};
    for (final int[] pair : resolutionPairs) {
      final int cellResolution = pair[0];
      final int resolution = pair[1];
      final List<Long> cells = new ArrayList<>(H3.getPentagons(cellResolution));
      for (final LatLng place : PLACES) {
        cells.add(H3.latLngToCell(place.lat, place.lng, cellResolution));
      }
      for (final long cell : cells) {
        final List<Long> children = H3.cellToChildren(cell, resolution);
        final long first = H3CellRanges.firstDescendant(cell, resolution);
        final long last = H3CellRanges.lastDescendant(cell, resolution);
        final String message =
            String.format("%x from resolution %d to %d", cell, cellResolution, resolution);
        assertEquals(message, (long) Collections.min(children), first);
        assertEquals(message, (long) Collections.max(children), last);
        assertTrue(message, H3.isValidCell(first) && H3.isValidCell(last));
        if (cellResolution == resolution) {
          assertEquals(message, cell, first);
          assertEquals(message, cell, last);
        }
        final ByteArrayRange range = H3CellRanges.descendantRange(cell, resolution);
        assertArrayEquals(message, H3CellRanges.toBin(first).getBytes(), range.getStart());
        assertArrayEquals(message, H3CellRanges.toBin(last).getBytes(), range.getEnd());
      }
    }
  }

  @Test
  public void descendantBoundsRejectResolutionsOutsideTheCellsDescendants() {
    final long cell = H3.latLngToCell(PLACES[0].lat, PLACES[0].lng, 5);
    for (final int resolution : new int[] {-1, 4, 16}) {
      assertRejected(() -> H3CellRanges.firstDescendant(cell, resolution));
      assertRejected(() -> H3CellRanges.lastDescendant(cell, resolution));
      assertRejected(() -> H3CellRanges.descendantRange(cell, resolution));
    }
  }

  @Test
  public void binsSortInCellIdOrder() {
    final Random random = new Random(SEED);
    final List<Long> cells = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      cells.add(
          H3.latLngToCell(
              (random.nextDouble() * 180) - 90,
              (random.nextDouble() * 360) - 180,
              random.nextInt(16)));
    }
    for (final long cell : cells) {
      final byte[] bin = H3CellRanges.toBin(cell).getBytes();
      assertArrayEquals(Lexicoders.LONG.toByteArray(cell), bin);
      assertEquals(cell, H3CellRanges.fromBin(bin));
    }
    final List<Long> byBin =
        cells.stream().sorted(
            (a, b) -> BYTE_ORDER.compare(
                H3CellRanges.toBin(a).getBytes(),
                H3CellRanges.toBin(b).getBytes())).collect(Collectors.toList());
    final List<Long> byId = cells.stream().sorted().collect(Collectors.toList());
    assertEquals(byId, byBin);
  }

  @Test(expected = IllegalArgumentException.class)
  public void toRangesRejectsCellsAtOtherResolutions() {
    H3CellRanges.toRanges(
        Arrays.asList(
            H3.latLngToCell(PLACES[0].lat, PLACES[0].lng, 5),
            H3.latLngToCell(PLACES[1].lat, PLACES[1].lng, 6)),
        5);
  }

  @Test
  public void noCellsMeansNoRanges() {
    assertEquals(0, H3CellRanges.toRanges(Collections.emptyList(), 7).length);
  }

  @Test
  public void contiguousSiblingsMergeIntoOneRange() {
    final List<Long> parents =
        sorted(H3.cellToChildren(H3.latLngToCell(PLACES[0].lat, PLACES[0].lng, 4), 5));
    final List<Long> children = sorted(H3.cellToChildren(parents.get(2), 6));
    assertEquals(1, assertRangesCoverExactly(children.subList(0, 5), 6).length);
    final List<Long> withGap = Arrays.asList(children.get(0), children.get(1), children.get(3));
    assertEquals(2, assertRangesCoverExactly(withGap, 6).length);
    // the last children of one parent are followed in id order by the first of the next parent
    final List<Long> acrossParents = new ArrayList<>(children.subList(4, 7));
    acrossParents.addAll(sorted(H3.cellToChildren(parents.get(3), 6)).subList(0, 2));
    assertEquals(1, assertRangesCoverExactly(acrossParents, 6).length);
  }

  @Test
  public void consecutiveBaseCellsMergeIntoOneRange() {
    final List<Long> baseCells = sorted(H3.getRes0Cells());
    final List<Long> cells = new ArrayList<>(H3.cellToChildren(baseCells.get(0), 2));
    cells.addAll(H3.cellToChildren(baseCells.get(1), 2));
    assertEquals(1, assertRangesCoverExactly(cells, 2).length);
  }

  @Test
  public void polygonCovers() {
    for (final Geometry geometry : new Geometry[] {TRIANGLE, SMALL_QUAD, WITH_HOLE, LINE}) {
      for (final int resolution : new int[] {2, 4, 6}) {
        final ByteArrayRange[] ranges =
            assertRangesCoverExactly(H3CellRanges.coveringCells(geometry, resolution), resolution);
        assertEquals(
            Arrays.asList(ranges),
            Arrays.asList(H3CellRanges.geometryToRanges(geometry, resolution)));
      }
    }
  }

  @Test
  public void pentagonNeighborhoods() {
    for (final long pentagon : H3.getPentagons(4)) {
      assertRangesCoverExactly(H3.gridDisk(pentagon, 2), 4);
    }
    for (final long pentagon : H3.getPentagons(2)) {
      final List<Long> children = sorted(H3.cellToChildren(pentagon, 5));
      assertEquals(1, assertRangesCoverExactly(children, 5).length);
      assertRangesCoverExactly(children.subList(1, children.size()), 5);
      assertRangesCoverExactly(children.subList(0, children.size() - 1), 5);
      final List<Long> missingMiddle = new ArrayList<>(children);
      missingMiddle.remove(children.size() / 2);
      assertRangesCoverExactly(missingMiddle, 5);
    }
  }

  @Test
  public void antimeridianCovers() {
    for (final int resolution : new int[] {2, 5}) {
      final List<Long> disk = H3.gridDisk(H3.latLngToCell(0, 180, resolution), 3);
      assertStraddlesAntimeridian(disk);
      assertRangesCoverExactly(disk, resolution);
    }
    final Set<Long> cover = H3CellRanges.coveringCells(ANTIMERIDIAN, 5);
    assertStraddlesAntimeridian(cover);
    assertRangesCoverExactly(cover, 5);
  }

  @Test
  public void polarCovers() {
    for (final int lat : new int[] {90, -90}) {
      for (final int resolution : new int[] {1, 4, 7}) {
        assertRangesCoverExactly(H3.gridDisk(H3.latLngToCell(lat, 0, resolution), 2), resolution);
      }
    }
    for (final Geometry cap : new Geometry[] {NORTH_CAP, SOUTH_CAP}) {
      final int resolution = 5;
      final Set<Long> cover = H3CellRanges.coveringCells(cap, resolution);
      final double poleLat = cap.getCentroid().getY() > 0 ? 90 : -90;
      assertTrue(cover.contains(H3.latLngToCell(poleLat, 0, resolution)));
      assertRangesCoverExactly(cover, resolution);
    }
  }

  @Test
  public void geometryConstraintsSelectExactlyTheGeometrysBins() {
    final Geometry envelopes =
        GeometryUtils.GEOMETRY_FACTORY.createGeometryCollection(
            new Geometry[] {
                GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(-77.1, -77, 38.8, 38.9)),
                GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(2.2, 2.5, 48.8, 48.9))});
    final Geometry[] geometries =
        new Geometry[] {
            GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(-77.0365, 38.8977)),
            TRIANGLE,
            WITH_HOLE,
            LINE,
            TINY,
            envelopes,
            ANTIMERIDIAN,
            NORTH_CAP};
    for (final Geometry geometry : geometries) {
      for (final int resolution : new int[] {0, 3, 6}) {
        final ByteArrayConstraints constraints =
            SpatialBinningType.H3.getGeometryConstraints(geometry, resolution);
        final Set<Long> bins =
            Arrays.stream(SpatialBinningType.H3.getSpatialBins(geometry, resolution)).map(
                bin -> H3CellRanges.fromBin(bin.getBytes())).collect(Collectors.toSet());
        final String message =
            String.format("%s at resolution %d", geometry.getGeometryType(), resolution);
        assertEquals(message, 0, constraints.getBins().length);
        assertFalse(message, constraints.isAllBins());
        assertFalse(message, bins.isEmpty());
        assertEquals(message, bins, cellsInRanges(constraints.getBinRanges(), resolution));
      }
    }
    final SpatialFieldValueBinningStrategy strategy =
        new SpatialFieldValueBinningStrategy(
            SpatialBinningType.H3,
            6,
            ComplexGeometryBinningOption.USE_FULL_GEOMETRY,
            "geom");
    final Envelope envelope = new Envelope(-77.1, -77, 38.8, 38.9);
    final Set<Long> bins =
        Arrays.stream(
            SpatialBinningType.H3.getSpatialBins(
                GeometryUtils.GEOMETRY_FACTORY.toGeometry(envelope),
                6)).map(ByteArray::getBytes).map(H3CellRanges::fromBin).collect(Collectors.toSet());
    assertEquals(bins, cellsInRanges(strategy.constraints(envelope).getBinRanges(), 6));
  }

  @Test
  public void duplicateCellsAreIgnored() {
    final List<Long> cells = new ArrayList<>(H3CellRanges.coveringCells(TRIANGLE, 4));
    final List<Long> withDuplicates = new ArrayList<>(cells);
    withDuplicates.addAll(cells.subList(0, cells.size() / 2));
    Collections.shuffle(withDuplicates, new Random(SEED));
    assertEquals(
        Arrays.asList(H3CellRanges.toRanges(new HashSet<>(cells), 4)),
        Arrays.asList(H3CellRanges.toRanges(withDuplicates, 4)));
  }

  @Test
  public void emptyGeometriesCoverNothing() {
    final GeometryFactory factory = GeometryUtils.GEOMETRY_FACTORY;
    final Geometry[] empties =
        new Geometry[] {
            factory.createPolygon(),
            factory.createPoint(),
            factory.createLineString(),
            factory.createMultiPolygon(),
            factory.createGeometryCollection(),
            factory.toGeometry(new Envelope()),
            TRIANGLE.intersection(factory.toGeometry(new Envelope(0, 1, 0, 1)))};
    for (final Geometry empty : empties) {
      assertTrue(empty.isEmpty());
      assertTrue(H3CellRanges.coveringCells(empty, 5).isEmpty());
      assertEquals(0, H3CellRanges.geometryToRanges(empty, 5).length);
      assertEquals(0, SpatialBinningType.H3.getSpatialBins(empty, 5).length);
    }
    final ByteArrayConstraints constraints =
        new SpatialFieldValueBinningStrategy(
            SpatialBinningType.H3,
            5,
            ComplexGeometryBinningOption.USE_FULL_GEOMETRY,
            "geom").constraints(new Envelope());
    assertEquals(0, constraints.getBins().length);
    assertEquals(0, constraints.getBinRanges().length);
  }

  @Test
  public void wideGeometriesMatchABruteForceCover() {
    assertCoverMatchesBruteForce(new Envelope(-100, 100, 0, 10), null, 2);
    assertCoverMatchesBruteForce(new Envelope(-170, 170, -60, 60), null, 2);
    assertCoverMatchesBruteForce(
        new Envelope(-170, 170, -60, 60),
        new Envelope(-20, 20, -10, 10),
        2);
    assertCoverMatchesBruteForce(new Envelope(-180, 180, -85.05, 85.05), null, 1);
    assertCoverMatchesBruteForce(new Envelope(-180, 180, 80, 90), null, 2);
    assertCoverMatchesBruteForce(new Envelope(-180, 180, -90, -80), null, 2);
    // past the antimeridian, outside the usual longitude domain
    assertCoverMatchesBruteForce(new Envelope(170, 190, 0, 10), null, 3);
    for (final int resolution : new int[] {1, 2}) {
      assertEquals(
          new HashSet<>(H3.uncompactCells(H3.getRes0Cells(), resolution)),
          assertCoverMatchesBruteForce(new Envelope(-180, 180, -90, 90), null, resolution));
    }
  }

  @Test
  public void invalidPolygonsAreCoveredAsTheirFixedForm() {
    final Polygon[] polygons =
        new Polygon[] {
            // bow-ties 150 and 200 degrees wide, and one 20 degrees wide past the antimeridian
            polygon(new double[][] {{-75, 0}, {75, 10}, {75, 0}, {-75, 10}, {-75, 0}}),
            polygon(new double[][] {{-100, 0}, {100, 10}, {100, 0}, {-100, 10}, {-100, 0}}),
            polygon(new double[][] {{170, 0}, {190, 10}, {190, 0}, {170, 10}, {170, 0}}),
            // a ring crossing itself
            polygon(
                new double[][] {
                    {-75, 0},
                    {75, 0},
                    {75, 10},
                    {0, 10},
                    {0, -5},
                    {-1, -5},
                    {-1, 10},
                    {-75, 10},
                    {-75, 0}})};
    for (final Polygon polygon : polygons) {
      assertFalse(polygon.isValid());
      final Geometry fixed = GeometryFixer.fix(polygon);
      assertTrue(fixed.getArea() > 0);
      assertBinnedAs(polygon, assertCoverMatchesBruteForce(polygon, fixed, 3), 3);
    }
  }

  @Test
  public void collapsedPolygonsAreCoveredAlongTheirEdges() {
    // H3 itself covers a polygon too narrow to be split along its edges
    for (final double halfWidth : new double[] {40, 100}) {
      final Polygon collapsed =
          polygon(
              new double[][] {{-halfWidth, 0}, {0, 0}, {halfWidth, 0}, {0, 0}, {-halfWidth, 0}});
      final Set<Long> lineCover =
          H3CellRanges.coveringCells(
              GeometryUtils.GEOMETRY_FACTORY.createLineString(
                  new Coordinate[] {new Coordinate(-halfWidth, 0), new Coordinate(halfWidth, 0)}),
              3);
      assertFalse(lineCover.isEmpty());
      assertBinnedAs(collapsed, lineCover, 3);
    }
  }

  @Test
  public void antimeridianCellGeometriesSplitAt180() {
    for (final int resolution : new int[] {3, 8}) {
      final long cell = H3.latLngToCell(0, 180, resolution);
      assertTrue(H3.cellToBoundary(cell).stream().anyMatch(v -> v.lng > 0));
      assertTrue(H3.cellToBoundary(cell).stream().anyMatch(v -> v.lng < 0));
      final Geometry geometry = H3CellRanges.cellToGeometry(cell);
      assertValidAndContainsCenter(cell, geometry);
      assertTrue(geometry instanceof MultiPolygon);
      assertEquals(2, geometry.getNumGeometries());
      final double cosLat = Math.cos(Math.toRadians(H3.cellToLatLng(cell).lat));
      final double expectedArea =
          H3.cellArea(cell, AreaUnit.km2) / (KM_PER_DEGREE * KM_PER_DEGREE * cosLat);
      assertEquals(1, geometry.getArea() / expectedArea, 0.05);
    }
  }

  @Test
  public void polarCellGeometriesCloseThroughThePole() {
    for (final double poleLat : new double[] {90, -90}) {
      for (final int resolution : new int[] {0, 3, 8}) {
        final long cell = H3.latLngToCell(poleLat, 0, resolution);
        final Geometry geometry = H3CellRanges.cellToGeometry(cell);
        assertValidAndContainsCenter(cell, geometry);
        assertTrue(geometry instanceof Polygon);
        final Envelope envelope = geometry.getEnvelopeInternal();
        assertEquals(-180, envelope.getMinX(), 0);
        assertEquals(180, envelope.getMaxX(), 0);
        assertEquals(poleLat, poleLat > 0 ? envelope.getMaxY() : envelope.getMinY(), 0);
        // the band between the pole and the boundary, at every longitude
        final double nearest =
            H3.cellToBoundary(cell).stream().mapToDouble(
                v -> 90 - Math.abs(v.lat)).min().getAsDouble();
        final double farthest =
            H3.cellToBoundary(cell).stream().mapToDouble(
                v -> 90 - Math.abs(v.lat)).max().getAsDouble();
        assertTrue(geometry.getArea() >= (360 * nearest));
        assertTrue(geometry.getArea() <= (360 * farthest));
      }
    }
  }

  @Test
  public void aVertexJustShortOfThePoleIsSplitAtThePole() {
    final List<LatLng> boundary =
        Arrays.asList(
            new LatLng(-89.99999, 10),
            new LatLng(-89.9999999999999, 50),
            new LatLng(-89.9999985, 70),
            new LatLng(-89.999995, 80),
            new LatLng(-89.99999, 90));
    final List<LatLng> split = H3CellRanges.splitPoleVertices(boundary);
    // the vertices at -89.9999985 and -89.9999999999999 split; the one at -89.999995 does not
    assertEquals(7, split.size());
    assertEquals(-90, split.get(1).lat, 0);
    assertEquals(10, split.get(1).lng, 0);
    assertEquals(-90, split.get(2).lat, 0);
    assertEquals(70, split.get(2).lng, 0);
    assertEquals(-90, split.get(3).lat, 0);
    assertEquals(50, split.get(3).lng, 0);
    assertEquals(-90, split.get(4).lat, 0);
    assertEquals(80, split.get(4).lng, 0);
    assertEquals(-89.999995, split.get(5).lat, 0);
  }

  @Test
  public void cellsWithAVertexAtThePoleAreClosedAlongThePole() {
    // the three resolution 15 cells that meet at the south pole, where each has a vertex
    final long[] cells = new long[] {0x8ff29380e0d0cc4L, 0x8ff29380e0d0cc6L, 0x8ff29380e0d0cc0L};
    final List<Geometry> geometries = new ArrayList<>();
    for (final long cell : cells) {
      assertTrue(
          "no pole vertex in " + H3.cellToBoundary(cell),
          H3.cellToBoundary(cell).stream().anyMatch(
              vertex -> Math.abs(vertex.lat + 90) <= H3CellRanges.POLE_TOLERANCE_DEGREES));
      final Geometry geometry = H3CellRanges.cellToGeometry(cell);
      assertValidAndContainsCenter(cell, geometry);
      geometries.add(geometry);
    }
    // this close to the pole, straight lon/lat edges stray from the cell edges by up to about 0.1 m
    // (a sixth of an edge), so only points at least 1e-6 degrees (0.11 m) from another cell count
    final double margin = 1e-6;
    final Random random = new Random(SEED);
    final Set<Long> owners = new HashSet<>();
    for (int i = 0; i < 5000; i++) {
      // uniform over a disk around the pole holding the three cells and their neighbors, in
      // degrees of arc on the pole's azimuthal plane
      final double distance = 1.5e-5 * Math.sqrt(random.nextDouble());
      final double azimuth = random.nextDouble() * 2 * Math.PI;
      final double x = distance * Math.cos(azimuth);
      final double y = distance * Math.sin(azimuth);
      final long owner = southPoleCell(x, y);
      boolean clear = true;
      for (int k = 0; clear && (k < 8); k++) {
        final double angle = (k * Math.PI) / 4;
        clear =
            southPoleCell(x + (margin * Math.cos(angle)), y + (margin * Math.sin(angle))) == owner;
      }
      if (clear) {
        owners.add(owner);
        final Geometry point =
            GeometryUtils.GEOMETRY_FACTORY.createPoint(
                new Coordinate(Math.toDegrees(Math.atan2(y, x)), distance - 90));
        for (int c = 0; c < cells.length; c++) {
          assertEquals(
              String.format("%x covering %s of %x", cells[c], point, owner),
              cells[c] == owner,
              geometries.get(c).covers(point));
        }
      }
    }
    assertTrue(owners.containsAll(Longs.asList(cells)));
  }

  @Test
  public void coarseCellGeometriesAreValid() {
    for (final int resolution : new int[] {0, 1}) {
      for (final long cell : H3.uncompactCells(H3.getRes0Cells(), resolution)) {
        final Geometry geometry = H3CellRanges.cellToGeometry(cell);
        assertValidAndContainsCenter(cell, geometry);
        final Envelope envelope = geometry.getEnvelopeInternal();
        assertTrue(envelope.getMinX() >= -180);
        assertTrue(envelope.getMaxX() <= 180);
      }
    }
    final long ordinary = H3.latLngToCell(PLACES[0].lat, PLACES[0].lng, 9);
    assertEquals(
        H3.cellToBoundary(ordinary).size() + 1,
        H3CellRanges.cellToGeometry(ordinary).getCoordinates().length);
  }

  /**
   * Asserts the ranges for a set of cells select exactly those cells: compacting loses nothing, the
   * ranges are sorted and disjoint, every cell is in a range, and no other valid cell at the
   * resolution is, whether a neighbor, a random nearby cell, or anything the ranges enclose.
   */
  private static ByteArrayRange[] assertRangesCoverExactly(
      final Collection<Long> cells,
      final int resolution) {
    final Set<Long> cellSet = new HashSet<>(cells);
    final ByteArrayRange[] ranges = H3CellRanges.toRanges(cells, resolution);

    final List<Long> compacted = H3.compactCells(cellSet);
    assertEquals(cellSet, new HashSet<>(H3.uncompactCells(compacted, resolution)));
    assertTrue(ranges.length <= compacted.size());
    for (int i = 1; i < ranges.length; i++) {
      assertTrue(BYTE_ORDER.compare(ranges[i - 1].getEnd(), ranges[i].getStart()) < 0);
    }

    for (final long cell : cellSet) {
      assertTrue(String.format("%x should be in a range", cell), inRanges(ranges, cell));
    }
    final Set<Long> outside = new HashSet<>();
    for (final long cell : cellSet) {
      outside.addAll(H3.gridDisk(cell, 2));
    }
    final Random random = new Random(SEED);
    final List<Long> cellList = sorted(cellSet);
    // roughly ten cell edges, in degrees
    final double spread = (10 * H3.getHexagonEdgeLengthAvg(resolution, LengthUnit.km)) / 111;
    for (int i = 0; i < 200; i++) {
      final LatLng center = H3.cellToLatLng(cellList.get(random.nextInt(cellList.size())));
      final double lat = center.lat + ((random.nextDouble() - 0.5) * spread);
      final double lng = center.lng + ((random.nextDouble() - 0.5) * spread);
      outside.add(
          H3.latLngToCell(
              Math.max(-90, Math.min(90, lat)),
              Math.IEEEremainder(lng, 360),
              resolution));
    }
    outside.removeAll(cellSet);
    for (final long cell : outside) {
      assertFalse(String.format("%x should not be in a range", cell), inRanges(ranges, cell));
    }

    assertEquals(cellSet, cellsInRanges(ranges, resolution));
    return ranges;
  }

  /**
   * Asserts the cover of a box, less an optional hole, holds every cell whose center is in it and
   * no cell whose vertices, padded for edge curvature, all lie clearly outside it. Only cell
   * centers and boundaries are used, not a polyfill. Returns the cover.
   */
  private static Set<Long> assertCoverMatchesBruteForce(
      final Envelope shell,
      final Envelope hole,
      final int resolution) {
    final Geometry geometry =
        hole == null ? GeometryUtils.GEOMETRY_FACTORY.toGeometry(shell)
            : GeometryUtils.GEOMETRY_FACTORY.createPolygon(
                ring(shell),
                new LinearRing[] {ring(hole)});
    final Set<Long> cover = H3CellRanges.coveringCells(geometry, resolution);
    int inside = 0;
    for (final long cell : H3.uncompactCells(H3.getRes0Cells(), resolution)) {
      final LatLng center = H3.cellToLatLng(cell);
      final double[] extents = paddedExtents(cell);
      final String message = String.format("%x in %s less %s", cell, shell, hole);
      if (within(shell, center, 0, 0) && ((hole == null) || !within(hole, center, 0, 0))) {
        assertTrue(message + " should be covered", cover.contains(cell));
        inside++;
      } else if (!within(shell, center, -extents[0], -extents[1])
          || ((hole != null) && within(hole, center, extents[0], extents[1]))) {
        assertFalse(message + " should not be covered", cover.contains(cell));
      }
    }
    assertTrue(inside > 0);
    assertEquals(
        cover,
        cellsInRanges(H3CellRanges.geometryToRanges(geometry, resolution), resolution));
    return cover;
  }

  /**
   * Asserts the cover of a geometry holds every cell whose center lies in the area and no cell
   * whose vertices, padded for edge curvature, all lie clearly outside it, comparing longitudes
   * modulo 360. Returns the cover.
   */
  private static Set<Long> assertCoverMatchesBruteForce(
      final Geometry geometry,
      final Geometry area,
      final int resolution) {
    final Set<Long> cover = H3CellRanges.coveringCells(geometry, resolution);
    final List<Geometry> wrapped = new ArrayList<>();
    for (final double shift : new double[] {-360, 0, 360}) {
      wrapped.add(AffineTransformation.translationInstance(shift, 0).transform(area));
    }
    int inside = 0;
    for (final long cell : H3.uncompactCells(H3.getRes0Cells(), resolution)) {
      final LatLng center = H3.cellToLatLng(cell);
      final double[] extents = paddedExtents(cell);
      final Geometry centerPoint =
          GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(center.lng, center.lat));
      final Geometry padded =
          GeometryUtils.GEOMETRY_FACTORY.toGeometry(
              new Envelope(
                  center.lng - extents[1],
                  center.lng + extents[1],
                  center.lat - extents[0],
                  center.lat + extents[0]));
      final String message = String.format("%x in %s", cell, area);
      if (wrapped.stream().anyMatch(part -> part.contains(centerPoint))) {
        assertTrue(message + " should be covered", cover.contains(cell));
        inside++;
      } else if (wrapped.stream().noneMatch(part -> part.intersects(padded))) {
        assertFalse(message + " should not be covered", cover.contains(cell));
      }
    }
    assertTrue(inside > 0);
    return cover;
  }

  /**
   * The cell's latitude and longitude extents from its center, padded for edge curvature. A cell
   * holding a pole spans every longitude.
   */
  private static double[] paddedExtents(final long cell) {
    final LatLng center = H3.cellToLatLng(cell);
    double latExtent = 0;
    double lngExtent = 0;
    for (final LatLng vertex : H3.cellToBoundary(cell)) {
      latExtent = Math.max(latExtent, Math.abs(vertex.lat - center.lat) * 1.1);
      lngExtent =
          Math.max(lngExtent, Math.abs(Math.IEEEremainder(vertex.lng - center.lng, 360)) * 1.1);
    }
    final int resolution = H3.getResolution(cell);
    if ((cell == H3.latLngToCell(90, 0, resolution))
        || (cell == H3.latLngToCell(-90, 0, resolution))) {
      lngExtent = 360;
    }
    return new double[] {latExtent, lngExtent};
  }

  /**
   * Asserts the geometry's bins, its full-geometry H3 strategy's bins and the cells in that
   * strategy's constraints are all the given cells.
   */
  private static void assertBinnedAs(
      final Geometry geometry,
      final Set<Long> cells,
      final int resolution) {
    final SpatialFieldValueBinningStrategy strategy =
        new SpatialFieldValueBinningStrategy(
            SpatialBinningType.H3,
            resolution,
            ComplexGeometryBinningOption.USE_FULL_GEOMETRY,
            "geometry");
    final String message = geometry.toText();
    assertEquals(message, cells, cells(SpatialBinningType.H3.getSpatialBins(geometry, resolution)));
    assertEquals(message, cells, cells(strategy.getBins(SHAPES, new Shape(geometry))));
    final ByteArrayConstraints constraints = strategy.constraints(geometry);
    assertEquals(message, 0, constraints.getBins().length);
    assertEquals(message, cells, cellsInRanges(constraints.getBinRanges(), resolution));
  }

  private static Set<Long> cells(final ByteArray[] bins) {
    return Arrays.stream(bins).map(bin -> H3CellRanges.fromBin(bin.getBytes())).collect(
        Collectors.toSet());
  }

  /** The resolution 15 cell at a point on the south pole's azimuthal plane, in degrees of arc. */
  private static long southPoleCell(final double x, final double y) {
    return H3.latLngToCell(Math.hypot(x, y) - 90, Math.toDegrees(Math.atan2(y, x)), 15);
  }

  /**
   * Whether the point lies in the box shrunk by the margins (or grown by negative margins),
   * comparing longitudes modulo 360.
   */
  private static boolean within(
      final Envelope box,
      final LatLng point,
      final double latMargin,
      final double lngMargin) {
    if ((point.lat < (box.getMinY() + latMargin)) || (point.lat > (box.getMaxY() - latMargin))) {
      return false;
    }
    final double west = box.getMinX() + lngMargin;
    final double east = box.getMaxX() - lngMargin;
    if ((east - west) >= 360) {
      return true;
    }
    final double lng = west + (((point.lng - west) % 360) + 360) % 360;
    return lng <= east;
  }

  private static void assertValidAndContainsCenter(final long cell, final Geometry geometry) {
    final LatLng center = H3.cellToLatLng(cell);
    final String message = String.format("%x", cell);
    assertTrue(message + " should be valid", geometry.isValid());
    assertTrue(
        message + " should contain its center",
        geometry.contains(
            GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(center.lng, center.lat))));
  }

  private static boolean inRanges(final ByteArrayRange[] ranges, final long cell) {
    final byte[] bin = H3CellRanges.toBin(cell).getBytes();
    return Arrays.stream(ranges).anyMatch(
        range -> (BYTE_ORDER.compare(range.getStart(), bin) <= 0)
            && (BYTE_ORDER.compare(bin, range.getEnd()) <= 0));
  }

  /** Every valid cell at the resolution whose bin falls in one of the ranges. */
  private static Set<Long> cellsInRanges(final ByteArrayRange[] ranges, final int resolution) {
    // steps through every combination of digits 1..resolution, 7 included, with the unused
    // digits left at 7, so any valid cell at the resolution in the range is visited
    final long step = 1L << ((15 - resolution) * 3);
    final Set<Long> cells = new HashSet<>();
    for (final ByteArrayRange range : ranges) {
      final long first = H3CellRanges.fromBin(range.getStart());
      final long last = H3CellRanges.fromBin(range.getEnd());
      assertTrue("range too wide to enumerate", ((last - first) / step) < 10_000_000);
      for (long id = first; id <= last; id += step) {
        if (H3.isValidCell(id)) {
          cells.add(id);
        }
      }
    }
    return cells;
  }

  private static void assertStraddlesAntimeridian(final Collection<Long> cells) {
    assertTrue(cells.stream().anyMatch(c -> H3.cellToLatLng(c).lng > 90));
    assertTrue(cells.stream().anyMatch(c -> H3.cellToLatLng(c).lng < -90));
  }

  private static void assertRejected(final Runnable call) {
    try {
      call.run();
      fail("expected an IllegalArgumentException");
    } catch (final IllegalArgumentException e) {
      // expected
    }
  }

  private static List<Long> sorted(final Collection<Long> cells) {
    return cells.stream().sorted().collect(Collectors.toList());
  }

  private static LinearRing ring(final Envelope envelope) {
    return ring(
        new double[][] {
            {envelope.getMinX(), envelope.getMinY()},
            {envelope.getMaxX(), envelope.getMinY()},
            {envelope.getMaxX(), envelope.getMaxY()},
            {envelope.getMinX(), envelope.getMaxY()},
            {envelope.getMinX(), envelope.getMinY()}});
  }

  private static LinearRing ring(final double[][] coords) {
    return GeometryUtils.GEOMETRY_FACTORY.createLinearRing(
        Arrays.stream(coords).map(c -> new Coordinate(c[0], c[1])).toArray(Coordinate[]::new));
  }

  private static Polygon polygon(final double[][] coords) {
    return GeometryUtils.GEOMETRY_FACTORY.createPolygon(ring(coords));
  }
}
