/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.statistics.binning;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.binning.ComplexGeometryBinningOption;
import org.locationtech.geowave.core.geotime.binning.H3CellRanges;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.binning.CompositeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.binning.FieldValueBinningStrategy;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.uber.h3core.H3Core;
import com.uber.h3core.PolygonToCellsFlags;
import com.uber.h3core.util.LatLng;

public class H3CellFieldValueBinningStrategyTest {
  private static final int RESOLUTION = 7;
  private static final H3Core H3 = H3CellRanges.h3();
  private static final long FINE_CELL = H3.latLngToCell(37.7749, -122.4194, 10);
  private static final long CELL_AT_RESOLUTION = H3.cellToParent(FINE_CELL, RESOLUTION);
  private static final long COARSE_CELL = H3.cellToParent(FINE_CELL, 4);

  private final H3CellFieldValueBinningStrategy strategy =
      new H3CellFieldValueBinningStrategy(RESOLUTION, "cell");

  private static Geometry box(
      final double minX,
      final double minY,
      final double maxX,
      final double maxY) {
    return GeometryUtils.GEOMETRY_FACTORY.createPolygon(
        new Coordinate[] {
            new Coordinate(minX, minY),
            new Coordinate(maxX, minY),
            new Coordinate(maxX, maxY),
            new Coordinate(minX, maxY),
            new Coordinate(minX, minY)});
  }

  public static class Cell {
    private String id;
    private Long cell;
    private String address;

    public Cell() {}

    public Cell(final String id, final Long cell, final String address) {
      this.id = id;
      this.cell = cell;
      this.address = address;
    }

    public String getId() {
      return id;
    }

    public void setId(final String id) {
      this.id = id;
    }

    public Long getCell() {
      return cell;
    }

    public void setCell(final Long cell) {
      this.cell = cell;
    }

    public String getAddress() {
      return address;
    }

    public void setAddress(final String address) {
      this.address = address;
    }
  }

  private final BasicDataTypeAdapter<Cell> adapter =
      BasicDataTypeAdapter.newAdapter("Cell", Cell.class, "id");

  private ByteArray[] binsOf(final Cell entry, final String field) {
    return new H3CellFieldValueBinningStrategy(RESOLUTION, field).getBins(adapter, entry);
  }

  @Test
  public void cellAtOrFinerThanTheResolutionBinsToItsParent() {
    assertEquals(
        Arrays.asList(H3CellRanges.toBin(CELL_AT_RESOLUTION)),
        Arrays.asList(binsOf(new Cell("a", CELL_AT_RESOLUTION, null), "cell")));
    assertEquals(
        Arrays.asList(H3CellRanges.toBin(CELL_AT_RESOLUTION)),
        Arrays.asList(binsOf(new Cell("b", FINE_CELL, null), "cell")));
  }

  @Test
  public void coarserOrInvalidCellsHaveNoBin() {
    assertEquals(0, binsOf(new Cell("a", COARSE_CELL, null), "cell").length);
    assertEquals(0, binsOf(new Cell("b", null, null), "cell").length);
    assertEquals(0, binsOf(new Cell("c", 12345L, null), "cell").length);
    assertEquals(0, binsOf(new Cell("d", null, "not-a-cell"), "address").length);
    assertEquals(0, binsOf(new Cell("e", null, H3.h3ToString(COARSE_CELL)), "address").length);
  }

  @Test
  public void stringAddressesBinLikeLongs() {
    assertEquals(
        Arrays.asList(H3CellRanges.toBin(CELL_AT_RESOLUTION)),
        Arrays.asList(binsOf(new Cell("a", null, H3.h3ToString(FINE_CELL)), "address")));
  }

  @Test
  public void binsAreEightBytesAndRoundTripToCells() {
    final ByteArray bin = strategy.getBin(FINE_CELL);
    assertEquals(Long.BYTES, bin.getBytes().length);
    assertEquals(CELL_AT_RESOLUTION, strategy.getCell(bin));
  }

  @Test
  public void geometryConstraintsAreRangesCoveringThePolyfill() {
    final Geometry geometry = box(-122.45, 37.75, -122.38, 37.80);
    final ByteArrayConstraints c = strategy.constraints(geometry);
    assertEquals(0, c.getBins().length);
    assertTrue(c.getBinRanges().length > 0);

    final ByteArrayRange[] expected = H3CellRanges.geometryToRanges(geometry, RESOLUTION);
    assertEquals(expected.length, c.getBinRanges().length);
    for (int i = 0; i < expected.length; i++) {
      assertArrayEquals(expected[i].getStart(), c.getBinRanges()[i].getStart());
      assertArrayEquals(expected[i].getEnd(), c.getBinRanges()[i].getEnd());
    }

    final List<LatLng> ring = new ArrayList<>();
    for (final Coordinate coordinate : geometry.getCoordinates()) {
      ring.add(new LatLng(coordinate.y, coordinate.x));
    }
    final List<Long> cover =
        H3.polygonToCellsExperimental(
            ring,
            null,
            RESOLUTION,
            PolygonToCellsFlags.containment_overlapping);
    assertTrue(cover.size() > 1);
    for (final long cell : cover) {
      assertTrue(inRanges(c, H3CellRanges.toBin(cell)));
    }
    assertTrue(!inRanges(c, H3CellRanges.toBin(H3.latLngToCell(-33.0, 151.0, RESOLUTION))));
  }

  @Test
  public void envelopeAndArrayConstraintsMatchTheGeometry() {
    final Geometry geometry = box(-122.45, 37.75, -122.38, 37.80);
    final ByteArrayConstraints expected = strategy.constraints(geometry);
    for (final Object constraint : new Object[] {
        new Envelope(-122.45, -122.38, 37.75, 37.80),
        new Geometry[] {geometry},
        new Envelope[] {new Envelope(-122.45, -122.38, 37.75, 37.80)}}) {
      final ByteArrayConstraints c = strategy.constraints(constraint);
      assertEquals(expected.getBinRanges().length, c.getBinRanges().length);
      for (int i = 0; i < expected.getBinRanges().length; i++) {
        assertArrayEquals(expected.getBinRanges()[i].getStart(), c.getBinRanges()[i].getStart());
        assertArrayEquals(expected.getBinRanges()[i].getEnd(), c.getBinRanges()[i].getEnd());
      }
    }
  }

  @Test
  public void fineCellConstraintsAreExactParentBins() {
    final ByteArray expected = H3CellRanges.toBin(CELL_AT_RESOLUTION);
    final long sibling = H3.cellToChildren(CELL_AT_RESOLUTION, RESOLUTION + 1).get(1);
    for (final Object constraint : new Object[] {
        FINE_CELL,
        H3.h3ToString(FINE_CELL),
        new long[] {FINE_CELL, CELL_AT_RESOLUTION},
        new Long[] {FINE_CELL, sibling},
        new String[] {H3.h3ToString(FINE_CELL)},
        Arrays.asList(FINE_CELL, H3.h3ToString(sibling))}) {
      final ByteArrayConstraints c = strategy.constraints(constraint);
      assertEquals(Arrays.asList(expected), Arrays.asList(c.getBins()));
      assertEquals(0, c.getBinRanges().length);
    }
  }

  @Test
  public void coarseCellConstraintsAreDescendantRanges() {
    final ByteArrayConstraints c = strategy.constraints(COARSE_CELL);
    assertEquals(0, c.getBins().length);
    assertEquals(1, c.getBinRanges().length);
    final ByteArrayRange expected = H3CellRanges.descendantRange(COARSE_CELL, RESOLUTION);
    assertArrayEquals(expected.getStart(), c.getBinRanges()[0].getStart());
    assertArrayEquals(expected.getEnd(), c.getBinRanges()[0].getEnd());
    assertTrue(inRanges(c, H3CellRanges.toBin(CELL_AT_RESOLUTION)));
  }

  @Test
  public void mixedFineAndCoarseCellsBecomeRanges() {
    final long farFineCell = H3.latLngToCell(-33.8688, 151.2093, 9);
    final ByteArrayConstraints c = strategy.constraints(Arrays.asList(COARSE_CELL, farFineCell));
    assertEquals(0, c.getBins().length);
    assertEquals(2, c.getBinRanges().length);
    final ByteArray farBin = strategy.getBin(farFineCell);
    assertTrue(inRanges(c, farBin));
    assertTrue(inRanges(c, H3CellRanges.toBin(CELL_AT_RESOLUTION)));
    final boolean hasExact =
        Arrays.stream(c.getBinRanges()).anyMatch(
            r -> Arrays.equals(r.getStart(), farBin.getBytes())
                && Arrays.equals(r.getEnd(), farBin.getBytes()));
    assertTrue(hasExact);
  }

  @Test
  public void overlappingConstraintsBecomeDisjointRanges() {
    // a fine cell inside a coarse cell, and two overlapping envelopes
    final ByteArrayConstraints cells =
        strategy.constraints(Arrays.asList(COARSE_CELL, FINE_CELL, CELL_AT_RESOLUTION));
    assertEquals(1, cells.getBinRanges().length);
    assertTrue(inRanges(cells, H3CellRanges.toBin(CELL_AT_RESOLUTION)));

    final ByteArrayConstraints envelopes =
        strategy.constraints(
            new Envelope[] {
                new Envelope(-122.5, -122.3, 37.7, 37.8),
                new Envelope(-122.4, -122.2, 37.75, 37.85)});
    final ByteArrayRange[] ranges = envelopes.getBinRanges();
    assertTrue(ranges.length > 0);
    for (int i = 1; i < ranges.length; i++) {
      assertTrue(
          ByteArrayUtils.compare(ranges[i - 1].getEndAsNextPrefix(), ranges[i].getStart()) <= 0);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidCellConstraintIsRejected() {
    strategy.constraints("not-a-cell");
  }

  @Test
  public void binGeometryIsAClosedLonLatPolygon() {
    final Geometry geometry = strategy.getBinGeometry(strategy.getBin(FINE_CELL));
    assertTrue(geometry.isValid());
    assertEquals(7, geometry.getCoordinates().length);
    assertTrue(
        geometry.contains(
            GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(-122.4194, 37.7749))));
  }

  @Test
  public void binGeometryOfAnAntimeridianCellIsSplitAtTheAntimeridian() {
    final H3CellFieldValueBinningStrategy coarse = new H3CellFieldValueBinningStrategy(3, "cell");
    final Geometry geometry = coarse.getBinGeometry(coarse.getBin(H3.latLngToCell(0, 180, 3)));
    assertTrue(geometry.isValid());
    assertTrue(geometry instanceof MultiPolygon);
    assertEquals(2, geometry.getNumGeometries());
    assertTrue(geometry.getEnvelopeInternal().getWidth() > 359);
    assertTrue(geometry.getArea() < 10);
  }

  @Test
  public void binGeometryOfAPolarCellSpansEveryLongitude() {
    final H3CellFieldValueBinningStrategy coarse = new H3CellFieldValueBinningStrategy(3, "cell");
    for (final double pole : new double[] {90, -90}) {
      final long cell = H3.latLngToCell(pole, 0, 3);
      final Geometry geometry = coarse.getBinGeometry(coarse.getBin(cell));
      assertTrue(geometry.isValid());
      assertTrue(
          geometry.covers(GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(0, pole))));
      assertEquals(-180, geometry.getEnvelopeInternal().getMinX(), 1e-9);
      assertEquals(180, geometry.getEnvelopeInternal().getMaxX(), 1e-9);
    }
  }

  @Test
  public void binLengthIsFixedAtEightBytes() {
    assertEquals(Long.BYTES, strategy.getFixedBinLength().getAsInt());
  }

  @Test
  public void resolutionIsValidatedFromTheCommandLineAndFromBinary() {
    final H3CellFieldValueBinningStrategy parsed = new H3CellFieldValueBinningStrategy();
    JCommander.newBuilder().addObject(parsed).build().parse(
        "--binField",
        "cell",
        "--binResolution",
        "15");
    assertEquals(15, parsed.getResolution());
    for (final String bad : new String[] {"16", "-1"}) {
      assertThrows(
          ParameterException.class,
          () -> JCommander.newBuilder().addObject(
              new H3CellFieldValueBinningStrategy()).build().parse(
                  "--binField",
                  "cell",
                  "--binResolution",
                  bad));
    }
    final byte[] valid = strategy.toBinary();
    // the resolution is the leading varint
    final byte[] corrupt = valid.clone();
    corrupt[0] = 16;
    assertThrows(
        IllegalArgumentException.class,
        () -> new H3CellFieldValueBinningStrategy().fromBinary(corrupt));
  }

  @Test
  public void multipleFieldsAreRejectedBeforeUse() {
    final H3CellFieldValueBinningStrategy twoFields = new H3CellFieldValueBinningStrategy();
    JCommander.newBuilder().addObject(twoFields).build().parse(
        "--binField",
        "cell",
        "--binField",
        "other");
    assertThrows(IllegalArgumentException.class, () -> twoFields.constraints(CELL_AT_RESOLUTION));
    assertThrows(IllegalArgumentException.class, twoFields::toBinary);
  }

  @Test
  public void h3LeadingCompositeRejectsAGeometryWithAnExactTrailingConstraint() {
    final Geometry geometry = box(-122.45, 37.75, -122.38, 37.80);
    final CompositeBinningStrategy composite =
        new CompositeBinningStrategy(strategy, new FieldValueBinningStrategy("id"));
    final IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> composite.constraints(new Object[] {geometry, "r1"}));
    assertTrue(e.getMessage(), e.getMessage().contains("only 'all bins' may follow"));

    final SpatialFieldValueBinningStrategy spatial =
        new SpatialFieldValueBinningStrategy(
            SpatialBinningType.H3,
            RESOLUTION,
            ComplexGeometryBinningOption.USE_CENTROID_ONLY,
            "geometry");
    final CompositeBinningStrategy spatialComposite =
        new CompositeBinningStrategy(spatial, new FieldValueBinningStrategy("id"));
    assertThrows(
        IllegalArgumentException.class,
        () -> spatialComposite.constraints(new Object[] {geometry, "r1"}));
  }

  @Test
  public void h3LeadingCompositeWorksWithExactH3BinsAndAnExactTrailingConstraint() {
    final Geometry geometry = box(-122.45, 37.75, -122.38, 37.80);
    final CompositeBinningStrategy composite =
        new CompositeBinningStrategy(strategy, new FieldValueBinningStrategy("id"));
    final ByteArrayConstraints cellConstraints =
        composite.constraints(new Object[] {new long[] {FINE_CELL}, "r1"});
    assertEquals(1, cellConstraints.getBins().length);
    assertEquals(0, cellConstraints.getBinRanges().length);

    final SpatialFieldValueBinningStrategy spatial =
        new SpatialFieldValueBinningStrategy(
            SpatialBinningType.H3,
            RESOLUTION,
            ComplexGeometryBinningOption.USE_CENTROID_ONLY,
            "geometry");
    final ByteArray[] exactBins = SpatialBinningType.H3.getSpatialBins(geometry, RESOLUTION);
    assertTrue(exactBins.length > 1);
    final ByteArrayConstraints spatialConstraints =
        new CompositeBinningStrategy(spatial, new FieldValueBinningStrategy("id")).constraints(
            new Object[] {exactBins, "r1"});
    assertEquals(exactBins.length, spatialConstraints.getBins().length);
    assertEquals(0, spatialConstraints.getBinRanges().length);
  }

  @Test
  public void binToStringIsTheH3Address() {
    assertEquals(
        H3.h3ToString(CELL_AT_RESOLUTION),
        strategy.binToString(H3CellRanges.toBin(CELL_AT_RESOLUTION)));
    assertEquals("cell-H3(7)", strategy.getDefaultTag());
  }

  @Test
  public void persistenceRoundTrip() {
    final H3CellFieldValueBinningStrategy copy =
        (H3CellFieldValueBinningStrategy) PersistenceUtils.fromBinary(
            PersistenceUtils.toBinary(strategy));
    assertNotNull(copy);
    assertEquals(RESOLUTION, copy.getResolution());
    assertEquals(strategy.getDefaultTag(), copy.getDefaultTag());
    assertEquals(strategy.getBin(FINE_CELL), copy.getBin(FINE_CELL));
  }

  @Test(expected = IllegalArgumentException.class)
  public void resolutionIsValidated() {
    new H3CellFieldValueBinningStrategy(16, "cell");
  }

  private static boolean inRanges(final ByteArrayConstraints c, final ByteArray bin) {
    for (final ByteArrayRange range : c.getBinRanges()) {
      if ((bin.compareTo(new ByteArray(range.getStart())) >= 0)
          && (bin.compareTo(new ByteArray(range.getEnd())) <= 0)) {
        return true;
      }
    }
    return false;
  }

  private DataStore dataStore;

  @Before
  public void createStore() {
    dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());
    dataStore.addType(BasicDataTypeAdapter.newAdapter("Cell", Cell.class, "id"));
  }

  @After
  public void tearDown() {
    dataStore.deleteAll();
  }

  @Test
  public void binnedCountRoundTripsThroughTheDataStore() {
    final CountStatistic count = new CountStatistic("Cell");
    count.setTag("by-cell");
    count.setBinningStrategy(new H3CellFieldValueBinningStrategy(RESOLUTION, "cell"));
    dataStore.addEmptyStatistic(count);

    final long sanFrancisco = H3.latLngToCell(37.7749, -122.4194, RESOLUTION);
    final long oakland = H3.latLngToCell(37.8044, -122.2712, RESOLUTION);
    final long sydney = H3.latLngToCell(-33.8688, 151.2093, RESOLUTION);
    final long[] cells = {sanFrancisco, oakland, sydney};
    for (int i = 0; i < cells.length; i++) {
      final CountValue value = count.createEmpty();
      value.fromBinary(VarintUtils.writeSignedLong(10L * (i + 1)));
      dataStore.incorporateStatisticValue(count, value, strategy.getBin(cells[i]));
    }

    final Geometry bayArea = box(-122.6, 37.6, -122.1, 38.0);
    final List<Pair<ByteArray, Long>> results = new ArrayList<>();
    try (CloseableIterator<Pair<ByteArray, Long>> it =
        dataStore.getBinnedStatisticValues(count, BinConstraints.ofObject(bayArea))) {
      while (it.hasNext()) {
        results.add(it.next());
      }
    }
    assertEquals(2, results.size());
    long total = 0;
    for (final Pair<ByteArray, Long> r : results) {
      total += r.getValue();
    }
    assertEquals(30L, total);
  }
}
