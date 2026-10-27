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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import java.util.Date;
import java.util.Map;
import java.util.OptionalInt;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.binning.ComplexGeometryBinningOption;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/** Multi-field constraints of field value subclasses must match the bins they produce. */
public class FieldValueBinningSubclassConstraintsTest {
  private static final DataTypeAdapter<Trip> ADAPTER =
      BasicDataTypeAdapter.newAdapter("trips", Trip.class, "id");
  private static final Geometry ORIGIN =
      GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(-77.03, 38.89));
  private static final Geometry DESTINATION =
      GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(2.35, 48.85));

  @Test
  public void spatialConstraintsUseTheSameJoinAsSpatialBins() {
    final SpatialFieldValueBinningStrategy strategy =
        new SpatialFieldValueBinningStrategy(
            SpatialBinningType.GEOHASH,
            5,
            ComplexGeometryBinningOption.USE_CENTROID_ONLY,
            "origin",
            "destination");
    final ByteArray[] bins = strategy.getBins(ADAPTER, trip());
    assertEquals(1, bins.length);

    final ByteArrayConstraints exact =
        strategy.constraints(Map.of("origin", ORIGIN, "destination", DESTINATION));
    assertFalse(exact.isPrefix());
    assertArrayEquals(bins, exact.getBins());

    final ByteArrayConstraints originOnly =
        strategy.constraints(Map.of("origin", ORIGIN, "destination", BinConstraints.allBins()));
    assertTrue(originOnly.isPrefix());
    assertEquals(1, originOnly.getBins().length);
    assertTrue(ByteArrayUtils.startsWith(bins[0].getBytes(), originOnly.getBins()[0].getBytes()));
  }

  @Test
  public void spatialBinsHaveTheirFixedLength() {
    final Geometry region =
        GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(-77.2, -76.8, 38.7, 39.1));
    for (final SpatialBinningType type : SpatialBinningType.values()) {
      final int precision = type == SpatialBinningType.GEOHASH ? 5 : 7;
      final int binLength = type == SpatialBinningType.GEOHASH ? 5 : 8;
      final SpatialFieldValueBinningStrategy single =
          new SpatialFieldValueBinningStrategy(
              type,
              precision,
              ComplexGeometryBinningOption.USE_FULL_GEOMETRY,
              "origin");
      assertEquals(type.name(), OptionalInt.of(binLength), single.getFixedBinLength());
      final Trip trip = trip();
      trip.setOrigin(region);
      final ByteArray[] regionBins = single.getBins(ADAPTER, trip);
      assertTrue(type.name(), regionBins.length > 1);
      for (final ByteArray bin : regionBins) {
        assertEquals(type.name(), binLength, bin.getBytes().length);
      }
      trip.setOrigin(null);
      assertEquals(type.name(), 0, single.getBins(ADAPTER, trip).length);

      final SpatialFieldValueBinningStrategy multi =
          new SpatialFieldValueBinningStrategy(
              type,
              precision,
              ComplexGeometryBinningOption.USE_CENTROID_ONLY,
              "origin",
              "destination");
      assertEquals(type.name(), OptionalInt.of(2 * binLength), multi.getFixedBinLength());
      assertEquals(type.name(), 2 * binLength, multi.getBins(ADAPTER, trip())[0].getBytes().length);
    }
  }

  @Test
  public void spatialConstraintsMustFitTheBinLength() {
    final SpatialFieldValueBinningStrategy strategy =
        new SpatialFieldValueBinningStrategy(
            SpatialBinningType.GEOHASH,
            5,
            ComplexGeometryBinningOption.USE_CENTROID_ONLY,
            "origin",
            "destination");
    assertThrows(
        IllegalArgumentException.class,
        () -> strategy.constraints(
            Map.of("origin", new ByteArray("dqc"), "destination", BinConstraints.allBins())));
  }

  @Test
  public void timeRangeFieldsCanBeFollowedByAllBins() {
    assertTrue(
        new TimeRangeFieldValueBinningStrategy(Unit.HOUR, "start").constraints(
            BinConstraints.allBins()).isAllBins());

    final TimeRangeFieldValueBinningStrategy strategy =
        new TimeRangeFieldValueBinningStrategy(Unit.HOUR, "start", "end");
    final Trip trip = trip();
    final ByteArrayConstraints startOnly =
        strategy.constraints(Map.of("start", trip.getStart(), "end", BinConstraints.allBins()));
    assertTrue(startOnly.isPrefix());
    assertEquals(1, startOnly.getBins().length);
    assertTrue(
        ByteArrayUtils.startsWith(
            strategy.getBins(ADAPTER, trip)[0].getBytes(),
            startOnly.getBins()[0].getBytes()));
  }

  private static Trip trip() {
    final Trip trip = new Trip();
    trip.setId("trip");
    trip.setOrigin(ORIGIN);
    trip.setDestination(DESTINATION);
    trip.setStart(new Date(1_700_000_000_000L));
    trip.setEnd(new Date(1_700_030_000_000L));
    return trip;
  }

  public static class Trip {
    private String id;
    private Geometry origin;
    private Geometry destination;
    private Date start;
    private Date end;

    public Trip() {}

    public String getId() {
      return id;
    }

    public void setId(final String id) {
      this.id = id;
    }

    public Geometry getOrigin() {
      return origin;
    }

    public void setOrigin(final Geometry origin) {
      this.origin = origin;
    }

    public Geometry getDestination() {
      return destination;
    }

    public void setDestination(final Geometry destination) {
      this.destination = destination;
    }

    public Date getStart() {
      return start;
    }

    public void setStart(final Date start) {
      this.start = start;
    }

    public Date getEnd() {
      return end;
    }

    public void setEnd(final Date end) {
      this.end = end;
    }
  }
}
