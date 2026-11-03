/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.binning.ComplexGeometryBinningOption;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.geotime.index.api.SpatialIndexBuilder;
import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import org.locationtech.geowave.core.geotime.store.statistics.binning.SpatialFieldValueBinningStrategy;
import org.locationtech.geowave.core.geotime.store.statistics.binning.TimeRangeFieldValueBinningStrategy;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.StatisticQueryBuilder;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.core.store.statistics.binning.CompositeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.binning.FieldValueBinningStrategy;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.threeten.extra.Interval;

/** H3 geometry constraints, which are range-only, through every statistic query path. */
public class H3BinConstraintsQueryTest {
  private static final String TYPE_NAME = "H3Site";
  private static final int RESOLUTION = 7;
  private static final Envelope ENVELOPE = new Envelope(-77.1, -77.0, 38.8, 38.9);
  private static final int INSIDE = 20;
  private static final int OUTSIDE = 5;
  private static final Instant FIRST_DAY = Instant.parse("2026-10-01T06:00:00Z");
  private static final int DAYS = 3;

  public static class Site {
    private String id;
    private Geometry geometry;
    private Date time;

    public Site() {}

    public Site(final String id, final Geometry geometry, final int day) {
      this.id = id;
      this.geometry = geometry;
      time = Date.from(FIRST_DAY.plus(Duration.ofDays(day)));
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

    public Date getTime() {
      return time;
    }

    public void setTime(final Date time) {
      this.time = time;
    }
  }

  private DataStore dataStore;
  private CountStatistic h3Count;
  private CountStatistic h3ByIdCount;
  private CountStatistic idByH3Count;
  private CountStatistic dayByH3Count;
  private int insideOnFirstDay;

  @Before
  public void createStore() {
    dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());
    dataStore.addType(
        BasicDataTypeAdapter.newAdapter(TYPE_NAME, Site.class, "id"),
        new SpatialIndexBuilder().createIndex());
    h3Count = new CountStatistic(TYPE_NAME);
    h3Count.setTag("h3");
    h3Count.setBinningStrategy(h3Strategy());
    h3ByIdCount = new CountStatistic(TYPE_NAME);
    h3ByIdCount.setTag("h3-id");
    h3ByIdCount.setBinningStrategy(
        new CompositeBinningStrategy(h3Strategy(), new FieldValueBinningStrategy("id")));
    idByH3Count = new CountStatistic(TYPE_NAME);
    idByH3Count.setTag("id-h3");
    idByH3Count.setBinningStrategy(
        new CompositeBinningStrategy(new FieldValueBinningStrategy("id"), h3Strategy()));
    dayByH3Count = new CountStatistic(TYPE_NAME);
    dayByH3Count.setTag("day-h3");
    dayByH3Count.setBinningStrategy(
        new CompositeBinningStrategy(
            new TimeRangeFieldValueBinningStrategy(Unit.DAY, "time"),
            h3Strategy()));
    dataStore.addEmptyStatistic(h3Count, h3ByIdCount, idByH3Count, dayByH3Count);
    insideOnFirstDay = 0;
    for (int i = 0; i < INSIDE; i++) {
      incorporate(
          new Site("r" + (i % 2), point(-77.09 + (i * 0.004), 38.81 + (i * 0.004)), i % DAYS));
      if ((i % DAYS) == 0) {
        insideOnFirstDay++;
      }
    }
    for (int i = 0; i < OUTSIDE; i++) {
      incorporate(new Site("r" + (i % 2), point(2.3 + (i * 0.01), 48.85), i % DAYS));
    }
  }

  // The in-memory store is cached statically, so anything left behind leaks into other tests.
  @After
  public void tearDown() {
    dataStore.deleteAll();
  }

  private static SpatialFieldValueBinningStrategy h3Strategy() {
    return new SpatialFieldValueBinningStrategy(
        SpatialBinningType.H3,
        RESOLUTION,
        ComplexGeometryBinningOption.USE_CENTROID_ONLY,
        "geometry");
  }

  private static Geometry point(final double lng, final double lat) {
    return GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(lng, lat));
  }

  private void incorporate(final Site site) {
    for (final CountStatistic count : new CountStatistic[] {
        h3Count,
        h3ByIdCount,
        idByH3Count,
        dayByH3Count}) {
      dataStore.incorporateStatisticFromEntry(count, TYPE_NAME, site);
    }
  }

  private StatisticQueryBuilder<CountValue, Long, ?> query(
      final String tag,
      final BinConstraints constraints) {
    return StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).typeName(TYPE_NAME).tag(
        tag).binConstraints(constraints);
  }

  @Test
  public void aggregateStatisticsWithAGeometryConstraint() {
    final BinConstraints constraints = BinConstraints.ofObject(ENVELOPE);
    assertEquals(0, h3Count.getBinningStrategy().constraints(ENVELOPE).getBins().length);
    assertEquals(INSIDE, dataStore.getStatisticValue(h3Count, constraints).longValue());
    assertEquals(
        INSIDE,
        dataStore.aggregateStatistics(query("h3", constraints).build()).getValue().longValue());
    assertEquals(INSIDE + OUTSIDE, dataStore.getStatisticValue(h3Count).longValue());
  }

  @Test
  public void queryStatisticsWithAGeometryConstraint() {
    final Set<ByteArray> envelopeBins =
        new HashSet<>(
            Arrays.asList(
                SpatialBinningType.H3.getSpatialBins(
                    GeometryUtils.GEOMETRY_FACTORY.toGeometry(ENVELOPE),
                    RESOLUTION)));
    final Set<ByteArray> bins = new HashSet<>();
    long total = 0;
    try (CloseableIterator<CountValue> values =
        dataStore.queryStatistics(query("h3", BinConstraints.ofObject(ENVELOPE)).build())) {
      while (values.hasNext()) {
        final CountValue value = values.next();
        assertTrue("each bin is returned once", bins.add(value.getBin()));
        assertTrue(envelopeBins.contains(value.getBin()));
        total += value.getValue();
      }
    }
    assertEquals(INSIDE, total);
  }

  @Test
  public void aGeometryConstraintOutsideTheDataMatchesNothing() {
    final BinConstraints constraints = BinConstraints.ofObject(new Envelope(100, 101, 10, 11));
    assertEquals(0, dataStore.getStatisticValue(h3Count, constraints).longValue());
    assertEquals(
        0,
        dataStore.aggregateStatistics(query("h3", constraints).build()).getValue().longValue());
  }

  /**
   * A cell range followed by an exact sub-bin is not one contiguous key range, so a composite led
   * by H3 takes the cells as exact bins when a later child is constrained exactly.
   */
  @Test
  public void compositeWithExactCellsAndAnExactTrailingConstraint() {
    assertCount(
        INSIDE / 2,
        h3ByIdCount,
        BinConstraints.ofObject(new Object[] {envelopeCells(), "r1"}));
  }

  /** Exact cells constrain an H3 child that follows a child whose bins vary in length. */
  @Test
  public void compositeWithAnExactLeadingConstraintAndExactCells() {
    assertCount(
        INSIDE / 2,
        idByH3Count,
        BinConstraints.ofObject(new Object[] {"r1", envelopeCells()}));
    final Interval firstDay = Interval.of(FIRST_DAY, FIRST_DAY.plus(Duration.ofHours(1)));
    assertCount(
        insideOnFirstDay,
        dayByH3Count,
        BinConstraints.ofObject(new Object[] {firstDay, envelopeCells()}));
  }

  private static ByteArray[] envelopeCells() {
    return SpatialBinningType.H3.getSpatialBins(
        GeometryUtils.GEOMETRY_FACTORY.toGeometry(ENVELOPE),
        RESOLUTION);
  }

  private void assertCount(
      final long expected,
      final CountStatistic count,
      final BinConstraints constraints) {
    assertEquals(expected, dataStore.getStatisticValue(count, constraints).longValue());
    assertEquals(
        expected,
        dataStore.aggregateStatistics(
            query(count.getTag(), constraints).build()).getValue().longValue());
  }
}
