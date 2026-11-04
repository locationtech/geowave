/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.basic;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.geotime.binning.H3CellRanges;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.geotime.motion.H3MotionSegmenter;
import org.locationtech.geowave.core.geotime.motion.MotionCellKey;
import org.locationtech.geowave.core.geotime.motion.MotionDomain;
import org.locationtech.geowave.core.geotime.motion.MotionReport;
import org.locationtech.geowave.core.geotime.motion.MotionSource;
import org.locationtech.geowave.core.geotime.motion.MotionVectorAccumulator;
import org.locationtech.geowave.core.geotime.motion.MotionVectorLayout;
import org.locationtech.geowave.core.geotime.motion.ResolutionPlan;
import org.locationtech.geowave.core.geotime.store.statistics.binning.EpochInterval;
import org.locationtech.geowave.core.geotime.store.statistics.binning.EpochIntervalBinningStrategy;
import org.locationtech.geowave.core.geotime.store.statistics.binning.H3CellFieldValueBinningStrategy;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.StatisticBinningStrategy;
import org.locationtech.geowave.core.store.api.StatisticQueryBuilder;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.statistics.adapter.AdditiveVectorStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.AdditiveVectorStatistic.AdditiveVectorValue;
import org.locationtech.geowave.core.store.statistics.binning.CompositeBinningStrategy;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

/**
 * Segments a synthetic shipping lane into H3 motion vectors and stores them as additive vector
 * statistics binned by (hour, H3 cell) at the surface analysis and overview levels, the way a
 * streaming writer would: batched flushes that add to the same bins, window x viewport reads, a
 * negated correction and a retention range delete.
 */
@RunWith(GeoWaveITRunner.class)
public class GeoWaveMotionStatisticsIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveMotionStatisticsIT.class);

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB,
          GeoWaveStoreType.FILESYSTEM})
  protected DataStorePluginOptions dataStore;

  private static final String TYPE_NAME = "motion_surface";
  private static final String TIME_FIELD = "time";
  private static final String CELL_FIELD = "cell";
  private static final MotionDomain DOMAIN = MotionDomain.SURFACE;
  // as DotMatrix configures AIS: 3 minute raw cadence at rest, regions up to 10 minutes late
  private static final MotionSource AIS =
      new MotionSource("AIS", 200, 10, Duration.ofMinutes(10).toMillis()).withRawIntervalMillis(
          Duration.ofMinutes(3).toMillis()).withAllowedLatenessMillis(
              Duration.ofMinutes(10).toMillis());
  private static final long START = Instant.parse("2026-10-26T00:00:00Z").toEpochMilli();
  private static final long MINUTE = Duration.ofMinutes(1).toMillis();
  private static final long HOUR = Duration.ofHours(1).toMillis();
  private static final double LANE_LATITUDE = 36.0;
  private static final double LANE_START_LONGITUDE = -6.0;
  private static final double LANE_METERS = 89_000;
  private static final double METERS_PER_DEGREE = 111_320;
  private static final int VESSELS = 30;
  private static final int DARK_VESSEL = 0;
  private static final int STOPPING_VESSEL = 1;

  private static final EpochIntervalBinningStrategy HOURS =
      new EpochIntervalBinningStrategy(EpochInterval.HOUR, TIME_FIELD);
  private static final int[] LEVELS = ResolutionPlan.defaults().getLevels(DOMAIN);

  private static long startMillis;

  private final Map<Integer, AdditiveVectorStatistic> statistics = new HashMap<>();

  @BeforeClass
  public static void reportTestStart() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("-------------------------------------");
    LOGGER.warn("*                                   *");
    LOGGER.warn("* RUNNING GeoWaveMotionStatisticsIT *");
    LOGGER.warn("*                                   *");
    LOGGER.warn("-------------------------------------");
  }

  @AfterClass
  public static void reportTestFinish() {
    LOGGER.warn("--------------------------------------");
    LOGGER.warn("*                                    *");
    LOGGER.warn("* FINISHED GeoWaveMotionStatisticsIT *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                    *");
    LOGGER.warn("--------------------------------------");
  }

  @Before
  public void initialize() {
    final DataStore ds = dataStore.createDataStore();
    ds.addType(
        SimpleIngest.createDataAdapter(createFeatureType()),
        SimpleIngest.createSpatialIndex());
    for (final int level : LEVELS) {
      final AdditiveVectorStatistic statistic = MotionVectorLayout.createStatistic(TYPE_NAME);
      statistic.setTag("surface-" + level);
      statistic.setBinningStrategy(
          new CompositeBinningStrategy(
              HOURS,
              new H3CellFieldValueBinningStrategy(level, CELL_FIELD)));
      statistics.put(level, statistic);
    }
    ds.addStatistic(statistics.values().toArray(new AdditiveVectorStatistic[0]));
  }

  @After
  public void cleanupWorkspace() {
    TestUtils.deleteAll(dataStore);
  }

  private static SimpleFeatureType createFeatureType() {
    final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
    final AttributeTypeBuilder attributes = new AttributeTypeBuilder();
    builder.setName(TYPE_NAME);
    builder.add(
        attributes.binding(Point.class).nillable(false).buildDescriptor(
            SimpleIngest.GEOMETRY_FIELD));
    builder.add(attributes.binding(Date.class).nillable(true).buildDescriptor(TIME_FIELD));
    builder.add(attributes.binding(Long.class).nillable(true).buildDescriptor(CELL_FIELD));
    return builder.buildFeatureType();
  }

  @Test
  public void testLaneMotionStatistics() {
    final DataStore ds = dataStore.createDataStore();
    final MotionVectorLayout layout = MotionVectorLayout.defaults(DOMAIN);
    final MotionVectorAccumulator accumulator = new MotionVectorAccumulator(layout);
    final H3MotionSegmenter<String> segmenter =
        H3MotionSegmenter.<String>builder(accumulator).build();
    final Map<MotionCellKey, AdditiveVectorValue> expected = new HashMap<>();

    // two flushes that both touch the hours around the cut, as a 15 minute flush cycle would
    final long cut = START + (95 * MINUTE);
    final List<Event> events = laneEvents();
    for (final Event event : events) {
      if (event.time >= cut) {
        break;
      }
      event.applyTo(segmenter);
    }
    segmenter.advanceTo(cut);
    flush(ds, accumulator.drain(), expected);
    events.stream().filter(e -> e.time >= cut).forEach(e -> e.applyTo(segmenter));
    segmenter.close();
    flush(ds, accumulator.drain(), expected);

    for (final int level : LEVELS) {
      assertStored(ds, level, BinConstraints.allBins(), expectedAt(expected, level, null, null));
    }

    // moving and stopped seconds and distance roll up exactly from the analysis level
    final AdditiveVectorValue analysis = total(expectedAt(expected, LEVELS[0], null, null));
    final AdditiveVectorValue overview = total(expectedAt(expected, LEVELS[1], null, null));
    for (final int sum : new int[] {
        MotionVectorLayout.MOVING_SECONDS,
        MotionVectorLayout.STOPPED_SECONDS,
        MotionVectorLayout.STABLE_SECONDS,
        MotionVectorLayout.DISTANCE_METERS}) {
      assertEquals(analysis.getSum(sum), overview.getSum(sum), analysis.getSum(sum) * 1e-6);
    }
    for (final AdditiveVectorValue levelTotal : new AdditiveVectorValue[] {analysis, overview}) {
      assertEquals(1, levelTotal.getCount(MotionVectorLayout.GAP_STARTS));
      // the 40 minute dark period, in the 30 to 60 minute bin
      assertEquals(1, levelTotal.getCount(MotionVectorLayout.GAP_DURATION_HISTOGRAM + 2));
      assertEquals(1, levelTotal.getCount(MotionVectorLayout.STOP_STARTS));
      assertEquals(30 * 60, levelTotal.getSum(MotionVectorLayout.STOPPED_SECONDS), 120);
      // every vessel has a stable identity, so all occupancy counts toward dwell
      assertEquals(
          levelTotal.getSum(MotionVectorLayout.MOVING_SECONDS)
              + levelTotal.getSum(MotionVectorLayout.STOPPED_SECONDS),
          levelTotal.getSum(MotionVectorLayout.STABLE_SECONDS),
          1e-6 * levelTotal.getSum(MotionVectorLayout.STABLE_SECONDS));
      // a stop ends one passage and begins another
      long flow = 0;
      for (int i = 0; i < (MotionVectorLayout.SLOTS * MotionVectorLayout.SLOTS); i++) {
        flow += levelTotal.getCount(MotionVectorLayout.FLOW + i);
      }
      assertEquals(
          levelTotal.getCount(MotionVectorLayout.VISITS)
              + levelTotal.getCount(MotionVectorLayout.STOP_STARTS),
          flow);
    }
    // every vessel crosses the analysis cells eastward, so through-traffic has exit timing
    long traversals = 0;
    for (int exit = 0; exit < MotionVectorLayout.DIRECTIONS; exit++) {
      traversals += analysis.getCount(MotionVectorLayout.EXIT_TRAVERSALS + exit);
    }
    assertTrue(traversals > VESSELS);

    // a window over a viewport is exact hour bins x compact H3 ranges
    final Interval window = Interval.of(Instant.ofEpochMilli(START + HOUR), Duration.ofHours(2));
    final Geometry viewport =
        GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(-5.8, -5.4, 35.9, 36.1));
    final int level = LEVELS[0];
    final Map<MotionCellKey, AdditiveVectorValue> inView =
        expectedAt(expected, level, window, viewport);
    assertTrue(inView.size() > 1);
    assertStored(ds, level, BinConstraints.ofObject(new Object[] {window, viewport}), inView);
    final AdditiveVectorValue aggregated =
        ds.aggregateStatistics(
            StatisticQueryBuilder.newBuilder(AdditiveVectorStatistic.STATS_TYPE).typeName(
                TYPE_NAME).tag("surface-" + level).binConstraints(
                    BinConstraints.ofObject(new Object[] {window, viewport})).build());
    assertVectorEquals(total(inView), aggregated);

    // a correction is a negated delta
    final MotionCellKey corrected = inView.keySet().iterator().next();
    final AdditiveVectorValue correction = layout.createValue(null);
    correction.subtract(expected.get(corrected));
    ds.incorporateStatisticValue(statistics.get(level), correction, corrected.getBin());
    expected.get(corrected).add(correction);
    final AdditiveVectorValue canceled =
        ds.getStatisticValue(statistics.get(level), BinConstraints.of(corrected.getBin()));
    assertArrayEquals(new long[MotionVectorLayout.COUNT_LENGTH], canceled.getCounts());

    // retention deletes whole hours of the analysis level and leaves the overview level alone
    final Interval firstHour = Interval.of(Instant.ofEpochMilli(START), Duration.ofHours(1));
    assertTrue(
        ds.removeStatisticValues(
            statistics.get(level),
            BinConstraints.ofRange(HOURS.getBinRanges(firstHour))));
    expected.keySet().removeIf(
        k -> (k.getLevel() == level)
            && firstHour.contains(Instant.ofEpochMilli(k.getBucketStart())));
    for (final int l : LEVELS) {
      assertStored(ds, l, BinConstraints.allBins(), expectedAt(expected, l, null, null));
    }

    final int largest =
        expected.values().stream().mapToInt(v -> PersistenceUtils.toBinary(v).length).max().orElse(
            0);
    LOGGER.info("{} motion values, the largest {} bytes", expected.size(), largest);
    assertTrue(largest <= 200);
  }

  private void flush(
      final DataStore ds,
      final Map<MotionCellKey, AdditiveVectorValue> drained,
      final Map<MotionCellKey, AdditiveVectorValue> expected) {
    for (final int level : LEVELS) {
      final Map<ByteArray, AdditiveVectorValue> batch =
          drained.entrySet().stream().filter(e -> e.getKey().getLevel() == level).collect(
              Collectors.toMap(e -> e.getKey().getBin(), Map.Entry::getValue));
      ds.incorporateStatisticValues(statistics.get(level), batch);
    }
    final MotionVectorLayout layout = MotionVectorLayout.defaults(DOMAIN);
    drained.forEach(
        (key, value) -> expected.computeIfAbsent(key, k -> layout.createValue(null)).add(value));
  }

  private static Map<MotionCellKey, AdditiveVectorValue> expectedAt(
      final Map<MotionCellKey, AdditiveVectorValue> expected,
      final int level,
      final Interval window,
      final Geometry viewport) {
    final Set<Long> cells = new HashSet<>();
    if (viewport != null) {
      for (final ByteArray bin : SpatialBinningType.H3.getSpatialBins(viewport, level)) {
        cells.add(H3CellRanges.fromBin(bin.getBytes()));
      }
    }
    return expected.entrySet().stream().filter(
        e -> (e.getKey().getLevel() == level)
            && ((window == null)
                || window.contains(Instant.ofEpochMilli(e.getKey().getBucketStart())))
            && ((viewport == null) || cells.contains(e.getKey().getCell()))).collect(
                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private void assertStored(
      final DataStore ds,
      final int level,
      final BinConstraints constraints,
      final Map<MotionCellKey, AdditiveVectorValue> expected) {
    final AdditiveVectorStatistic statistic = statistics.get(level);
    final CompositeBinningStrategy binning =
        (CompositeBinningStrategy) statistic.getBinningStrategy();
    final Map<MotionCellKey, AdditiveVectorValue> stored = new HashMap<>();
    try (CloseableIterator<Pair<ByteArray, AdditiveVectorValue>> it =
        ds.getBinnedStatisticValues(statistic, constraints)) {
      while (it.hasNext()) {
        final Pair<ByteArray, AdditiveVectorValue> binned = it.next();
        if (binned.getKey().getBytes().length == 0) {
          // nothing matched, so the store returned the statistic's empty value
          continue;
        }
        final Pair<StatisticBinningStrategy, ByteArray>[] subBins =
            binning.getSubBins(binned.getKey());
        stored.put(
            new MotionCellKey(
                HOURS.getInterval(subBins[0].getRight()).getStart().toEpochMilli(),
                level,
                H3CellRanges.fromBin(subBins[1].getRight().getBytes())),
            binned.getValue());
      }
    }
    assertEquals(expected.keySet(), stored.keySet());
    expected.forEach((key, value) -> assertVectorEquals(value, stored.get(key)));
  }

  private static AdditiveVectorValue total(final Map<MotionCellKey, AdditiveVectorValue> values) {
    final AdditiveVectorValue total = MotionVectorLayout.defaults(DOMAIN).createValue(null);
    values.values().forEach(total::add);
    return total;
  }

  private static void assertVectorEquals(
      final AdditiveVectorValue expected,
      final AdditiveVectorValue actual) {
    assertArrayEquals(expected.getCounts(), actual.getCounts());
    // stored sums are 32-bit floats, and a canceled sum keeps the rounding of each stored delta
    for (int i = 0; i < expected.getSumLength(); i++) {
      assertEquals(
          expected.getSum(i),
          actual.getSum(i),
          Math.max(0.1, Math.abs(expected.getSum(i)) * 1e-5));
    }
  }

  /**
   * Vessels sail east along the lane at 6 to 10 m/s, reporting every 10 seconds, with starts two
   * minutes apart and lanes offset across a 2 km corridor. One vessel goes dark for 40 minutes, one
   * stops for 30 minutes, and each track ends explicitly.
   */
  private static List<Event> laneEvents() {
    final List<Event> events = new ArrayList<>();
    for (int vessel = 0; vessel < VESSELS; vessel++) {
      final String key = "vessel-" + vessel;
      final double speed = 6 + (vessel % 5);
      final double offset = ((vessel % 7) - 3) * 300.0;
      final long departure = START + (vessel * 2 * MINUTE);
      double meters = 0;
      long time = departure;
      long stopUntil = -1;
      while (meters < LANE_METERS) {
        final boolean dark =
            (vessel == DARK_VESSEL)
                && (time >= (departure + (40 * MINUTE)))
                && (time < (departure + (80 * MINUTE)));
        if ((vessel == STOPPING_VESSEL) && (stopUntil < 0) && (time >= (departure + HOUR))) {
          stopUntil = time + (30 * MINUTE);
        }
        final boolean stopped = time < stopUntil;
        if (!dark) {
          // a stopped vessel's reported position jitters by a few meters
          final double jitter = stopped ? (((time / 10_000) % 3) - 1) * 5.0 : 0;
          events.add(
              new Event(
                  time,
                  new MotionReport<>(
                      key,
                      true,
                      DOMAIN,
                      AIS,
                      time,
                      LANE_LATITUDE + ((offset + jitter) / METERS_PER_DEGREE),
                      LANE_START_LONGITUDE
                          + (meters
                              / (METERS_PER_DEGREE * Math.cos(Math.toRadians(LANE_LATITUDE)))),
                      stopped ? 0 : speed,
                      90,
                      Double.NaN)));
        }
        if (!stopped) {
          meters += speed * 10;
        }
        time += 10_000;
      }
      events.add(new Event(time, key));
    }
    events.sort((a, b) -> Long.compare(a.time, b.time));
    return events;
  }

  private static class Event {
    private final long time;
    private final MotionReport<String> report;
    private final String end;

    private Event(final long time, final MotionReport<String> report) {
      this.time = time;
      this.report = report;
      end = null;
    }

    private Event(final long time, final String end) {
      this.time = time;
      report = null;
      this.end = end;
    }

    private void applyTo(final H3MotionSegmenter<String> segmenter) {
      if (report != null) {
        segmenter.accept(report);
      } else {
        segmenter.end(end);
      }
    }
  }
}
