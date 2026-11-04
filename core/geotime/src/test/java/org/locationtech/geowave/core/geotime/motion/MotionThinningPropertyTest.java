/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.motion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.locationtech.geowave.core.geotime.motion.MotionTestSupport.MARITIME;
import static org.locationtech.geowave.core.geotime.motion.MotionTestSupport.T0;
import static org.locationtech.geowave.core.geotime.motion.MotionTestSupport.admit;
import static org.locationtech.geowave.core.geotime.motion.MotionTestSupport.gaussian;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.motion.MotionSlice.Type;
import org.locationtech.geowave.core.store.statistics.adapter.AdditiveVectorStatistic.AdditiveVectorValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The same synthetic traffic, once with every report and once thinned by dead-reckoning admission,
 * must give the same statistics within the plan's tolerances. 240 vessels each make a round trip on
 * a 60 km east-west lane, spread across 2 km, with gentle meanders, speed changes and an anchored
 * stop at each end, reporting every 10 seconds with 5 m position noise. Thinning moves a curved
 * path by up to about a quarter of the tolerance, so a transition flips only for passes that close
 * to a cell vertex; a lane much narrower than a cell would concentrate those. The same traffic is
 * compared again without reported speeds, thinned by distance alone.
 */
public class MotionThinningPropertyTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(MotionThinningPropertyTest.class);
  private static final int LEVEL = 6;
  private static final double KNOT = 1852.0 / 3600;
  private static final int VESSELS = 240;

  /** Unthinned and thinned runs over the same traffic, with reported speed and without. */
  private static final List<Run[]> PAIRS = new ArrayList<>();

  private static final class Run implements Consumer<MotionSlice<String>> {
    final String label;
    final MotionVectorAccumulator accumulator =
        new MotionVectorAccumulator(MotionVectorLayout.defaults(MotionDomain.SURFACE));
    final List<Double> speeds = new ArrayList<>();
    int reports;

    Run(final String label) {
      this.label = label;
    }

    @Override
    public void accept(final MotionSlice<String> slice) {
      accumulator.accept(slice);
      if ((slice.getType() == Type.VISIT_END)
          && (slice.getLevel() == LEVEL)
          && !Double.isNaN(slice.getSpeed())) {
        speeds.add(slice.getSpeed());
      }
    }

    Map<Long, AdditiveVectorValue> byCell() {
      final MotionVectorLayout layout = accumulator.getLayout();
      final Map<Long, AdditiveVectorValue> cells = new HashMap<>();
      accumulator.getValues().forEach((key, value) -> {
        if (key.getLevel() == LEVEL) {
          cells.computeIfAbsent(key.getCell(), c -> layout.createValue(null)).add(value);
        }
      });
      return cells;
    }
  }

  @BeforeClass
  public static void runAll() {
    final Random random = new Random(20261027L);
    final List<List<MotionReport<String>>> vessels = new ArrayList<>();
    for (int v = 0; v < VESSELS; v++) {
      vessels.add(new LaneVessel("v" + v, random).sail(T0 + (random.nextInt(360) * 60_000L)));
    }
    final List<List<MotionReport<String>>> admitted = new ArrayList<>();
    final List<List<MotionReport<String>>> speedless = new ArrayList<>();
    final List<List<MotionReport<String>>> speedlessAdmitted = new ArrayList<>();
    for (final List<MotionReport<String>> vessel : vessels) {
      admitted.add(admit(vessel, MARITIME.getToleranceMeters(), MARITIME.getMaxIntervalMillis()));
      final List<MotionReport<String>> withoutSpeed =
          H3MotionSegmenterTest.speedless(vessel, MotionDomain.SURFACE, MARITIME);
      speedless.add(withoutSpeed);
      speedlessAdmitted.add(
          admitByDistance(
              withoutSpeed,
              MARITIME.getToleranceMeters(),
              MARITIME.getMaxIntervalMillis()));
    }
    PAIRS.add(new Run[] {segment("unthinned", vessels), segment("thinned", admitted)});
    PAIRS.add(
        new Run[] {
            segment("unthinned without speed", speedless),
            segment("thinned without speed", speedlessAdmitted)});
  }

  /**
   * Admission for reports without a speed: keeps a report farther than the tolerance from the last
   * kept one, or when the max interval has elapsed, and the first and last reports.
   */
  private static List<MotionReport<String>> admitByDistance(
      final List<MotionReport<String>> reports,
      final double toleranceMeters,
      final long maxIntervalMillis) {
    final List<MotionReport<String>> kept = new ArrayList<>();
    MotionReport<String> last = null;
    for (int i = 0; i < reports.size(); i++) {
      final MotionReport<String> report = reports.get(i);
      if ((last == null)
          || (i == (reports.size() - 1))
          || ((report.getTime() - last.getTime()) >= maxIntervalMillis)
          || (Spherical.distance(
              last.getLatitude(),
              last.getLongitude(),
              report.getLatitude(),
              report.getLongitude()) > toleranceMeters)) {
        kept.add(report);
        last = report;
      }
    }
    return kept;
  }

  private static Run segment(final String label, final List<List<MotionReport<String>>> vessels) {
    final Run run = new Run(label);
    final List<MotionReport<String>> reports = new ArrayList<>();
    vessels.forEach(reports::addAll);
    reports.sort((a, b) -> Long.compare(a.getTime(), b.getTime()));
    final long start = System.nanoTime();
    final H3MotionSegmenter<String> segmenter = H3MotionSegmenter.<String>builder(run).build();
    long nextAdvance = Long.MIN_VALUE;
    for (final MotionReport<String> report : reports) {
      segmenter.accept(report);
      if (report.getTime() >= nextAdvance) {
        segmenter.advanceTo(report.getTime());
        nextAdvance = report.getTime() + 60_000L;
      }
    }
    segmenter.close();
    final double seconds = (System.nanoTime() - start) / 1e9;
    LOGGER.info(
        "Segmented {} {} reports in {} s, {} reports/s; {} slots used the non-adjacent fallback",
        reports.size(),
        label,
        seconds,
        Math.round(reports.size() / seconds),
        segmenter.getNonAdjacentSlots());
    assertEquals(0, segmenter.getImplausibleJumps());
    run.reports = reports.size();
    return run;
  }

  @Test
  public void thinningRemovesMostReports() {
    final Run[] pair = PAIRS.get(0);
    assertTrue(pair[1].reports * 10 < pair[0].reports);
    for (final Run[] each : PAIRS) {
      // a finished track times out like any other, at its last report, which admission keeps
      assertEquals(each[0].label, gapStarts(each[0]), gapStarts(each[1]));
      assertTrue(each[0].label, gapStarts(each[0]) <= VESSELS);
    }
  }

  private static long gapStarts(final Run run) {
    return run.byCell().values().stream().mapToLong(
        v -> v.getCount(MotionVectorLayout.GAP_STARTS)).sum();
  }

  @Test
  public void occupancyAgreesPerCell() {
    for (final Run[] pair : PAIRS) {
      final Map<Long, AdditiveVectorValue> fullCells = pair[0].byCell();
      final Map<Long, AdditiveVectorValue> thinnedCells = pair[1].byCell();
      final double total =
          fullCells.values().stream().mapToDouble(MotionThinningPropertyTest::occupancy).sum();
      assertEquals(
          total,
          thinnedCells.values().stream().mapToDouble(MotionThinningPropertyTest::occupancy).sum(),
          total * 1e-9);
      final Set<Long> cells = new HashSet<>(fullCells.keySet());
      cells.addAll(thinnedCells.keySet());
      int compared = 0;
      double worst = 0;
      for (final Long cell : cells) {
        final double expected = occupancy(fullCells.get(cell));
        final double actual = occupancy(thinnedCells.get(cell));
        if (Math.max(expected, actual) < (0.01 * total)) {
          continue;
        }
        compared++;
        final double relative = Math.abs(actual - expected) / expected;
        worst = Math.max(worst, relative);
        assertTrue(
            pair[1].label + " " + Long.toHexString(cell) + " differs by " + relative,
            relative <= 0.03);
      }
      LOGGER.info(
          "{}: compared occupancy of {} of {} cells; worst difference {}",
          pair[1].label,
          compared,
          cells.size(),
          worst);
      assertTrue(compared >= 10);
    }
  }

  @Test
  public void medianVisitSpeedAgrees() {
    for (final Run[] pair : PAIRS) {
      final double expected = median(pair[0].speeds);
      final double actual = median(pair[1].speeds);
      LOGGER.info("{}: median visit speed {} vs {} m/s", pair[1].label, expected, actual);
      assertEquals(pair[1].label, expected, actual, 0.5 * KNOT);
    }
  }

  @Test
  public void headingRoseAgrees() {
    for (final Run[] pair : PAIRS) {
      final double[] expected = new double[MotionVectorLayout.HEADING_SECTORS];
      final double[] actual = new double[MotionVectorLayout.HEADING_SECTORS];
      final Map<Long, AdditiveVectorValue> fullCells = pair[0].byCell();
      final Map<Long, AdditiveVectorValue> thinnedCells = pair[1].byCell();
      double worstCell = 0;
      for (final Map.Entry<Long, AdditiveVectorValue> entry : fullCells.entrySet()) {
        final double[] cellExpected = rose(entry.getValue());
        final double[] cellActual = rose(thinnedCells.get(entry.getKey()));
        for (int i = 0; i < expected.length; i++) {
          expected[i] += cellExpected[i];
          actual[i] += cellActual[i];
        }
        if ((sum(cellExpected) >= 100) && (sum(cellActual) >= 100)) {
          worstCell = Math.max(worstCell, totalVariation(cellExpected, cellActual));
        }
      }
      final double overall = totalVariation(expected, actual);
      LOGGER.info(
          "{}: rose total variation {} overall, {} worst cell",
          pair[1].label,
          overall,
          worstCell);
      assertTrue(pair[1].label, overall <= 0.05);
      assertTrue(pair[1].label, worstCell <= 0.05);
    }
  }

  @Test
  public void transitionProbabilitiesAgree() {
    for (final Run[] pair : PAIRS) {
      final Map<Long, AdditiveVectorValue> fullCells = pair[0].byCell();
      final Map<Long, AdditiveVectorValue> thinnedCells = pair[1].byCell();
      int rows = 0;
      double worst = 0;
      for (final Map.Entry<Long, AdditiveVectorValue> entry : fullCells.entrySet()) {
        for (int entrySlot = 0; entrySlot < MotionVectorLayout.SLOTS; entrySlot++) {
          final double[] expected = row(entry.getValue(), entrySlot);
          final double[] actual = row(thinnedCells.get(entry.getKey()), entrySlot);
          final double n = sum(expected);
          if (n < 100) {
            continue;
          }
          rows++;
          final double m = sum(actual);
          for (int exit = 0; exit < MotionVectorLayout.SLOTS; exit++) {
            final double delta = Math.abs((expected[exit] / n) - (m == 0 ? 0 : actual[exit] / m));
            worst = Math.max(worst, delta);
            assertTrue(
                pair[1].label
                    + " "
                    + Long.toHexString(entry.getKey())
                    + " "
                    + entrySlot
                    + "->"
                    + exit
                    + " differs by "
                    + delta,
                delta <= 0.05);
          }
        }
      }
      LOGGER.info(
          "{}: compared {} transition rows with n >= 100; worst |dp| {}",
          pair[1].label,
          rows,
          worst);
      assertTrue(rows >= 5);
    }
  }

  @Test
  public void stopsAgree() {
    final Run[] withSpeed = PAIRS.get(0);
    assertEquals(stops(withSpeed[0]), stops(withSpeed[1]));
    // each vessel starts at anchor, which is not a stop start, and stops twice more
    assertEquals(VESSELS * 2, stops(withSpeed[0]));
    final Run[] withoutSpeed = PAIRS.get(1);
    assertEquals(stops(withoutSpeed[0]), stops(withoutSpeed[1]));
    // without a speed the first anchorage is only recognized once a baseline has passed
    assertEquals(VESSELS * 3, stops(withoutSpeed[0]));
  }

  /**
   * A vessel at anchor for 6 hours reports every 3 minutes with jitter. Admission keeps the first
   * report at or after 10 minutes, so kept reports are 12 minutes and a little apart: continuous
   * only when the source declares its raw interval.
   */
  @Test
  public void anchoredThreeMinuteCadenceStaysContinuous() {
    for (final long[] jitter : new long[][] {{0, 3_000L}, {-18_000L, 18_000L}}) {
      final List<MotionReport<String>> full = new ArrayList<>();
      final Random random = new Random(42);
      final MotionSource ais = MARITIME.withRawIntervalMillis(180_000L);
      for (int i = 0; i < 120; i++) {
        final long time =
            T0
                + (i * 180_000L)
                + jitter[0]
                + (long) (random.nextDouble() * (jitter[1] - jitter[0]));
        // an 80 m swing around the anchor over 6 hours, plus 5 m of noise
        final double[] swung = MotionTestSupport.destination(1.24, 103.9, (360.0 * i) / 120, 80);
        final double[] p =
            MotionTestSupport.destination(
                swung[0],
                swung[1],
                random.nextDouble() * 360,
                Math.abs(random.nextGaussian() * 5));
        full.add(
            new MotionReport<>(
                "anchored",
                true,
                MotionDomain.SURFACE,
                ais,
                time,
                p[0],
                p[1],
                random.nextDouble() * 0.1,
                random.nextDouble() * 360,
                Double.NaN));
      }
      final List<MotionReport<String>> thinned =
          admit(full, MARITIME.getToleranceMeters(), MARITIME.getMaxIntervalMillis());
      assertTrue(thinned.size() < (full.size() / 3));
      final double span = (full.get(full.size() - 1).getTime() - full.get(0).getTime()) / 1000.0;
      for (final List<MotionReport<String>> reports : List.of(full, thinned)) {
        final MotionTestSupport.Recorder recorder = new MotionTestSupport.Recorder();
        final H3MotionSegmenter<String> segmenter =
            H3MotionSegmenter.<String>builder(recorder).build();
        reports.forEach(segmenter::accept);
        segmenter.close();
        assertTrue(recorder.of(Type.GAP_START).isEmpty());
        assertEquals(1, recorder.of(Type.VISIT_END, LEVEL).size());
        assertEquals(
            span,
            recorder.of(Type.OCCUPANCY, LEVEL).stream().mapToDouble(
                MotionSlice::getStoppedSeconds).sum(),
            1e-6);
      }
      // without the raw interval the 12 minute window is shorter than the kept spacing
      final List<MotionReport<String>> undeclared =
          thinned.stream().map(
              r -> new MotionReport<>(
                  r.getEntityKey(),
                  true,
                  MotionDomain.SURFACE,
                  MARITIME,
                  r.getTime(),
                  r.getLatitude(),
                  r.getLongitude(),
                  r.getSpeed(),
                  r.getCourse(),
                  Double.NaN)).collect(Collectors.toList());
      final MotionTestSupport.Recorder recorder = new MotionTestSupport.Recorder();
      final H3MotionSegmenter<String> segmenter =
          H3MotionSegmenter.<String>builder(recorder).build();
      undeclared.forEach(segmenter::accept);
      segmenter.close();
      assertFalse(recorder.of(Type.GAP_START).isEmpty());
    }
  }

  /**
   * The same anchored vessel losing 10% of its raw reports: admission's first report after 10
   * minutes is then sometimes lost, so kept reports are up to two raw intervals past the max
   * interval apart. One lost report never gaps the thinned input; two in a row can.
   */
  @Test
  public void anchoredWithLostRawReportsStaysMostlyContinuous() {
    final MotionSource ais =
        MARITIME.withRawIntervalMillis(180_000L).withAllowedLatenessMillis(600_000L);
    final int seeds = 50;
    for (final long[] jitter : new long[][] {{0, 3_000L}, {-18_000L, 18_000L}}) {
      for (final boolean isolated : new boolean[] {true, false}) {
        int fullGaps = 0;
        int thinnedGaps = 0;
        int thinnedVisits = 0;
        for (int seed = 0; seed < seeds; seed++) {
          final List<MotionReport<String>> full =
              anchored(ais, new Random(seed), jitter, 0.1, isolated);
          fullGaps += gapsAndVisits(full)[0];
          final int[] thinned =
              gapsAndVisits(
                  admit(full, MARITIME.getToleranceMeters(), MARITIME.getMaxIntervalMillis()));
          thinnedGaps += thinned[0];
          thinnedVisits += thinned[1];
        }
        LOGGER.info(
            "Anchored, jitter {} to {} ms, 10% {} loss: {} gaps unthinned, {} gaps and {} visits "
                + "thinned over {} vessels",
            jitter[0],
            jitter[1],
            isolated ? "isolated" : "random",
            fullGaps,
            thinnedGaps,
            thinnedVisits,
            seeds);
        assertEquals(0, fullGaps);
        if (isolated) {
          assertEquals(0, thinnedGaps);
        } else {
          // two losses in a row: 16 gaps over 50 vessels, against 153 with a window that
          // covers no lost report
          assertTrue(thinnedGaps <= 16);
          assertEquals(seeds + thinnedGaps, thinnedVisits);
        }
      }
    }
  }

  /**
   * A vessel at anchor for 6 hours reporting every 3 minutes with jitter, losing {@code loss} of
   * its reports (never two in a row when {@code isolated}); the first and last are kept.
   */
  private static List<MotionReport<String>> anchored(
      final MotionSource source,
      final Random random,
      final long[] jitter,
      final double loss,
      final boolean isolated) {
    final List<MotionReport<String>> reports = new ArrayList<>();
    boolean lost = false;
    for (int i = 0; i < 120; i++) {
      final long time =
          T0 + (i * 180_000L) + jitter[0] + (long) (random.nextDouble() * (jitter[1] - jitter[0]));
      final double[] swung = MotionTestSupport.destination(1.24, 103.9, (360.0 * i) / 120, 80);
      final double[] p =
          MotionTestSupport.destination(
              swung[0],
              swung[1],
              random.nextDouble() * 360,
              Math.abs(random.nextGaussian() * 5));
      final boolean drop = (random.nextDouble() < loss) && (i > 0) && (i < 119);
      lost = drop && !(isolated && lost);
      if (!lost) {
        reports.add(
            new MotionReport<>(
                "anchored",
                true,
                MotionDomain.SURFACE,
                source,
                time,
                p[0],
                p[1],
                random.nextDouble() * 0.1,
                random.nextDouble() * 360,
                Double.NaN));
      }
    }
    return reports;
  }

  private static int[] gapsAndVisits(final List<MotionReport<String>> reports) {
    final MotionTestSupport.Recorder recorder = new MotionTestSupport.Recorder();
    final H3MotionSegmenter<String> segmenter = H3MotionSegmenter.<String>builder(recorder).build();
    reports.forEach(segmenter::accept);
    segmenter.close();
    return new int[] {
        recorder.of(Type.GAP_START, LEVEL).size(),
        recorder.of(Type.VISIT_END, LEVEL).size()};
  }

  /**
   * An anchored vessel whose reported speed is half-normal noise near the stop thresholds. A stop
   * ends on two fast reports spanning 30 s, which a 10 s cadence meets more often than a 3 minute
   * one or thinned input, so stop starts agree only within a tolerance that grows with the noise.
   */
  @Test
  public void anchoredSpeedNoiseStopStartsAgreeWithinATolerance() {
    // sigma, then the largest differences seen: 10 s against 3 minutes, against thinned, and
    // 3 minutes against thinned
    for (final double[] tolerance : new double[][] {{0.6, 3, 2, 3}, {1.0, 15, 17, 8}}) {
      final int[] worst = new int[3];
      final int[] total = new int[3];
      for (long seed = 1; seed <= 20; seed++) {
        final List<MotionReport<String>> full =
            noisyAnchor(new Random(seed), tolerance[0], 10_000L);
        final int dense = stopStarts(full);
        final int sparse = stopStarts(noisyAnchor(new Random(seed), tolerance[0], 180_000L));
        final int thinned =
            stopStarts(admit(full, MARITIME.getToleranceMeters(), MARITIME.getMaxIntervalMillis()));
        total[0] += dense;
        total[1] += sparse;
        total[2] += thinned;
        worst[0] = Math.max(worst[0], Math.abs(dense - sparse));
        worst[1] = Math.max(worst[1], Math.abs(dense - thinned));
        worst[2] = Math.max(worst[2], Math.abs(sparse - thinned));
      }
      LOGGER.info(
          "Speed noise sigma {} m/s at anchor for 6 h, 20 seeds: stop starts {}, {} and {} at 10 s, "
              + "3 minutes and thinned; largest differences {}, {} and {}",
          tolerance[0],
          total[0],
          total[1],
          total[2],
          worst[0],
          worst[1],
          worst[2]);
      for (int i = 0; i < worst.length; i++) {
        assertTrue(tolerance[0] + " difference " + i, worst[i] <= tolerance[i + 1]);
      }
    }
  }

  private static List<MotionReport<String>> noisyAnchor(
      final Random random,
      final double speedSigma,
      final long intervalMillis) {
    final List<MotionReport<String>> reports = new ArrayList<>();
    final double metersPerDegree = Math.toRadians(Spherical.EARTH_RADIUS_METERS);
    for (long t = 0; t <= (6 * 3_600_000L); t += intervalMillis) {
      final double east = gaussian(random, 5);
      final double north = gaussian(random, 5);
      reports.add(
          MotionTestSupport.report(
              "anchored",
              true,
              T0 + t,
              30 + (north / metersPerDegree),
              -40 + (east / (metersPerDegree * Math.cos(Math.toRadians(30)))),
              Math.abs(gaussian(random, speedSigma)),
              random.nextDouble() * 360));
    }
    return reports;
  }

  private static int stopStarts(final List<MotionReport<String>> reports) {
    final MotionTestSupport.Recorder recorder = new MotionTestSupport.Recorder();
    final H3MotionSegmenter<String> segmenter = H3MotionSegmenter.<String>builder(recorder).build();
    reports.forEach(segmenter::accept);
    segmenter.close();
    assertTrue(recorder.of(Type.GAP_START).isEmpty());
    return recorder.of(Type.STOP, LEVEL).size();
  }

  private static long stops(final Run run) {
    return run.byCell().values().stream().mapToLong(
        v -> v.getCount(MotionVectorLayout.STOP_STARTS)).sum();
  }

  private static double occupancy(final AdditiveVectorValue value) {
    return value == null ? 0
        : value.getSum(MotionVectorLayout.MOVING_SECONDS)
            + value.getSum(MotionVectorLayout.STOPPED_SECONDS);
  }

  private static double[] rose(final AdditiveVectorValue value) {
    final double[] rose = new double[MotionVectorLayout.HEADING_SECTORS];
    for (int i = 0; (value != null) && (i < rose.length); i++) {
      rose[i] = value.getCount(MotionVectorLayout.HEADING_ROSE + i);
    }
    return rose;
  }

  private static double[] row(final AdditiveVectorValue value, final int entrySlot) {
    final double[] row = new double[MotionVectorLayout.SLOTS];
    for (int exit = 0; (value != null) && (exit < row.length); exit++) {
      row[exit] = value.getCount(MotionVectorLayout.flowIndex(entrySlot, exit));
    }
    return row;
  }

  private static double sum(final double[] values) {
    double sum = 0;
    for (final double value : values) {
      sum += value;
    }
    return sum;
  }

  private static double totalVariation(final double[] p, final double[] q) {
    final double sp = sum(p);
    final double sq = sum(q);
    double variation = 0;
    for (int i = 0; i < p.length; i++) {
      variation += Math.abs((p[i] / sp) - (q[i] / sq));
    }
    return variation / 2;
  }

  private static double median(final List<Double> values) {
    final List<Double> sorted = new ArrayList<>(values);
    Collections.sort(sorted);
    final int middle = sorted.size() / 2;
    return (sorted.size() % 2) == 1 ? sorted.get(middle)
        : (sorted.get(middle - 1) + sorted.get(middle)) / 2;
  }

  /**
   * A vessel that anchors at the west end of the lane, sails east, anchors, sails back and anchors
   * again, simulated in one second steps and reported every ten seconds.
   */
  private static final class LaneVessel {
    private static final double LATITUDE = 30;
    private static final double LONGITUDE = -40;
    private static final double LANE_METERS = 60_000;
    private static final double ACCELERATION = 0.05;
    private static final double POSITION_NOISE = 5;

    private final String id;
    private final Random random;
    private final List<MotionReport<String>> reports = new ArrayList<>();
    private final double cruise;
    private final double meanderAmplitude;
    private final double meanderWavelength;
    private final double meanderPhase;
    private final double speedPhase;
    private long time;
    private long start;
    private double x;
    private double y;
    private double speed;
    private double course;

    LaneVessel(final String id, final Random random) {
      this.id = id;
      this.random = random;
      cruise = 6 + (6 * random.nextDouble());
      meanderAmplitude = 50 + (100 * random.nextDouble());
      meanderWavelength = 12_000 + (12_000 * random.nextDouble());
      meanderPhase = 2 * Math.PI * random.nextDouble();
      speedPhase = 2 * Math.PI * random.nextDouble();
    }

    List<MotionReport<String>> sail(final long departure) {
      start = departure;
      time = departure;
      anchor(1_800_000L + random.nextInt(600_000));
      leg(LANE_METERS, (random.nextDouble() - 0.5) * 2000);
      anchor(1_800_000L + random.nextInt(900_000));
      leg(0, (random.nextDouble() - 0.5) * 2000);
      anchor(1_800_000L);
      return reports;
    }

    private void anchor(final long millis) {
      speed = 0;
      final long end = time + millis;
      while (time < end) {
        tick();
      }
    }

    private void leg(final double toX, final double offset) {
      final double direction = Math.signum(toX - x);
      while (Math.abs(toX - x) > 1) {
        final double target = cruise * (1 + (0.08 * Math.sin((time / 1_500_000.0) + speedPhase)));
        final double braking = (speed * speed) / (2 * ACCELERATION);
        if (Math.abs(toX - x) <= (braking + 1)) {
          speed = Math.max(0.2, speed - ACCELERATION);
        } else if (speed < target) {
          speed = Math.min(target, speed + ACCELERATION);
        } else {
          speed = Math.max(target, speed - ACCELERATION);
        }
        final double slope = slope(x, offset);
        final double dx = (direction * speed) / Math.sqrt(1 + (slope * slope));
        final double nextX = Math.abs(toX - x) <= Math.abs(dx) ? toX : x + dx;
        final double nextY = lateral(nextX, offset);
        course = (Math.toDegrees(Math.atan2(nextX - x, nextY - y)) + 360) % 360;
        x = nextX;
        y = nextY;
        tick();
      }
      speed = 0;
    }

    private double lateral(final double at, final double offset) {
      final double envelope = Math.sin((Math.PI * at) / LANE_METERS);
      return envelope
          * (offset
              + (meanderAmplitude
                  * Math.sin(((2 * Math.PI * at) / meanderWavelength) + meanderPhase)));
    }

    private double slope(final double at, final double offset) {
      return (lateral(at + 0.5, offset) - lateral(at - 0.5, offset));
    }

    private void tick() {
      time += 1000;
      if (((time - start) % 10_000L) != 0) {
        return;
      }
      final double noisyX = x + gaussian(random, POSITION_NOISE);
      final double noisyY = y + gaussian(random, POSITION_NOISE);
      final double latitude = LATITUDE + Math.toDegrees(noisyY / Spherical.EARTH_RADIUS_METERS);
      final double longitude =
          LONGITUDE
              + Math.toDegrees(
                  noisyX / (Spherical.EARTH_RADIUS_METERS * Math.cos(Math.toRadians(LATITUDE))));
      final boolean anchored = speed == 0;
      final double reportedSpeed =
          anchored ? Math.abs(gaussian(random, 0.08)) : Math.max(0, speed + gaussian(random, 0.05));
      final double reportedCourse =
          anchored ? random.nextDouble() * 360 : (course + gaussian(random, 1) + 360) % 360;
      reports.add(
          MotionTestSupport.report(
              id,
              true,
              time,
              latitude,
              longitude,
              reportedSpeed,
              reportedCourse));
    }
  }
}
