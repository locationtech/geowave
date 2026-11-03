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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.locationtech.geowave.core.geotime.motion.MotionTestSupport.MARITIME;
import static org.locationtech.geowave.core.geotime.motion.MotionTestSupport.T0;
import static org.locationtech.geowave.core.geotime.motion.MotionTestSupport.destination;
import static org.locationtech.geowave.core.geotime.motion.MotionTestSupport.report;
import static org.locationtech.geowave.core.geotime.motion.MotionTestSupport.straightTrack;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.motion.MotionSlice.Type;
import org.locationtech.geowave.core.geotime.motion.MotionTestSupport.Recorder;
import org.locationtech.geowave.core.store.statistics.adapter.AdditiveVectorStatistic.AdditiveVectorValue;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;

public class H3MotionSegmenterTest {
  private static final H3Core H3 = H3Holder.h3();
  private static final int[] SURFACE_LEVELS = {6, 4};
  private static final long WINDOW = 720_000L;

  private static H3MotionSegmenter<String> segmenter(final Recorder recorder) {
    return H3MotionSegmenter.<String>builder(recorder).build();
  }

  private static void run(
      final H3MotionSegmenter<String> segmenter,
      final List<MotionReport<String>> reports) {
    reports.forEach(segmenter::accept);
    segmenter.close();
  }

  @Test
  public void darkPeriodYieldsOneGapPairAndNoOccupancyAcrossIt() {
    final long darkStart = T0 + 3_600_000L;
    final long darkEnd = darkStart + 1_800_000L;
    for (final boolean streaming : new boolean[] {false, true}) {
      final Recorder recorder = new Recorder();
      final H3MotionSegmenter<String> segmenter = segmenter(recorder);
      final List<MotionReport<String>> track =
          straightTrack("v", true, 10, 20, 80, 8, T0, 10_000L, 1 + (2 * 360) + 180);
      int gapStartIndex = -1;
      for (final MotionReport<String> report : track) {
        if ((report.getTime() > darkStart) && (report.getTime() < darkEnd)) {
          if (streaming && ((report.getTime() % 60_000L) == 0)) {
            segmenter.advanceTo(report.getTime());
            if ((gapStartIndex < 0) && !recorder.of(Type.GAP_START).isEmpty()) {
              gapStartIndex = recorder.slices.size();
              assertTrue(report.getTime() - darkStart > WINDOW);
              assertTrue(report.getTime() - darkStart <= WINDOW + 60_000L);
            }
          }
          continue;
        }
        segmenter.accept(report);
      }
      segmenter.close();
      if (streaming) {
        assertTrue("the gap starts before the entity reappears", gapStartIndex > 0);
      }
      for (final int level : SURFACE_LEVELS) {
        final List<MotionSlice<String>> starts = recorder.of(Type.GAP_START, level);
        final List<MotionSlice<String>> ends = recorder.of(Type.GAP_END, level);
        assertEquals(1, starts.size());
        assertEquals(1, ends.size());
        assertEquals(darkStart, starts.get(0).getStartTime());
        assertEquals(1800, ends.get(0).getDurationSeconds(), 0);
        assertFalse(ends.get(0).isCensored());
        assertEquals(starts.get(0).getCell(), ends.get(0).getCell());
        assertNotEquals(0, ends.get(0).getOtherCell());
        assertEquals(darkStart - (darkStart % 3_600_000L), ends.get(0).getBucketStart());
        for (final MotionSlice<String> slice : recorder.of(Type.OCCUPANCY, level)) {
          if (!slice.isBridged()) {
            assertTrue(
                slice.toString(),
                (slice.getEndTime() <= darkStart) || (slice.getStartTime() >= darkEnd));
          }
        }
        assertEquals(1800, recorder.seconds(level, true), 1e-6);
        final double observed = (track.get(track.size() - 1).getTime() - T0) / 1000.0 - 1800;
        assertEquals(observed, recorder.seconds(level, false), 1e-6);
        assertEquals(
            1,
            recorder.of(Type.VISIT_END, level).stream().filter(
                s -> s.isCensored() && (s.getEndTime() == darkStart)).count());
      }
    }
  }

  @Test
  public void implausibleJumpBreaksContinuityWithoutBridging() {
    final Recorder recorder = new Recorder();
    final H3MotionSegmenter<String> segmenter = segmenter(recorder);
    final List<MotionReport<String>> reports =
        new ArrayList<>(straightTrack("v", true, 10, 20, 90, 8, T0, 10_000L, 30));
    final double[] far = destination(10, 20, 0, 100_000);
    reports.add(report("v", true, T0 + 300_000L, far[0], far[1], 8, 90));
    reports.add(report("v", true, T0 + 310_000L, far[0], far[1] + 0.001, 8, 90));
    run(segmenter, reports);
    assertEquals(1, segmenter.getImplausibleJumps());
    assertEquals(1, recorder.of(Type.GAP_START, 6).size());
    assertEquals(1, recorder.of(Type.GAP_END, 6).size());
    assertEquals(0, recorder.seconds(6, true), 0);
  }

  @Test
  public void outOfOrderReportsAreDroppedDeterministically() {
    final Random random = new Random(7);
    final List<MotionReport<String>> track =
        straightTrack("v", true, 30, -40, 60, 9, T0, 10_000L, 400);
    final List<MotionReport<String>> arrival = new ArrayList<>(track);
    for (int i = 0; i < 25; i++) {
      final int index = 1 + random.nextInt(arrival.size() - 3);
      final MotionReport<String> swapped = arrival.get(index);
      arrival.set(index, arrival.get(index + 1));
      arrival.set(index + 1, swapped);
    }
    final List<MotionReport<String>> expected = new ArrayList<>();
    long last = Long.MIN_VALUE;
    for (final MotionReport<String> report : arrival) {
      if (report.getTime() > last) {
        expected.add(report);
        last = report.getTime();
      }
    }
    assertTrue(expected.size() < arrival.size());

    final Recorder shuffled = new Recorder();
    final H3MotionSegmenter<String> segmenter = segmenter(shuffled);
    run(segmenter, arrival);
    final Recorder ordered = new Recorder();
    run(segmenter(ordered), expected);
    assertEquals(arrival.size() - expected.size(), segmenter.getOutOfOrderReports());
    assertEquals(describe(ordered.slices), describe(shuffled.slices));
  }

  @Test
  public void interleavedEntitiesDoNotAffectEachOther() {
    final List<MotionReport<String>> a = straightTrack("a", true, 30, -40, 60, 9, T0, 10_000L, 300);
    final List<MotionReport<String>> b =
        straightTrack("b", true, 30.02, -40.01, 75, 7, T0 + 3_000L, 7_000L, 400);
    final List<MotionReport<String>> merged = new ArrayList<>(a);
    merged.addAll(b);
    merged.sort(Comparator.comparingLong(MotionReport::getTime));
    final MotionVectorAccumulator together =
        new MotionVectorAccumulator(MotionVectorLayout.defaults(MotionDomain.SURFACE));
    final H3MotionSegmenter<String> segmenter = H3MotionSegmenter.<String>builder(together).build();
    for (final MotionReport<String> report : merged) {
      segmenter.accept(report);
      segmenter.advanceTo(report.getTime());
    }
    segmenter.close();
    final MotionVectorAccumulator apart =
        new MotionVectorAccumulator(MotionVectorLayout.defaults(MotionDomain.SURFACE));
    final H3MotionSegmenter<String> first = H3MotionSegmenter.<String>builder(apart).build();
    a.forEach(first::accept);
    first.close();
    final H3MotionSegmenter<String> second = H3MotionSegmenter.<String>builder(apart).build();
    b.forEach(second::accept);
    second.close();
    assertSameValues(apart.getValues(), together.getValues());
  }

  @Test
  public void stopWithJitterAnchorsTheCell() {
    final long cell = H3.latLngToCell(30, -40, 6);
    final List<LatLng> boundary = H3.cellToBoundary(cell);
    final LatLng center = H3.cellToLatLng(cell);
    final double edgeLat = (boundary.get(0).lat + boundary.get(1).lat) / 2;
    final double edgeLon = (boundary.get(0).lng + boundary.get(1).lng) / 2;
    final double inward = Spherical.bearing(edgeLat, edgeLon, center.lat, center.lng);
    final double[] anchor = destination(edgeLat, edgeLon, inward, 20);
    assertEquals(cell, H3.latLngToCell(anchor[0], anchor[1], 6));
    // arrive from the neighbor across the edge, stop 20 m inside it, and leave the way we came
    final double[] approach = destination(anchor[0], anchor[1], inward + 180, 3000);
    final double inbound = Spherical.bearing(approach[0], approach[1], anchor[0], anchor[1]);
    final List<MotionReport<String>> reports = new ArrayList<>();
    for (int i = 0; i <= 30; i++) {
      final double[] p = destination(approach[0], approach[1], inbound, 100 * i);
      reports.add(report("v", true, T0 + (i * 10_000L), p[0], p[1], i == 30 ? 0.1 : 10, inbound));
    }
    final Random random = new Random(11);
    final long stopStart = T0 + 300_000L;
    final long stopEnd = stopStart + 7_200_000L;
    for (long time = stopStart + 10_000L; time <= stopEnd; time += 10_000L) {
      final double[] p =
          destination(
              anchor[0],
              anchor[1],
              random.nextDouble() * 360,
              Math.abs(random.nextGaussian()) * 30);
      reports.add(report("v", true, time, p[0], p[1], Math.abs(random.nextGaussian()) * 0.2, 0));
    }
    for (int i = 1; i <= 60; i++) {
      final double[] p = destination(anchor[0], anchor[1], inward + 180, 80 * i);
      reports.add(report("v", true, stopEnd + (i * 10_000L), p[0], p[1], 8, inward + 180));
    }
    final Recorder recorder = new Recorder();
    run(segmenter(recorder), reports);

    // the fourth fast report spans 30 s, so the stop ends at the third
    final long resumed = stopEnd + 30_000L;
    for (final int level : SURFACE_LEVELS) {
      assertEquals(1, recorder.of(Type.STOP, level).size());
      assertEquals(1, recorder.of(Type.START, level).size());
      assertEquals(stopStart, recorder.of(Type.STOP, level).get(0).getStartTime());
      assertEquals(resumed, recorder.of(Type.START, level).get(0).getStartTime());
    }
    assertTrue(
        recorder.where(
            s -> ((s.getType() == Type.TRANSITION) || (s.getType() == Type.VISIT_END))
                && (s.getEndTime() > stopStart)
                && (s.getEndTime() < resumed)).isEmpty());
    final Set<Long> stoppedCells =
        recorder.of(Type.OCCUPANCY, 6).stream().filter(s -> s.getStoppedSeconds() > 0).map(
            MotionSlice::getCell).collect(Collectors.toSet());
    assertEquals(Set.of(cell), stoppedCells);
    assertEquals(
        (resumed - stopStart) / 1000.0,
        recorder.of(Type.OCCUPANCY, 6).stream().mapToDouble(MotionSlice::getStoppedSeconds).sum(),
        1e-6);
    // the stop splits the visit into a passage that ends here and one that begins here
    final List<String> passages =
        recorder.where(
            s -> (s.getType() == Type.TRANSITION)
                && (s.getLevel() == 6)
                && (s.getCell() == cell)).stream().map(
                    s -> (s.getEntrySlot() < MotionSlice.ENDPOINT ? "neighbor" : "endpoint")
                        + ">"
                        + (s.getExitSlot() < MotionSlice.ENDPOINT ? "neighbor"
                            : "endpoint")).collect(Collectors.toList());
    assertEquals(List.of("neighbor>endpoint", "endpoint>neighbor"), passages);
    final List<MotionSlice<String>> visits =
        recorder.where(
            s -> (s.getType() == Type.VISIT_END) && (s.getLevel() == 6) && (s.getCell() == cell));
    assertEquals(1, visits.size());
    assertTrue(Double.isNaN(visits.get(0).getTraversalSeconds()));
  }

  @Test
  public void shallowExcursionIsOneVisit() {
    final long cell = H3.latLngToCell(30, -40, 6);
    final List<LatLng> boundary = H3.cellToBoundary(cell);
    final LatLng center = H3.cellToLatLng(cell);
    final double edgeLat = (boundary.get(0).lat + boundary.get(1).lat) / 2;
    final double edgeLon = (boundary.get(0).lng + boundary.get(1).lng) / 2;
    final double outward = Spherical.bearing(center.lat, center.lng, edgeLat, edgeLon);
    for (final double depth : new double[] {100, 600}) {
      final double[] inside = destination(edgeLat, edgeLon, outward + 180, 2000);
      final double[] beyond = destination(edgeLat, edgeLon, outward, depth);
      final long neighbor = H3.latLngToCell(beyond[0], beyond[1], 6);
      assertNotEquals(cell, neighbor);
      assertTrue(H3.areNeighborCells(cell, neighbor));
      final List<MotionReport<String>> reports = new ArrayList<>();
      reports.add(report("v", true, T0, inside[0], inside[1], 8, outward));
      reports.add(report("v", true, T0 + 300_000L, beyond[0], beyond[1], 8, outward));
      reports.add(report("v", true, T0 + 600_000L, center.lat, center.lng, 8, outward + 180));
      final Recorder recorder = new Recorder();
      run(segmenter(recorder), reports);
      final List<Long> visited =
          recorder.of(Type.VISIT_END, 6).stream().map(MotionSlice::getCell).collect(
              Collectors.toList());
      assertTrue(
          recorder.of(Type.OCCUPANCY, 6).stream().anyMatch(
              s -> (s.getCell() == neighbor) && (s.getMovingSeconds() > 0)));
      if (depth < MARITIME.getToleranceMeters()) {
        assertEquals(List.of(cell), visited);
        assertTrue(recorder.of(Type.VISIT_END, 6).get(0).getDistanceMeters() > 4000);
      } else {
        assertEquals(List.of(cell, neighbor, cell), visited);
      }
    }
  }

  @Test
  public void occupancySplitAtBucketBoundariesSumsToTheTotal() {
    final List<MotionReport<String>> track =
        straightTrack("v", true, 30, -40, 100, 9, T0, 10_000L, 1 + (4 * 360));
    final double duration = (track.get(track.size() - 1).getTime() - T0) / 1000.0;
    final LongUnaryOperator sevenMinutes = t -> (Math.floorDiv(t - 123, 420_000L) * 420_000L) + 123;
    for (final LongUnaryOperator buckets : List.of(H3MotionSegmenter.HOUR, sevenMinutes)) {
      final Recorder recorder = new Recorder();
      run(H3MotionSegmenter.<String>builder(recorder).bucketFunction(buckets).build(), track);
      for (final int level : SURFACE_LEVELS) {
        final List<MotionSlice<String>> pieces = recorder.of(Type.OCCUPANCY, level);
        assertEquals(duration, recorder.seconds(level, false), 1e-6);
        final Set<Long> distinct = new HashSet<>();
        for (final MotionSlice<String> piece : pieces) {
          assertEquals(buckets.applyAsLong(piece.getStartTime()), piece.getBucketStart());
          assertEquals(
              piece.toString(),
              piece.getBucketStart(),
              buckets.applyAsLong(Math.max(piece.getStartTime(), piece.getEndTime() - 1)));
          distinct.add(piece.getBucketStart());
        }
        assertTrue(distinct.size() >= 4);
      }
    }
  }

  @Test
  public void coarseOccupancyEqualsTheSumOverFineChildren() {
    final MotionVectorAccumulator accumulator =
        new MotionVectorAccumulator(MotionVectorLayout.defaults(MotionDomain.SURFACE));
    final H3MotionSegmenter<String> segmenter =
        H3MotionSegmenter.<String>builder(accumulator).build();
    final Random random = new Random(3);
    for (int v = 0; v < 5; v++) {
      randomWalk("w" + v, random, T0 + (v * 600_000L)).forEach(segmenter::accept);
    }
    segmenter.close();
    final Map<MotionCellKey, AdditiveVectorValue> values = accumulator.getValues();
    final Map<MotionCellKey, double[]> rolledUp = new HashMap<>();
    for (final Map.Entry<MotionCellKey, AdditiveVectorValue> entry : values.entrySet()) {
      final MotionCellKey key = entry.getKey();
      if (key.getLevel() == 6) {
        final MotionCellKey parent =
            new MotionCellKey(key.getBucketStart(), 4, H3.cellToParent(key.getCell(), 4));
        final double[] sums = rolledUp.computeIfAbsent(parent, k -> new double[5]);
        final AdditiveVectorValue value = entry.getValue();
        sums[0] += value.getSum(MotionVectorLayout.MOVING_SECONDS);
        sums[1] += value.getSum(MotionVectorLayout.STOPPED_SECONDS);
        sums[2] += value.getSum(MotionVectorLayout.DISTANCE_METERS);
        sums[3] += value.getCount(MotionVectorLayout.STOP_STARTS);
        sums[4] += value.getCount(MotionVectorLayout.GAP_STARTS);
      }
    }
    final Set<MotionCellKey> coarse =
        values.keySet().stream().filter(k -> k.getLevel() == 4).collect(Collectors.toSet());
    assertEquals(coarse, rolledUp.keySet());
    double stops = 0;
    for (final MotionCellKey key : coarse) {
      final AdditiveVectorValue value = values.get(key);
      final double[] sums = rolledUp.get(key);
      assertEquals(value.getSum(MotionVectorLayout.MOVING_SECONDS), sums[0], 1e-6);
      assertEquals(value.getSum(MotionVectorLayout.STOPPED_SECONDS), sums[1], 1e-6);
      assertEquals(value.getSum(MotionVectorLayout.DISTANCE_METERS), sums[2], 1e-6);
      assertEquals(value.getCount(MotionVectorLayout.STOP_STARTS), sums[3], 0);
      assertEquals(value.getCount(MotionVectorLayout.GAP_STARTS), sums[4], 0);
      stops += sums[3];
    }
    assertTrue("the walks stop", stops > 0);
  }

  @Test
  public void pathsThroughPentagonsAntimeridianAndPoleStayContinuous() {
    for (final int[] plan : new int[][] {{6, 4}, {7, 5}, {5, 3}}) {
      final ResolutionPlan levels =
          ResolutionPlan.builder().levels(MotionDomain.SURFACE, plan).build();
      final long pentagon = H3.getPentagons(plan[0]).iterator().next();
      final LatLng pentagonCenter = H3.cellToLatLng(pentagon);
      // about eight cells at 10 m/s, a report every 30 s
      final double length = 16 * H3Holder.averageEdgeMeters(plan[0]);
      final int count = 1 + (int) Math.ceil(length / 300);
      final double half = Math.toDegrees(length / 2 / Spherical.EARTH_RADIUS_METERS);
      final double[] beforePentagon =
          destination(pentagonCenter.lat, pentagonCenter.lng, 50, length / 2);
      final double towardPentagon =
          Spherical.bearing(
              beforePentagon[0],
              beforePentagon[1],
              pentagonCenter.lat,
              pentagonCenter.lng);
      final List<List<MotionReport<String>>> tracks =
          List.of(
              straightTrack(
                  "pentagon",
                  true,
                  beforePentagon[0],
                  beforePentagon[1],
                  towardPentagon,
                  10,
                  T0,
                  30_000L,
                  count),
              straightTrack("antimeridian", true, 0.05, 180 - half, 90, 10, T0, 30_000L, count),
              straightTrack("pole", true, 90 - half, 10, 0, 10, T0, 30_000L, count));
      boolean sawPentagon = false;
      for (final List<MotionReport<String>> track : tracks) {
        final Recorder recorder = new Recorder();
        final H3MotionSegmenter<String> segmenter =
            H3MotionSegmenter.<String>builder(recorder).resolutionPlan(levels).build();
        run(segmenter, track);
        final String label = track.get(0).getEntityKey() + " at " + plan[0] + "/" + plan[1];
        final double duration = (track.get(track.size() - 1).getTime() - T0) / 1000.0;
        assertEquals(label, 0, segmenter.getImplausibleJumps());
        assertTrue(label, recorder.of(Type.GAP_START).isEmpty());
        for (final int level : plan) {
          assertEquals(label, duration, recorder.seconds(level, false), 1e-6);
          assertEquals(
              label,
              duration * 10,
              recorder.of(Type.OCCUPANCY, level).stream().mapToDouble(
                  MotionSlice::getDistanceMeters).sum(),
              1);
        }
        final List<MotionSlice<String>> visits = recorder.of(Type.VISIT_END, plan[0]);
        assertTrue(label, visits.size() >= 4);
        for (int i = 0; i < visits.size(); i++) {
          final MotionSlice<String> visit = visits.get(i);
          if (visit.getCell() == pentagon) {
            sawPentagon = true;
            assertNotEquals(label, 0, visit.getEntrySlot());
            assertNotEquals(label, 0, visit.getExitSlot());
          }
          if (i + 1 < visits.size()) {
            final MotionSlice<String> next = visits.get(i + 1);
            assertTrue(label, H3.areNeighborCells(visit.getCell(), next.getCell()));
            assertEquals(label, digit(visit.getCell(), next.getCell()) - 1, visit.getExitSlot());
            assertEquals(label, digit(next.getCell(), visit.getCell()) - 1, next.getEntrySlot());
            assertEquals(label, visit.getEndTime(), next.getStartTime());
          }
        }
      }
      assertTrue(sawPentagon);
    }
  }

  private static int digit(final long origin, final long destination) {
    return (int) ((H3.cellsToDirectedEdge(origin, destination) >>> 56) & 7);
  }

  @Test
  public void unstableIdentitiesContributeOccupancyAndTransitionsOnly() {
    final List<MotionReport<String>> stable = randomWalk("s", new Random(5), T0);
    final List<MotionReport<String>> unstable =
        stable.stream().map(
            r -> new MotionReport<>(
                "u",
                false,
                r.getDomain(),
                r.getSource(),
                r.getTime(),
                r.getLatitude(),
                r.getLongitude(),
                r.getSpeed(),
                r.getCourse(),
                r.getAltitude())).collect(Collectors.toList());
    final Recorder stableRecorder = new Recorder();
    run(segmenter(stableRecorder), stable);
    final Recorder unstableRecorder = new Recorder();
    run(segmenter(unstableRecorder), unstable);
    assertFalse(stableRecorder.of(Type.GAP_START).isEmpty());
    assertFalse(stableRecorder.of(Type.STOP).isEmpty());
    // dwell counts only stable occupancy, since unstable entities have no visits
    final MotionVectorAccumulator accumulator =
        new MotionVectorAccumulator(MotionVectorLayout.defaults(MotionDomain.SURFACE));
    stableRecorder.slices.forEach(accumulator);
    unstableRecorder.slices.forEach(accumulator);
    double stableSeconds = 0;
    double allSeconds = 0;
    for (final Map.Entry<MotionCellKey, AdditiveVectorValue> entry : accumulator.getValues().entrySet()) {
      if (entry.getKey().getLevel() == 6) {
        stableSeconds += entry.getValue().getSum(MotionVectorLayout.STABLE_SECONDS);
        allSeconds +=
            entry.getValue().getSum(MotionVectorLayout.MOVING_SECONDS)
                + entry.getValue().getSum(MotionVectorLayout.STOPPED_SECONDS);
      }
    }
    assertEquals(stableRecorder.seconds(6, false), stableSeconds, 1e-6);
    assertEquals(2 * stableSeconds, allSeconds, 1e-6);
    final Set<Type> types =
        unstableRecorder.slices.stream().map(MotionSlice::getType).collect(Collectors.toSet());
    assertEquals(Set.of(Type.OCCUPANCY, Type.TRANSITION), types);
    for (final int level : SURFACE_LEVELS) {
      assertEquals(
          stableRecorder.seconds(level, false),
          unstableRecorder.seconds(level, false),
          1e-6);
      assertEquals(
          neighborTransitions(stableRecorder, level),
          neighborTransitions(unstableRecorder, level));
    }
  }

  private static List<String> neighborTransitions(final Recorder recorder, final int level) {
    return recorder.of(Type.TRANSITION, level).stream().filter(
        s -> (s.getEntrySlot() < MotionSlice.ENDPOINT)
            && (s.getExitSlot() < MotionSlice.ENDPOINT)).map(
                s -> s.getCell()
                    + ":"
                    + s.getEntrySlot()
                    + ">"
                    + s.getExitSlot()
                    + "@"
                    + s.getEndTime()).collect(Collectors.toList());
  }

  @Test
  public void accumulatingTwoHalvesEqualsTheWhole() {
    final List<MotionReport<String>> reports = new ArrayList<>();
    for (int v = 0; v < 6; v++) {
      reports.addAll(
          straightTrack(
              "v" + v,
              true,
              30 + (0.03 * v),
              -40,
              30 * v,
              6 + v,
              T0 + (v * 60_000L),
              10_000L,
              1200));
    }
    reports.sort(Comparator.comparingLong(MotionReport::getTime));
    final MotionVectorLayout layout = MotionVectorLayout.defaults(MotionDomain.SURFACE);

    final MotionVectorAccumulator whole = new MotionVectorAccumulator(layout);
    run(H3MotionSegmenter.<String>builder(whole).build(), reports);

    final MotionVectorAccumulator streamed = new MotionVectorAccumulator(layout);
    final H3MotionSegmenter<String> segmenter = H3MotionSegmenter.<String>builder(streamed).build();
    final long boundary = T0 + 5_400_000L;
    Map<MotionCellKey, AdditiveVectorValue> firstHalf = null;
    for (final MotionReport<String> report : reports) {
      segmenter.accept(report);
      segmenter.advanceTo(report.getTime());
      if ((firstHalf == null) && (segmenter.getCompleteThrough() >= boundary)) {
        firstHalf = streamed.drain();
      }
    }
    segmenter.close();
    final Map<MotionCellKey, AdditiveVectorValue> secondHalf = streamed.drain();
    assertTrue(firstHalf != null);
    assertTrue(
        "nothing lands before the watermark",
        secondHalf.keySet().stream().noneMatch(k -> k.getBucketStart() < boundary));

    final Map<MotionCellKey, AdditiveVectorValue> merged = new HashMap<>();
    for (final Map<MotionCellKey, AdditiveVectorValue> half : List.of(firstHalf, secondHalf)) {
      half.forEach(
          (key, value) -> merged.computeIfAbsent(key, k -> layout.createValue(null)).add(value));
    }
    assertSameValues(whole.getValues(), merged);
  }

  @Test
  public void evictionClosesVisitsAsAGap() {
    final Recorder recorder = new Recorder();
    final H3MotionSegmenter<String> segmenter =
        H3MotionSegmenter.<String>builder(recorder).maxEntities(2).idleTtlMillis(
            3_600_000L).build();
    segmenter.accept(report("a", true, T0, 30, -40, 5, 0));
    segmenter.accept(report("b", true, T0 + 1_000L, 31, -40, 5, 0));
    segmenter.accept(report("c", true, T0 + 2_000L, 32, -40, 5, 0));
    assertEquals(2, segmenter.getEntityCount());
    assertEquals(1, segmenter.getEvictions());
    assertEquals(
        List.of("a"),
        recorder.of(Type.GAP_START, 6).stream().map(MotionSlice::getEntityKey).collect(
            Collectors.toList()));
    final MotionSlice<String> censoredEnd = recorder.of(Type.GAP_END, 6).get(0);
    assertTrue(censoredEnd.isCensored());
    assertEquals(0, censoredEnd.getOtherCell());
    assertEquals(2, censoredEnd.getDurationSeconds(), 0);
    assertTrue(recorder.of(Type.VISIT_END, 6).get(0).isCensored());

    segmenter.advanceTo(T0 + WINDOW + 1_500L);
    assertEquals(
        1,
        recorder.of(Type.GAP_START, 6).stream().filter(s -> s.getEntityKey().equals("b")).count());
    assertEquals(
        0,
        recorder.of(Type.GAP_START, 6).stream().filter(s -> s.getEntityKey().equals("c")).count());
    assertEquals(T0 + 1_500L, segmenter.getCompleteThrough());

    segmenter.advanceTo(T0 + (3 * 3_600_000L));
    assertEquals(0, segmenter.getEntityCount());
    assertEquals(3, segmenter.getEvictions());
    assertEquals(3, recorder.of(Type.GAP_END, 6).size());
    assertTrue(recorder.of(Type.GAP_END, 6).stream().allMatch(MotionSlice::isCensored));
  }

  @Test
  public void endingAnEntityIsNotAGap() {
    final Recorder recorder = new Recorder();
    final H3MotionSegmenter<String> segmenter = segmenter(recorder);
    straightTrack("v", true, 30, -40, 90, 8, T0, 10_000L, 100).forEach(segmenter::accept);
    segmenter.end("v");
    assertEquals(0, segmenter.getEntityCount());
    assertTrue(recorder.of(Type.GAP_START).isEmpty());
    final List<MotionSlice<String>> ends = recorder.of(Type.VISIT_END, 6);
    final MotionSlice<String> last = ends.get(ends.size() - 1);
    assertFalse(last.isCensored());
    assertEquals(MotionSlice.ENDPOINT, last.getExitSlot());
  }

  @Test
  public void closeGivesStaleEntitiesAGap() {
    final Recorder recorder = new Recorder();
    final H3MotionSegmenter<String> segmenter = segmenter(recorder);
    segmenter.accept(report("stale", true, T0, 30, -40, 5, 0));
    segmenter.accept(report("fresh", true, T0 + 3_600_000L, 31, -40, 5, 0));
    segmenter.close();
    assertEquals(
        List.of("stale"),
        recorder.of(Type.GAP_START, 6).stream().map(MotionSlice::getEntityKey).collect(
            Collectors.toList()));
    final MotionSlice<String> gap = recorder.of(Type.GAP_END, 6).get(0);
    assertTrue(gap.isCensored());
    assertEquals(3600, gap.getDurationSeconds(), 0);
    assertEquals(
        2,
        recorder.of(Type.VISIT_END, 6).stream().filter(MotionSlice::isCensored).count());
  }

  @Test
  public void watermarkTrailsByTheLargestWindow() {
    final H3MotionSegmenter<String> segmenter = segmenter(new Recorder());
    assertEquals(Long.MIN_VALUE, segmenter.getCompleteThrough());
    segmenter.accept(report("v", true, T0, 30, -40, 5, 0));
    assertEquals(T0 - WINDOW, segmenter.getCompleteThrough());
    final MotionSource air = new MotionSource("air", 1000, 30, 60_000L);
    segmenter.accept(new MotionReport<>("a", true, MotionDomain.AIR, air, T0 + 60_000L, 30, -40));
    assertEquals(T0 + 60_000L - WINDOW, segmenter.getCompleteThrough());
    segmenter.advanceTo(T0 + 600_000L);
    assertEquals(T0 + 600_000L - WINDOW, segmenter.getCompleteThrough());
  }

  @Test
  public void leavingAStopByItsRadiusIsNotAJump() {
    final List<MotionReport<String>> reports = new ArrayList<>();
    long time = T0;
    for (; time < (T0 + 600_000L); time += 10_000L) {
      reports.add(report("v", true, time, 30, -40, 0.1, 0));
    }
    // creeping at 0.8 m/s is never fast, so only the 420 m radius ends the stop
    final long creepStart = time;
    for (; time <= (creepStart + 1_200_000L); time += 10_000L) {
      final double[] p = destination(30, -40, 90, (0.8 * (time - creepStart)) / 1000.0);
      reports.add(report("v", true, time, p[0], p[1], 0.8, 90));
    }
    final Recorder recorder = new Recorder();
    final H3MotionSegmenter<String> segmenter = segmenter(recorder);
    run(segmenter, reports);
    assertEquals(0, segmenter.getImplausibleJumps());
    assertTrue(recorder.of(Type.GAP_START).isEmpty());
    assertEquals(1, recorder.of(Type.START, 6).size());
    final double leftAt =
        (0.8 * (recorder.of(Type.START, 6).get(0).getStartTime() - creepStart)) / 1000.0;
    assertTrue(leftAt + " m", (leftAt > 410) && (leftAt <= 420));
    assertEquals((time - 10_000L - T0) / 1000.0, recorder.seconds(6, false), 1e-6);
  }

  @Test
  public void aDuplicateReceptionInsideTheStopRadiusIsPlausible() {
    final double[] moored = destination(30, -40, 45, 120);
    final List<MotionReport<String>> reports = new ArrayList<>();
    reports.add(report("v", true, T0, 30, -40, 0.1, 0));
    for (int k = 1; k < 30; k++) {
      final long time = T0 + (k * 60_000L);
      reports.add(report("v", true, time, moored[0], moored[1], 0.1, 0));
      if (k == 15) {
        reports.add(report("v", true, time + 1_000L, moored[0], moored[1], 0.1, 0));
      }
    }
    final Recorder recorder = new Recorder();
    final H3MotionSegmenter<String> segmenter = segmenter(recorder);
    run(segmenter, reports);
    assertEquals(0, segmenter.getImplausibleJumps());
    assertTrue(recorder.of(Type.GAP_START).isEmpty());
    assertTrue(recorder.of(Type.START).isEmpty());
    assertEquals(29 * 60, recorder.seconds(6, false), 1e-6);
  }

  @Test
  public void speedlessReportsMeasureSpeedOverABaseline() {
    final MotionSource ground = new MotionSource("ground", 300, 20, 600_000L);
    final MotionSource gps = new MotionSource("gps", 20, 5, 600_000L);
    final Object[][] cases = {{ground, 5.0, 5_000L}, {ground, 5.0, 1_000L}, {gps, 1.4, 5_000L}};
    for (final Object[] c : cases) {
      final MotionSource source = (MotionSource) c[0];
      final double speed = (Double) c[1];
      final long cadence = (Long) c[2];
      final int count = 1 + (int) (1_800_000L / cadence);
      final List<MotionReport<String>> reports =
          speedless(
              straightTrack("g", true, 30, -40, 70, speed, T0, cadence, count),
              MotionDomain.GROUND,
              source);
      final Recorder recorder = new Recorder();
      final H3MotionSegmenter<String> segmenter = segmenter(recorder);
      run(segmenter, reports);
      final String label = source.getName() + " at " + speed + " m/s every " + cadence + " ms";
      assertEquals(label, 0, segmenter.getImplausibleJumps());
      assertTrue(label, recorder.of(Type.STOP).isEmpty());
      assertEquals(label, 1800, recorder.seconds(7, false), 1e-6);
    }
    // driving for 10 minutes, then parked with 20 m of jitter
    final Random random = new Random(13);
    final List<MotionReport<String>> reports = new ArrayList<>();
    final double[] parked = destination(30, -40, 70, 3000);
    for (long time = T0; time <= (T0 + 1_800_000L); time += 5_000L) {
      final double[] p =
          time <= (T0 + 600_000L) ? destination(30, -40, 70, (5.0 * (time - T0)) / 1000.0)
              : destination(
                  parked[0],
                  parked[1],
                  random.nextDouble() * 360,
                  Math.abs(random.nextGaussian()) * 20);
      reports.add(new MotionReport<>("g", true, MotionDomain.GROUND, ground, time, p[0], p[1]));
    }
    final Recorder recorder = new Recorder();
    run(segmenter(recorder), reports);
    assertEquals(1, recorder.of(Type.STOP, 7).size());
    final long baseline = GapPolicy.defaults().getSpeedlessBaselineMillis(ground);
    assertEquals(160_000L, baseline);
    final long stoppedAt = recorder.of(Type.STOP, 7).get(0).getStartTime();
    assertTrue(stoppedAt >= (T0 + 600_000L));
    assertTrue(stoppedAt <= (T0 + 600_000L + (2 * baseline) + 5_000L));
  }

  static List<MotionReport<String>> speedless(
      final List<MotionReport<String>> reports,
      final MotionDomain domain,
      final MotionSource source) {
    return reports.stream().map(
        r -> new MotionReport<>(
            r.getEntityKey(),
            r.isStableIdentity(),
            domain,
            source,
            r.getTime(),
            r.getLatitude(),
            r.getLongitude())).collect(Collectors.toList());
  }

  @Test
  public void timeoutsFollowReportTimesNotArrivalOrder() {
    final MotionReport<String> a = report("a", true, T0 + 300_000L, 30, -40, 5, 0);
    final MotionReport<String> b = report("b", true, T0, 31, -40, 5, 0);
    for (final List<MotionReport<String>> order : List.of(List.of(a, b), List.of(b, a))) {
      final Recorder recorder = new Recorder();
      final H3MotionSegmenter<String> segmenter = segmenter(recorder);
      order.forEach(segmenter::accept);
      // b has been silent 12.5 minutes, a 7.5
      segmenter.advanceTo(T0 + 750_000L);
      assertEquals(List.of("b"), gapStarts(recorder));
      segmenter.advanceTo(T0 + 1_100_000L);
      assertEquals(List.of("b", "a"), gapStarts(recorder));
    }
    final MotionReport<String> c = report("c", true, T0 + 780_000L, 32, -40, 5, 0);
    for (final List<MotionReport<String>> order : List.of(List.of(a, b, c), List.of(b, a, c))) {
      final Recorder recorder = new Recorder();
      run(segmenter(recorder), order);
      assertEquals(List.of("b"), gapStarts(recorder));
      assertEquals(
          List.of("b"),
          recorder.of(Type.GAP_END, 6).stream().map(MotionSlice::getEntityKey).collect(
              Collectors.toList()));
    }
  }

  private static List<String> gapStarts(final Recorder recorder) {
    return recorder.of(Type.GAP_START, 6).stream().map(MotionSlice::getEntityKey).collect(
        Collectors.toList());
  }

  @Test
  public void reportsWithinTheirLatenessAreNotGapped() {
    final MotionSource late = MARITIME.withAllowedLatenessMillis(600_000L);
    final List<MotionReport<String>> reports = new ArrayList<>();
    for (final String id : new String[] {"a", "b"}) {
      reports.addAll(
          // a reports every 10 s, b every 3 minutes, both for 40 minutes
          straightTrack(
              id,
              true,
              id.equals("a") ? 30 : 30.1,
              -40,
              80,
              8,
              T0,
              id.equals("a") ? 10_000L : 180_000L,
              id.equals("a") ? 241 : 14).stream().map(
                  r -> new MotionReport<>(
                      r.getEntityKey(),
                      true,
                      MotionDomain.SURFACE,
                      late,
                      r.getTime(),
                      r.getLatitude(),
                      r.getLongitude(),
                      r.getSpeed(),
                      r.getCourse(),
                      Double.NaN)).collect(Collectors.toList()));
    }
    // b's region is delivered 10 minutes behind a's, so b's last report is up to 13 minutes old
    final List<String> onTime = streamB(reports, 0);
    final List<String> lagged = streamB(reports, 600_000L);
    assertEquals(onTime, lagged);
    assertTrue(onTime.stream().noneMatch(s -> s.startsWith("GAP")));
    final List<MotionReport<String>> strict =
        reports.stream().map(
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
    assertTrue(streamB(strict, 600_000L).stream().anyMatch(s -> s.startsWith("GAP_START")));
  }

  /** Streams the reports in arrival order, b's delayed by lag, advancing every minute. */
  private static List<String> streamB(final List<MotionReport<String>> reports, final long lag) {
    final List<MotionReport<String>> arrivals = new ArrayList<>(reports);
    arrivals.sort(
        Comparator.comparingLong(r -> r.getTime() + (r.getEntityKey().equals("b") ? lag : 0)));
    final Recorder recorder = new Recorder();
    final H3MotionSegmenter<String> segmenter = segmenter(recorder);
    long nextAdvance = Long.MIN_VALUE;
    for (final MotionReport<String> report : arrivals) {
      final long arrival = report.getTime() + (report.getEntityKey().equals("b") ? lag : 0);
      segmenter.accept(report);
      if (arrival >= nextAdvance) {
        segmenter.advanceTo(arrival);
        nextAdvance = arrival + 60_000L;
      }
    }
    final long window = GapPolicy.defaults().getTimeoutMillis(reports.get(0).getSource());
    assertEquals(segmenter.getProcessedTime() - window, segmenter.getCompleteThrough());
    segmenter.close();
    return recorder.where(s -> s.getEntityKey().equals("b")).stream().map(
        s -> s.getType() + " " + s).collect(Collectors.toList());
  }

  @Test
  public void fastReportsMustSpanThirtySecondsToEndAStop() {
    final double[] speeds = {0.1, 0.1, 0.1, 1.5, 1.5, 0.1, 0.1, 0.1, 1.5, 1.5, 1.5, 1.5, 1.5};
    final List<MotionReport<String>> reports = new ArrayList<>();
    for (int i = 0; i < speeds.length; i++) {
      reports.add(report("v", true, T0 + (i * 10_000L), 30, -40, speeds[i], 0));
    }
    final Recorder recorder = new Recorder();
    run(segmenter(recorder), reports);
    final List<MotionSlice<String>> starts = recorder.of(Type.START, 6);
    assertEquals(1, starts.size());
    // the run of fast reports begins at index 8 and spans 30 s at index 11
    assertEquals(T0 + 100_000L, starts.get(0).getStartTime());
  }

  @Test
  public void aSourceChangeRestartsTheEntity() {
    final MotionSource renamed = new MotionSource("renamed", 200, 10, 600_000L);
    final List<MotionReport<String>> track =
        straightTrack("v", true, 30, -40, 90, 8, T0, 10_000L, 20);
    final List<MotionReport<String>> alternating = new ArrayList<>();
    for (int i = 0; i < track.size(); i++) {
      final MotionReport<String> r = track.get(i);
      alternating.add(
          new MotionReport<>(
              "v",
              true,
              MotionDomain.SURFACE,
              (i % 2) == 0 ? MARITIME : renamed,
              r.getTime(),
              r.getLatitude(),
              r.getLongitude(),
              r.getSpeed(),
              r.getCourse(),
              Double.NaN));
    }
    final Recorder recorder = new Recorder();
    final H3MotionSegmenter<String> segmenter = segmenter(recorder);
    run(segmenter, alternating);
    assertEquals(track.size() - 1, segmenter.getRestarts());
    assertEquals(track.size(), recorder.of(Type.VISIT_END, 6).size());
    assertEquals(0, recorder.seconds(6, false), 0);
    final H3MotionSegmenter<String> steady = segmenter(new Recorder());
    run(steady, track);
    assertEquals(0, steady.getRestarts());
  }

  @Test
  public void aShallowExcursionLongerThanTheWindowIsItsOwnVisit() {
    final long cell = H3.latLngToCell(30, -40, 6);
    final List<LatLng> boundary = H3.cellToBoundary(cell);
    final LatLng center = H3.cellToLatLng(cell);
    final double edgeLat = (boundary.get(0).lat + boundary.get(1).lat) / 2;
    final double edgeLon = (boundary.get(0).lng + boundary.get(1).lng) / 2;
    final double outward = Spherical.bearing(center.lat, center.lng, edgeLat, edgeLon);
    final double along =
        Spherical.bearing(
            boundary.get(0).lat,
            boundary.get(0).lng,
            boundary.get(1).lat,
            boundary.get(1).lng);
    // 100 m into the neighbor, then diagonally back along the edge at 1 m/s: 15 minutes in it
    final double[][] waypoints =
        {
            offset(edgeLat, edgeLon, along, -900, outward, -300),
            offset(edgeLat, edgeLon, along, -900, outward, 100),
            offset(edgeLat, edgeLon, along, 900, outward, -100),
            offset(edgeLat, edgeLon, along, 900, outward, -1000)};
    final List<MotionReport<String>> dense = new ArrayList<>();
    long time = T0;
    for (int w = 0; w < (waypoints.length - 1); w++) {
      final double[] a = waypoints[w];
      final double[] b = waypoints[w + 1];
      final double meters = Spherical.distance(a[0], a[1], b[0], b[1]);
      final double course = Spherical.bearing(a[0], a[1], b[0], b[1]);
      final int steps = (int) Math.round(meters / 10);
      for (int i = 0; i < steps; i++) {
        final double[] p = destination(a[0], a[1], course, (meters * i) / steps);
        dense.add(report("v", true, time, p[0], p[1], 1, course));
        time += 10_000L;
      }
    }
    final double[] last = waypoints[waypoints.length - 1];
    dense.add(report("v", true, time, last[0], last[1], 1, 0));
    final long neighbor = H3.latLngToCell(waypoints[1][0], waypoints[1][1], 6);
    for (final List<MotionReport<String>> reports : List.of(
        dense,
        MotionTestSupport.admit(
            dense,
            MARITIME.getToleranceMeters(),
            MARITIME.getMaxIntervalMillis()))) {
      final Recorder recorder = new Recorder();
      run(segmenter(recorder), reports);
      assertEquals(
          List.of(cell, neighbor, cell),
          recorder.of(Type.VISIT_END, 6).stream().map(MotionSlice::getCell).collect(
              Collectors.toList()));
    }
  }

  private static double[] offset(
      final double latitude,
      final double longitude,
      final double along,
      final double alongMeters,
      final double outward,
      final double outMeters) {
    final double[] p = destination(latitude, longitude, along, alongMeters);
    return destination(p[0], p[1], outward, outMeters);
  }

  @Test
  public void unstableEntitiesAreKeptOnlyWhileTheyCouldBridge() {
    final MotionSource rotating = new MotionSource("rotating", 300, 300, 60_000L);
    final H3MotionSegmenter<String> segmenter =
        H3MotionSegmenter.<String>builder(new Recorder()).build();
    int most = 0;
    for (long time = T0; time < (T0 + (6 * 3_600_000L)); time += 10_000L) {
      final double[] p = destination(30, -40, 90, (5.0 * (time - T0)) / 1000.0);
      // the identity rotates every hour
      segmenter.accept(
          new MotionReport<>(
              "id" + ((time - T0) / 3_600_000L),
              false,
              MotionDomain.GROUND,
              rotating,
              time,
              p[0],
              p[1]));
      if (((time - T0) % 60_000L) == 0) {
        segmenter.advanceTo(time);
      }
      most = Math.max(most, segmenter.getEntityCount());
    }
    assertEquals(2, most);
    assertEquals(5, segmenter.getEvictions());
  }

  @Test
  public void flowTotalsAreVisitsPlusStopStarts() {
    final MotionVectorAccumulator accumulator =
        new MotionVectorAccumulator(MotionVectorLayout.defaults(MotionDomain.SURFACE));
    final H3MotionSegmenter<String> segmenter =
        H3MotionSegmenter.<String>builder(accumulator).build();
    final Random random = new Random(17);
    for (int v = 0; v < 20; v++) {
      randomWalk("w" + v, random, T0 + (v * 300_000L)).forEach(segmenter::accept);
    }
    segmenter.close();
    long stops = 0;
    for (final Map.Entry<MotionCellKey, AdditiveVectorValue> entry : accumulator.getValues().entrySet()) {
      final AdditiveVectorValue value = entry.getValue();
      long flow = 0;
      for (int i = 0; i < (MotionVectorLayout.SLOTS * MotionVectorLayout.SLOTS); i++) {
        flow += value.getCount(MotionVectorLayout.FLOW + i);
      }
      assertEquals(
          entry.getKey().toString(),
          value.getCount(MotionVectorLayout.VISITS)
              + value.getCount(MotionVectorLayout.STOP_STARTS),
          flow);
      stops += value.getCount(MotionVectorLayout.STOP_STARTS);
    }
    assertTrue(stops > 0);
  }

  @Test
  public void onlyGapEndsAndBridgedOccupancyLandBeforeTheWatermark() {
    // dark for 90 minutes (bridged), then for 3 hours, then the track goes silent for good
    final List<MotionReport<String>> track =
        straightTrack("v", true, 30, -40, 80, 6, T0, 10_000L, 1 + (7 * 360)).stream().filter(r -> {
          final long t = r.getTime() - T0;
          return !(((t > 3_600_000L) && (t < 9_000_000L))
              || ((t > 10_800_000L) && (t < 21_600_000L)));
        }).collect(Collectors.toList());
    // per level, the level's watermark before the call that emitted the slice
    final long[] watermark = new long[16];
    final Map<String, Integer> late = new HashMap<>();
    final Map<String, Integer> lateToPlainWatermark = new HashMap<>();
    final H3MotionSegmenter<String> segmenter =
        H3MotionSegmenter.<String>builder((MotionSlice<String> s) -> {
          final long time =
              (s.getType() == Type.VISIT_END) || (s.getType() == Type.TRANSITION) ? s.getEndTime()
                  : s.getStartTime();
          final String kind = s.getType() + (s.isBridged() ? " bridged" : "");
          if (time < watermark[s.getLevel()]) {
            late.merge(kind, 1, Integer::sum);
          }
          if (time < watermark[0]) {
            lateToPlainWatermark.merge(kind, 1, Integer::sum);
          }
        }).idleTtlMillis(2 * 3_600_000L).build();
    int next = 0;
    for (long time = T0; time <= (T0 + (10 * 3_600_000L)); time += 10_000L) {
      final boolean advance = (time % 60_000L) == 0;
      watermark[6] = segmenter.getCompleteThrough(6);
      watermark[4] = segmenter.getCompleteThrough(4);
      watermark[0] = segmenter.getCompleteThrough();
      while ((next < track.size()) && (track.get(next).getTime() == time)) {
        segmenter.accept(track.get(next++));
      }
      if (advance) {
        watermark[6] = segmenter.getCompleteThrough(6);
        watermark[4] = segmenter.getCompleteThrough(4);
        watermark[0] = segmenter.getCompleteThrough();
        segmenter.advanceTo(time);
      }
    }
    assertEquals(Set.of("GAP_END", "OCCUPANCY bridged"), late.keySet());
    // against the plain watermark, derived-level visits held by an excursion are late too
    assertTrue(lateToPlainWatermark.containsKey("VISIT_END"));
    // both gaps and the eviction at both levels, except the 90 minute gap's end at level 4,
    // whose watermark backs off further than that
    assertEquals(5, (int) late.get("GAP_END"));
  }

  @Test
  public void unboundedIdleTtlAndLatenessDoNotOverflow() {
    // an idle TTL of Long.MAX_VALUE never evicts, so the gap is measured when the vessel returns
    final Recorder kept = new Recorder();
    final H3MotionSegmenter<String> keeping =
        H3MotionSegmenter.<String>builder(kept).idleTtlMillis(Long.MAX_VALUE).build();
    keeping.accept(report("v", true, T0, 30, -40, 5, 0));
    keeping.advanceTo(T0 + WINDOW + 60_000L);
    keeping.advanceTo(T0 + 3_600_000L);
    assertEquals(1, kept.of(Type.GAP_START, 6).size());
    assertEquals(0, keeping.getEvictions());
    keeping.accept(report("v", true, T0 + 7_200_000L, 30.01, -40, 5, 0));
    assertEquals(
        List.of(7200.0),
        kept.of(Type.GAP_END, 6).stream().filter(s -> !s.isCensored()).map(
            MotionSlice::getDurationSeconds).collect(Collectors.toList()));

    // a lateness of Long.MAX_VALUE never times out, and the watermark backs off without bound
    final MotionSource unbounded = MARITIME.withAllowedLatenessMillis(Long.MAX_VALUE);
    assertEquals(Long.MAX_VALUE, GapPolicy.defaults().getTimeoutMillis(unbounded));
    final Recorder waiting = new Recorder();
    final H3MotionSegmenter<String> segmenter = segmenter(waiting);
    segmenter.accept(sourced(report("v", true, T0, 30, -40, 5, 0), unbounded));
    segmenter.advanceTo(T0 + 3_600_000L);
    assertTrue(waiting.of(Type.GAP_START).isEmpty());
    assertEquals(1, segmenter.getEntityCount());
    assertTrue(segmenter.getCompleteThrough() < (T0 - (365 * 24 * 3_600_000L)));
    assertTrue(segmenter.getCompleteThrough(4) <= segmenter.getCompleteThrough());
    segmenter.accept(sourced(report("v", true, T0 + 7_200_000L, 30.01, -40, 5, 0), unbounded));
    assertEquals(
        List.of(7200.0),
        waiting.of(Type.GAP_END, 6).stream().map(MotionSlice::getDurationSeconds).collect(
            Collectors.toList()));
  }

  @Test
  public void closeGapsByTheContinuityWindowWhateverTheLateness() {
    final MotionSource onTime = MARITIME.withRawIntervalMillis(180_000L);
    final MotionSource late = onTime.withAllowedLatenessMillis(600_000L);
    final long window = GapPolicy.defaults().getContinuityWindowMillis(late);
    for (final int silentMinutes : new int[] {10, 16, 20, 23, 25, 30}) {
      final List<String> expected = closeAfterSilence(onTime, silentMinutes);
      assertEquals(expected, closeAfterSilence(late, silentMinutes));
      assertEquals(
          "silent " + silentMinutes + " min",
          (silentMinutes * 60_000L) > window,
          expected.stream().anyMatch(s -> s.startsWith("GAP_START")));
    }
  }

  /** The slices of an entity that falls silent while another reports on until close. */
  private static List<String> closeAfterSilence(
      final MotionSource source,
      final int silentMinutes) {
    final Recorder recorder = new Recorder();
    final H3MotionSegmenter<String> segmenter = segmenter(recorder);
    for (long t = T0 - 1_800_000L; t <= (T0 + (silentMinutes * 60_000L)); t += 60_000L) {
      if (t <= T0) {
        segmenter.accept(sourced(report("silent", true, t, 30, -40, 0.1, 0), source));
      }
      segmenter.accept(sourced(report("other", true, t, 31, -40, 0.1, 0), source));
    }
    segmenter.close();
    return recorder.where(s -> s.getEntityKey().equals("silent")).stream().map(
        s -> s.getType() + " " + s).collect(Collectors.toList());
  }

  @Test
  public void advanceToBeforeTheFirstReportLeavesTheWatermarks() {
    final MotionSource ais =
        MARITIME.withRawIntervalMillis(180_000L).withAllowedLatenessMillis(600_000L);
    final GapPolicy policy = GapPolicy.defaults();
    final H3MotionSegmenter<String> idle = segmenter(new Recorder());
    idle.advanceTo(T0);
    assertEquals(Long.MIN_VALUE, idle.getCompleteThrough());
    assertEquals(Long.MIN_VALUE, idle.getCompleteThrough(6));
    assertEquals(Long.MIN_VALUE, idle.getCompleteThrough(4));
    // the first report, 9 minutes late, sets them with its back-off
    idle.accept(sourced(report("v", true, T0 - 540_000L, 30, -40, 5, 0), ais));
    assertEquals(T0 - policy.getTimeoutMillis(ais), idle.getCompleteThrough());
    assertEquals(
        idle.getCompleteThrough() - (2 * policy.getContinuityWindowMillis(ais)),
        idle.getCompleteThrough(6));
    assertTrue(idle.getCompleteThrough(4) < idle.getCompleteThrough(6));

    // a writer's timer starts before the feed, whose reports trail it by 9 minutes; the vessel
    // sails north at 5 m/s
    final WatermarkCheck check = new WatermarkCheck(null);
    check.advanceTo(T0);
    for (long t = T0 - 540_000L; t <= (T0 + 3_600_000L); t += 10_000L) {
      check.feed(sourced(report("v", true, t, 30 + ((t - T0) / 2.22e7), -40, 5, 0), ais));
      if (((t + 540_000L) % 60_000L) == 0) {
        check.advanceTo(t + 540_000L);
      }
    }
    assertTrue(check.late.toString(), check.late.isEmpty());
    assertTrue(check.total > 800);
  }

  @Test
  public void aDeclaredSourceBacksTheWatermarksOffFromTheStart() {
    final MotionSource fast = new MotionSource("fast", 200, 10, 60_000L);
    final MotionSource ais =
        MARITIME.withRawIntervalMillis(180_000L).withAllowedLatenessMillis(600_000L);
    for (final boolean declared : new boolean[] {false, true}) {
      final WatermarkCheck check = new WatermarkCheck("late");
      if (declared) {
        check.declare(MotionDomain.SURFACE, ais);
      }
      // an hour of a fast source moves the watermarks to within its short back-off
      long now = T0;
      for (; now <= (T0 + 3_600_000L); now += 10_000L) {
        check.feed(new MotionReport<>("fast", true, MotionDomain.SURFACE, fast, now, 20, 20));
        if ((now % 60_000L) == 0) {
          check.advanceTo(now);
        }
      }
      // then an AIS vessel sails for 30 minutes, 9 minutes late, which its lateness allows
      final long first = now - 540_000L;
      for (long t = first; t <= (first + 1_800_000L); t += 10_000L, now += 10_000L) {
        check.feed(new MotionReport<>("fast", true, MotionDomain.SURFACE, fast, now, 20, 20));
        check.feed(sourced(report("late", true, t, 30 + ((t - T0) / 2.22e7), -40, 5, 0), ais));
        if ((now % 60_000L) == 0) {
          check.advanceTo(now);
        }
      }
      // undeclared, the first slices of the newly seen source land behind the level watermark
      assertEquals(check.late.toString(), declared, check.late.isEmpty());
    }
  }

  /**
   * Counts slices emitted before the per-level watermark read just before the call that emitted
   * them, except gap ends and bridged occupancy.
   */
  private static final class WatermarkCheck implements Consumer<MotionSlice<String>> {
    private final String entityKey;
    private final Map<String, Integer> late = new HashMap<>();
    private final long[] watermark = new long[16];
    private final H3MotionSegmenter.Builder<String> builder = H3MotionSegmenter.builder(this);
    private H3MotionSegmenter<String> segmenter;
    private int total;

    WatermarkCheck(final String entityKey) {
      this.entityKey = entityKey;
    }

    void declare(final MotionDomain domain, final MotionSource source) {
      builder.expectSource(domain, source);
    }

    private H3MotionSegmenter<String> segmenter() {
      if (segmenter == null) {
        segmenter = builder.build();
      }
      for (int level = 0; level < watermark.length; level++) {
        watermark[level] = segmenter.getCompleteThrough(level);
      }
      return segmenter;
    }

    void feed(final MotionReport<String> report) {
      segmenter().accept(report);
    }

    void advanceTo(final long time) {
      segmenter().advanceTo(time);
    }

    @Override
    public void accept(final MotionSlice<String> slice) {
      if (((entityKey != null) && !entityKey.equals(slice.getEntityKey()))
          || (slice.getType() == Type.GAP_END)
          || slice.isBridged()) {
        return;
      }
      total++;
      final long time =
          (slice.getType() == Type.VISIT_END) || (slice.getType() == Type.TRANSITION)
              ? slice.getEndTime()
              : slice.getStartTime();
      if (time < watermark[slice.getLevel()]) {
        late.merge(slice.getType() + " L" + slice.getLevel(), 1, Integer::sum);
      }
    }
  }

  @Test
  public void aGapAfterACrossingStartsInTheCellCrossedInto() {
    final long p = H3.latLngToCell(30.1, -40.1, 4);
    final long q = H3.gridRing(p, 1).get(0);
    final LatLng center = H3.cellToLatLng(p);
    final List<LatLng> edge = H3.directedEdgeToBoundary(H3.cellsToDirectedEdge(p, q));
    final double midLatitude = (edge.get(0).lat + edge.get(1).lat) / 2;
    final double midLongitude = (edge.get(0).lng + edge.get(1).lng) / 2;
    final double bearing = Spherical.bearing(center.lat, center.lng, midLatitude, midLongitude);
    final double toEdge = Spherical.distance(center.lat, center.lng, midLatitude, midLongitude);
    // going dark 3 km into Q, well short of the level 4 allowance
    final List<MotionReport<String>> reports = new ArrayList<>();
    long time = T0;
    for (double meters = 0; meters <= (toEdge + 3000); meters += 480, time += 60_000L) {
      final double[] position = destination(center.lat, center.lng, bearing, meters);
      reports.add(report("g", true, time, position[0], position[1], 8, bearing));
    }
    final Recorder recorder = new Recorder();
    final H3MotionSegmenter<String> segmenter = segmenter(recorder);
    reports.forEach(segmenter::accept);
    segmenter.advanceTo(time + 3_600_000L);
    final List<MotionSlice<String>> visits = recorder.of(Type.VISIT_END, 4);
    assertEquals(List.of(p, q), MotionLevelsTest.cells(visits));
    assertFalse(visits.get(0).isCensored());
    assertEquals(digit(p, q) - 1, visits.get(0).getExitSlot());
    assertTrue(visits.get(1).isCensored());
    assertEquals(MotionSlice.ENDPOINT, visits.get(1).getExitSlot());
    assertEquals(q, recorder.of(Type.GAP_START, 4).get(0).getCell());
  }

  @Test
  public void gapStartsLieInTheCellOfTheVisitTheyCensor() {
    for (final long seed : new long[] {7, 8}) {
      final Random random = new Random(seed);
      final List<MotionReport<String>> reports = new ArrayList<>();
      for (int w = 0; w < 1000; w++) {
        reports.addAll(gappyWalk("w" + w, random, T0 + (random.nextInt(3_600) * 1000L)));
      }
      reports.sort(Comparator.comparingLong(MotionReport::getTime));
      final MotionVectorAccumulator accumulator =
          new MotionVectorAccumulator(MotionVectorLayout.defaults(MotionDomain.SURFACE));
      final Map<String, MotionSlice<String>> censoredEnds = new HashMap<>();
      final int[] gapStarts = new int[1];
      final H3MotionSegmenter<String> segmenter =
          H3MotionSegmenter.<String>builder((MotionSlice<String> s) -> {
            accumulator.accept(s);
            final String key = s.getEntityKey() + " " + s.getLevel();
            if ((s.getType() == Type.VISIT_END) && s.isCensored()) {
              censoredEnds.put(key, s);
            } else if (s.getType() == Type.GAP_START) {
              gapStarts[0]++;
              final MotionSlice<String> visit = censoredEnds.remove(key);
              assertEquals(s.toString(), s.getStartTime(), visit.getEndTime());
              assertEquals(s.toString(), visit.getCell(), s.getCell());
            }
          }).build();
      long nextAdvance = T0;
      for (final MotionReport<String> report : reports) {
        while (report.getTime() >= nextAdvance) {
          segmenter.advanceTo(nextAdvance);
          nextAdvance += 60_000L;
        }
        segmenter.accept(report);
      }
      segmenter.close();
      assertTrue(gapStarts[0] > 4000);
      assertTrue(segmenter.getImplausibleJumps() > 400);
      for (final Map.Entry<MotionCellKey, AdditiveVectorValue> entry : accumulator.getValues().entrySet()) {
        final AdditiveVectorValue value = entry.getValue();
        long flow = 0;
        for (int i = 0; i < (MotionVectorLayout.SLOTS * MotionVectorLayout.SLOTS); i++) {
          flow += value.getCount(MotionVectorLayout.FLOW + i);
        }
        assertEquals(
            entry.getKey().toString(),
            value.getCount(MotionVectorLayout.VISITS)
                + value.getCount(MotionVectorLayout.STOP_STARTS),
            flow);
      }
    }
  }

  /**
   * A walk that may start stopped, meanders, stops twice, goes dark for 15 to 65 minutes, often
   * with an excursion pending, and sometimes jumps 30 km.
   */
  private static List<MotionReport<String>> gappyWalk(
      final String id,
      final Random random,
      final long start) {
    final List<MotionReport<String>> reports = new ArrayList<>();
    double latitude = 30 + random.nextDouble();
    double longitude = -40 + random.nextDouble();
    double course = random.nextDouble() * 360;
    double speed = 2 + (random.nextDouble() * 10);
    long time = start;
    final long stop1 = start + (random.nextBoolean() ? 0 : 1_500_000L + random.nextInt(1_000_000));
    final long stop2 = start + 6_000_000L + random.nextInt(1_000_000);
    final long dark = start + 3_500_000L + random.nextInt(1_000_000);
    final long darkLength = 900_000L + random.nextInt(3_000_000);
    final long jump = random.nextInt(4) == 0 ? start + 8_500_000L : Long.MAX_VALUE;
    while (time < (start + 10_000_000L)) {
      final long step = 10_000L + (random.nextInt(5) * 10_000L);
      time += step;
      final boolean stopped =
          ((time >= stop1) && (time < (stop1 + 900_000L)))
              || ((time >= stop2) && (time < (stop2 + 1_200_000L)));
      course += random.nextGaussian() * 15;
      speed = Math.max(1.2, Math.min(14, speed + (random.nextGaussian() * 0.5)));
      final double[] next =
          destination(latitude, longitude, course, ((stopped ? 0 : speed) * step) / 1000.0);
      latitude = next[0];
      longitude = next[1];
      if ((time >= dark) && (time < (dark + darkLength))) {
        continue;
      }
      double[] position =
          destination(
              latitude,
              longitude,
              random.nextDouble() * 360,
              Math.abs(random.nextGaussian()) * 8);
      if ((time >= jump) && (time < (jump + step))) {
        position = destination(latitude, longitude, random.nextDouble() * 360, 30_000);
      }
      reports.add(report(id, true, time, position[0], position[1], stopped ? 0.05 : speed, course));
    }
    return reports;
  }

  private static MotionReport<String> sourced(
      final MotionReport<String> report,
      final MotionSource source) {
    return new MotionReport<>(
        report.getEntityKey(),
        report.isStableIdentity(),
        report.getDomain(),
        source,
        report.getTime(),
        report.getLatitude(),
        report.getLongitude(),
        report.getSpeed(),
        report.getCourse(),
        report.getAltitude());
  }

  /**
   * A meandering surface track with speed changes, two stops and one 40 minute dark period, one
   * report every 10 to 30 seconds.
   */
  static List<MotionReport<String>> randomWalk(
      final String id,
      final Random random,
      final long start) {
    final List<MotionReport<String>> reports = new ArrayList<>();
    double latitude = 30 + random.nextDouble();
    double longitude = -40 + random.nextDouble();
    double course = random.nextDouble() * 360;
    double speed = 8;
    long time = start;
    final long dark = start + 4_000_000L;
    final long[] stops = {start + 2_000_000L, start + 7_000_000L};
    while (time < (start + 10_000_000L)) {
      final long step = 10_000L + (random.nextInt(3) * 10_000L);
      time += step;
      boolean stopped = false;
      for (final long stop : stops) {
        stopped |= (time >= stop) && (time < (stop + 1_200_000L));
      }
      final double moving = stopped ? 0 : speed;
      course += random.nextGaussian() * 6;
      speed = Math.max(3, Math.min(14, speed + (random.nextGaussian() * 0.3)));
      final double[] next = destination(latitude, longitude, course, (moving * step) / 1000.0);
      latitude = next[0];
      longitude = next[1];
      if ((time >= dark) && (time < (dark + 2_400_000L))) {
        continue;
      }
      final double[] noisy =
          destination(
              latitude,
              longitude,
              random.nextDouble() * 360,
              Math.abs(random.nextGaussian()) * 5);
      reports.add(report(id, true, time, noisy[0], noisy[1], stopped ? 0.05 : speed, course));
    }
    return reports;
  }

  private static List<String> describe(final List<MotionSlice<String>> slices) {
    return slices.stream().map(MotionSlice::toString).collect(Collectors.toList());
  }

  static void assertSameValues(
      final Map<MotionCellKey, AdditiveVectorValue> expected,
      final Map<MotionCellKey, AdditiveVectorValue> actual) {
    assertEquals(expected.keySet(), actual.keySet());
    for (final Map.Entry<MotionCellKey, AdditiveVectorValue> entry : expected.entrySet()) {
      final AdditiveVectorValue want = entry.getValue();
      final AdditiveVectorValue got = actual.get(entry.getKey());
      for (int i = 0; i < MotionVectorLayout.COUNT_LENGTH; i++) {
        assertEquals(entry.getKey() + " count " + i, want.getCount(i), got.getCount(i));
      }
      for (int i = 0; i < MotionVectorLayout.SUM_LENGTH; i++) {
        assertEquals(
            entry.getKey() + " sum " + i,
            want.getSum(i),
            got.getSum(i),
            1e-9 * Math.max(1, Math.abs(want.getSum(i))));
      }
    }
  }
}
