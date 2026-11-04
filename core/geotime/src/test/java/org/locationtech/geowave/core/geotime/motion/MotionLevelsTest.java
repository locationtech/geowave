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
import static org.junit.Assert.assertTrue;
import static org.locationtech.geowave.core.geotime.motion.MotionTestSupport.T0;
import static org.locationtech.geowave.core.geotime.motion.MotionTestSupport.straightTrack;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.motion.MotionSlice.Type;
import org.locationtech.geowave.core.geotime.motion.MotionTestSupport.Recorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Visits at derived levels follow the parent partition but do not turn jagged edges into U-turns.
 */
public class MotionLevelsTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(MotionLevelsTest.class);

  @Test
  public void straightTracksMakeNoCoarseUTurns() {
    for (final int[] plan : new int[][] {{6, 4}, {5, 3}, {7, 5}}) {
      final ResolutionPlan levels =
          ResolutionPlan.builder().levels(MotionDomain.SURFACE, plan).build();
      final Random random = new Random(42);
      final long[] fine = new long[3];
      final long[] coarse = new long[3];
      double traversalSeconds = 0;
      long traversals = 0;
      double spanSeconds = 0;
      long distinct = 0;
      for (int t = 0; t < 400; t++) {
        final double latitude = -60 + (120 * random.nextDouble());
        final double longitude = -180 + (360 * random.nextDouble());
        final double bearing = 360 * random.nextDouble();
        final Recorder recorder = new Recorder();
        final H3MotionSegmenter<String> segmenter =
            H3MotionSegmenter.<String>builder(recorder).resolutionPlan(levels).build();
        // 150 km at 10 m/s, a report every 30 s
        straightTrack("t" + t, true, latitude, longitude, bearing, 10, T0, 30_000L, 501).forEach(
            segmenter::accept);
        segmenter.close();
        count(recorder.of(Type.VISIT_END, plan[0]), fine);
        final List<MotionSlice<String>> coarseVisits = recorder.of(Type.VISIT_END, plan[1]);
        count(coarseVisits, coarse);
        final Set<Long> cells = new HashSet<>();
        long first = Long.MAX_VALUE;
        long last = Long.MIN_VALUE;
        for (final MotionSlice<String> visit : coarseVisits) {
          if (!Double.isNaN(visit.getTraversalSeconds())) {
            traversalSeconds += visit.getTraversalSeconds();
            traversals++;
            cells.add(visit.getCell());
            first = Math.min(first, visit.getStartTime());
            last = Math.max(last, visit.getEndTime());
          }
        }
        if (!cells.isEmpty()) {
          spanSeconds += (last - first) / 1000.0;
          distinct += cells.size();
        }
      }
      // mean traversal over the true time per distinct cell crossed
      final double ratio = (traversalSeconds / traversals) / (spanSeconds / distinct);
      LOGGER.info(
          "Levels {}/{}: fine U-turns {} of {} neighbor-to-neighbor visits, coarse {} of {}; "
              + "coarse traversal ratio {}",
          plan[0],
          plan[1],
          fine[1],
          fine[0],
          coarse[1],
          coarse[0],
          ratio);
      assertEquals(0, fine[1]);
      assertEquals(0, coarse[1]);
      assertTrue(coarse[0] >= 100);
      assertEquals(1.0, ratio, 0.01);
    }
  }

  /**
   * Below the derived level's allowance over its excursion window (1.5 m/s at 6/4 and 4.0 m/s at
   * 5/3 for this source) a straight track's coarse excursion times out before it is deep enough, so
   * a later shallow re-entry is a U-turn. This pins how many such tracks make.
   */
  @Test
  public void slowStraightTracksMakeFewCoarseUTurns() {
    // levels, speed and the largest share of coarse neighbor-to-neighbor visits that U-turn
    final double[][] bounds =
        {{6, 4, 0.6, 0.094}, {6, 4, 1, 0.049}, {5, 3, 0.6, 0.221}, {5, 3, 1, 0.171}};
    for (final double[] bound : bounds) {
      final int[] plan = {(int) bound[0], (int) bound[1]};
      final double speed = bound[2];
      final ResolutionPlan levels =
          ResolutionPlan.builder().levels(MotionDomain.SURFACE, plan).build();
      final Random random = new Random(42);
      final long[] fine = new long[3];
      final long[] coarse = new long[3];
      for (int t = 0; t < 100; t++) {
        final double latitude = -60 + (120 * random.nextDouble());
        final double longitude = -180 + (360 * random.nextDouble());
        final double bearing = 360 * random.nextDouble();
        final Recorder recorder = new Recorder();
        final H3MotionSegmenter<String> segmenter =
            H3MotionSegmenter.<String>builder(recorder).resolutionPlan(levels).build();
        // 200 km, a report every 2 minutes
        final int reports = 1 + (int) (200_000 / (speed * 120));
        straightTrack(
            "t" + t,
            true,
            latitude,
            longitude,
            bearing,
            speed,
            T0,
            120_000L,
            reports).forEach(segmenter::accept);
        segmenter.close();
        count(recorder.of(Type.VISIT_END, plan[0]), fine);
        count(recorder.of(Type.VISIT_END, plan[1]), coarse);
      }
      final double share = (double) coarse[1] / coarse[0];
      LOGGER.info(
          "Levels {}/{} at {} m/s: fine U-turns {} of {}, coarse {} of {} ({})",
          plan[0],
          plan[1],
          speed,
          fine[1],
          fine[0],
          coarse[1],
          coarse[0],
          share);
      assertEquals(0, fine[1]);
      assertTrue(coarse[0] >= 100);
      assertTrue(plan[0] + "/" + plan[1] + " at " + speed + " m/s: " + share, share <= bound[3]);
    }
  }

  @Test
  public void classThreeCellsHideNoCrossings() {
    // the review's misses: through a pentagon at levels 5 and 3, and past a reflex vertex at 5
    assertChord(5, -23.597917, 112.701412, -23.698390, 112.982395, "85a60003fffffff");
    assertChord(3, 65.204015, 12.822735, 63.753300, 9.875488, "830800fffffffff");
    assertChord(5, -2.371986, 175.216612, -2.398555, 175.206858, "857e0627fffffff");
    // chords across and beside every pentagon at the Class III levels
    final Random random = new Random(7);
    int chords = 0;
    for (final int level : new int[] {3, 5, 7}) {
      final double half = 1.5 * H3Holder.averageEdgeMeters(level);
      for (final Long pentagon : H3Holder.h3().getPentagons(level)) {
        final com.uber.h3core.util.LatLng center = H3Holder.h3().cellToLatLng(pentagon);
        for (int k = 0; k < 12; k++) {
          final double bearing = (30 * k) + (random.nextDouble() * 30);
          final double[] from =
              MotionTestSupport.destination(center.lat, center.lng, bearing, half);
          final double[] to =
              MotionTestSupport.destination(
                  center.lat,
                  center.lng,
                  bearing + 180 + ((random.nextDouble() - 0.5) * 50),
                  half);
          assertChord(level, from[0], from[1], to[0], to[1], null);
          chords++;
        }
      }
    }
    assertEquals(3 * 12 * 12, chords);
  }

  /**
   * Segments one chord at {@code level} and compares the meters in each cell with sampling every
   * meter or so; {@code expectedCell}, if given, must be among them.
   */
  private static void assertChord(
      final int level,
      final double lat0,
      final double lon0,
      final double lat1,
      final double lon1,
      final String expectedCell) {
    final MotionSource fine = new MotionSource("fine", 50, 5, 3_600_000L);
    final ResolutionPlan plan = ResolutionPlan.builder().levels(MotionDomain.AIR, level).build();
    final double meters = Spherical.distance(lat0, lon0, lat1, lon1);
    final long millis = (long) Math.ceil(meters / 0.2);
    final Recorder recorder = new Recorder();
    final H3MotionSegmenter<String> segmenter =
        H3MotionSegmenter.<String>builder(recorder).resolutionPlan(plan).build();
    segmenter.accept(new MotionReport<>("c", true, MotionDomain.AIR, fine, T0, lat0, lon0));
    segmenter.accept(
        new MotionReport<>("c", true, MotionDomain.AIR, fine, T0 + millis, lat1, lon1));
    segmenter.close();
    final java.util.Map<Long, Double> actual = new java.util.HashMap<>();
    for (final MotionSlice<String> slice : recorder.of(Type.OCCUPANCY, level)) {
      actual.merge(slice.getCell(), slice.getDistanceMeters(), Double::sum);
    }
    final double step = Math.max(1, H3Holder.averageEdgeMeters(level) / 2000);
    final java.util.Map<Long, Double> reference = new java.util.HashMap<>();
    final double[] a = new double[3];
    final double[] b = new double[3];
    final double[] p = new double[3];
    Spherical.toVector(lat0, lon0, a);
    Spherical.toVector(lat1, lon1, b);
    final double theta = Spherical.angle(a, b);
    final int samples = (int) Math.ceil(meters / step);
    for (int j = 0; j < samples; j++) {
      Spherical.interpolate(a, b, theta, (j + 0.5) / samples, p);
      reference.merge(
          H3Holder.h3().latLngToCell(Spherical.latitude(p), Spherical.longitude(p), level),
          meters / samples,
          Double::sum);
    }
    final double allowed =
        2 * Math.max(step, Math.max(1, H3Holder.averageEdgeMeters(level) / 1000));
    final Set<Long> cells = new HashSet<>(reference.keySet());
    cells.addAll(actual.keySet());
    for (final Long cell : cells) {
      final double want = reference.getOrDefault(cell, 0.0);
      final double got = actual.getOrDefault(cell, 0.0);
      assertEquals(
          "level " + level + " cell " + Long.toHexString(cell) + " on " + lat0 + "," + lon0,
          want,
          got,
          allowed);
    }
    if (expectedCell != null) {
      assertTrue(actual.getOrDefault(H3Holder.h3().stringToH3(expectedCell), 0.0) > allowed);
    }
  }

  /** Neighbor-to-neighbor visits in [0] and those that leave the way they came in [1]. */
  private static void count(final List<MotionSlice<String>> visits, final long[] counts) {
    for (final MotionSlice<String> visit : visits) {
      if ((visit.getEntrySlot() < MotionSlice.ENDPOINT)
          && (visit.getExitSlot() < MotionSlice.ENDPOINT)) {
        counts[0]++;
        if (visit.getEntrySlot() == visit.getExitSlot()) {
          counts[1]++;
        }
      }
    }
  }

  static List<Long> cells(final List<MotionSlice<String>> visits) {
    final List<Long> cells = new ArrayList<>();
    visits.forEach(v -> cells.add(v.getCell()));
    return cells;
  }
}
