/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.motion;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.locationtech.geowave.core.geotime.motion.MotionTestSupport.MARITIME;
import static org.locationtech.geowave.core.geotime.motion.MotionTestSupport.T0;
import java.util.Map;
import java.util.Random;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.motion.MotionSlice.Type;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;
import org.locationtech.geowave.core.store.statistics.adapter.AdditiveVectorStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.AdditiveVectorStatistic.AdditiveVectorValue;
import org.locationtech.geowave.core.store.statistics.binning.CompositeBinningStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MotionVectorLayoutTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(MotionVectorLayoutTest.class);

  @Test
  public void layoutIsDense() {
    assertEquals(84, MotionVectorLayout.COUNT_LENGTH);
    assertEquals(20, MotionVectorLayout.SUM_LENGTH);
    assertEquals(MotionVectorLayout.COUNT_LENGTH - 1, MotionVectorLayout.flowIndex(6, 6));
    final AdditiveVectorValue value =
        MotionVectorLayout.defaults(MotionDomain.SURFACE).createValue(null);
    assertEquals(MotionVectorLayout.SCHEMA_ID, value.getSchemaId());
    assertEquals(MotionVectorLayout.SCHEMA_VERSION, value.getSchemaVersion());
    assertEquals(MotionVectorLayout.COUNT_LENGTH, value.getCountLength());
    assertEquals(MotionVectorLayout.SUM_LENGTH, value.getSumLength());
    assertEquals(
        MotionVectorLayout.COUNT_LENGTH,
        MotionVectorLayout.createStatistic("motion.maritime").getCountLength());
  }

  @Test
  public void valuesTakeTheirSchemaFromTheStatistic() {
    final MotionVectorLayout layout = MotionVectorLayout.defaults(MotionDomain.SURFACE);
    final AdditiveVectorStatistic statistic = MotionVectorLayout.createStatistic("motion.maritime");
    final AdditiveVectorValue value = layout.createValue(statistic);
    assertEquals(statistic, value.getStatistic());
    assertEquals(MotionVectorLayout.SCHEMA_ID, value.getSchemaId());
    // a later version that appends positions still holds this layout
    final AdditiveVectorStatistic appended =
        new AdditiveVectorStatistic(
            "motion.maritime",
            MotionVectorLayout.SCHEMA_ID,
            MotionVectorLayout.SCHEMA_VERSION + 1,
            MotionVectorLayout.COUNT_LENGTH + 2,
            MotionVectorLayout.SUM_LENGTH);
    assertEquals(
        MotionVectorLayout.SCHEMA_VERSION + 1,
        layout.createValue(appended).getSchemaVersion());
    assertEquals(
        MotionVectorLayout.COUNT_LENGTH + 2,
        layout.createValue(appended).getCountLength());
    for (final AdditiveVectorStatistic other : new AdditiveVectorStatistic[] {
        new AdditiveVectorStatistic(
            "motion.maritime",
            2,
            MotionVectorLayout.SCHEMA_VERSION,
            MotionVectorLayout.COUNT_LENGTH,
            MotionVectorLayout.SUM_LENGTH),
        new AdditiveVectorStatistic(
            "motion.maritime",
            MotionVectorLayout.SCHEMA_ID,
            MotionVectorLayout.SCHEMA_VERSION,
            MotionVectorLayout.COUNT_LENGTH,
            MotionVectorLayout.SUM_LENGTH - 1)}) {
      try {
        layout.createValue(other);
        fail("a statistic of another layout must be refused");
      } catch (final IllegalArgumentException expected) {
        // the accumulator refuses it up front too
      }
      try {
        new MotionVectorAccumulator(layout, other);
        fail("a statistic of another layout must be refused");
      } catch (final IllegalArgumentException expected) {
      }
    }
  }

  @Test
  public void binsAndSectors() {
    final MotionVectorLayout layout = MotionVectorLayout.defaults(MotionDomain.SURFACE);
    assertEquals(0, layout.speedBin(0.2));
    assertEquals(1, layout.speedBin(0.5));
    assertEquals(7, layout.speedBin(40));
    assertEquals(0, layout.gapDurationBin(300));
    assertEquals(3, layout.gapDurationBin(5000));
    assertEquals(5, layout.gapDurationBin(86400));
    assertEquals(0, layout.altitudeBin(500));
    assertEquals(3, layout.altitudeBin(11000));
    assertEquals(0, MotionVectorLayout.headingSector(0));
    assertEquals(0, MotionVectorLayout.headingSector(22.4));
    assertEquals(1, MotionVectorLayout.headingSector(22.5));
    assertEquals(2, MotionVectorLayout.headingSector(90));
    assertEquals(4, MotionVectorLayout.headingSector(180));
    assertEquals(7, MotionVectorLayout.headingSector(337.4));
    assertEquals(0, MotionVectorLayout.headingSector(337.5));
    assertEquals(0, MotionVectorLayout.headingSector(359.9));
  }

  @Test
  public void binIsTheCompositeOfBucketAndCell() {
    final long cell = H3Holder.h3().latLngToCell(30, -40, 6);
    final ByteArray expected =
        CompositeBinningStrategy.getBin(
            new ByteArray(Lexicoders.LONG.toByteArray(T0)),
            new ByteArray(Lexicoders.LONG.toByteArray(cell)));
    assertEquals(expected, new MotionCellKey(T0, 6, cell).getBin());
    assertEquals(expected, MotionCellKey.getBin(T0, cell));
  }

  @Test
  public void resolutionPlanCapsBySourceAccuracy() {
    final ResolutionPlan plan = ResolutionPlan.defaults();
    assertArrayEquals(new int[] {6, 4}, plan.getLevels(MotionDomain.SURFACE));
    assertArrayEquals(new int[] {5, 3}, plan.getLevels(MotionDomain.AIR));
    assertArrayEquals(new int[] {7, 5}, plan.getLevels(MotionDomain.GROUND));
    assertEquals(7, plan.getMaxLevel(MARITIME));
    assertArrayEquals(new int[] {6, 4}, plan.getContributingLevels(MotionDomain.SURFACE, MARITIME));
    final MotionSource coarse = new MotionSource("coarse", 300, 1000, 600_000L);
    assertEquals(5, plan.getMaxLevel(coarse));
    assertArrayEquals(new int[] {4}, plan.getContributingLevels(MotionDomain.SURFACE, coarse));
    assertArrayEquals(
        new int[] {5},
        plan.getContributingLevels(MotionDomain.GROUND, new MotionSource("s", 300, 300, 60_000L)));
    assertArrayEquals(
        new int[] {4},
        plan.getContributingLevels(MotionDomain.SURFACE, MARITIME.withMaxLevel(5)));
    assertArrayEquals(
        new int[0],
        plan.getContributingLevels(MotionDomain.SURFACE, MARITIME.withMaxLevel(3)));
    assertArrayEquals(
        new int[] {8, 7, 2},
        ResolutionPlan.builder().levels(MotionDomain.AIR, 2, 8, 7).build().getLevels(
            MotionDomain.AIR));
  }

  @Test
  public void gapPolicyWindows() {
    final GapPolicy policy = GapPolicy.defaults();
    assertEquals(720_000L, policy.getContinuityWindowMillis(MARITIME));
    assertEquals(
        180_000L,
        policy.getContinuityWindowMillis(new MotionSource("l16", 500, 50, 120_000L)));
    assertEquals(
        120_000L,
        policy.getContinuityWindowMillis(new MotionSource("air", 1000, 30, 60_000L)));
    // a declared raw interval covers admission's extra raw interval and one lost raw report
    final MotionSource ais =
        MARITIME.withRawIntervalMillis(180_000L).withAllowedLatenessMillis(600_000L);
    assertEquals(1_020_000L, policy.getContinuityWindowMillis(ais));
    assertEquals(1_620_000L, policy.getTimeoutMillis(ais));
    assertEquals(
        Long.MAX_VALUE,
        policy.getContinuityWindowMillis(MARITIME.withRawIntervalMillis(Long.MAX_VALUE / 2)));
    assertEquals(
        Long.MAX_VALUE,
        policy.getTimeoutMillis(MARITIME.withAllowedLatenessMillis(Long.MAX_VALUE - 1)));
    assertEquals(420, policy.getStopRadiusMeters(MotionDomain.SURFACE, MARITIME), 0);
    assertTrue(policy.isPlausible(MotionDomain.SURFACE, MARITIME, 20_000, 600_000L));
    assertTrue(!policy.isPlausible(MotionDomain.SURFACE, MARITIME, 30_000, 600_000L));
    assertTrue(policy.isPlausible(MotionDomain.AIR, MARITIME, 200_000, 600_000L));
    assertTrue(!policy.isPlausible(MotionDomain.SURFACE, MARITIME, 60_000, 7_200_000L));
  }

  @Test
  public void accumulatorFillsTheLayout() {
    final MotionVectorLayout layout = MotionVectorLayout.defaults(MotionDomain.AIR);
    final MotionVectorAccumulator accumulator = new MotionVectorAccumulator(layout);
    final long cell = H3Holder.h3().latLngToCell(30, -40, 5);
    accumulator.accept(
        new MotionSlice<>(
            Type.OCCUPANCY,
            "a",
            MotionDomain.AIR,
            true,
            5,
            cell,
            T0,
            T0,
            T0 + 1000).withOccupancy(60, 0, 12000, false));
    accumulator.accept(
        new MotionSlice<>(
            Type.OCCUPANCY,
            "u",
            MotionDomain.AIR,
            false,
            5,
            cell,
            T0,
            T0,
            T0 + 1000).withOccupancy(30, 10, 6000, false));
    accumulator.accept(
        new MotionSlice<>(
            Type.OCCUPANCY,
            "a",
            MotionDomain.AIR,
            true,
            5,
            cell,
            T0,
            T0,
            T0 + 1000).withOccupancy(60, 0, 12000, true));
    accumulator.accept(
        new MotionSlice<>(
            Type.VISIT_END,
            "a",
            MotionDomain.AIR,
            true,
            5,
            cell,
            T0,
            T0,
            T0 + 60_000).withSlots(2, 5).withVisit(60, 0, 12000, 200, 90, 11000, 60, false));
    accumulator.accept(
        new MotionSlice<>(
            Type.TRANSITION,
            "a",
            MotionDomain.AIR,
            true,
            5,
            cell,
            T0,
            T0,
            T0).withSlots(2, 5));
    accumulator.accept(
        new MotionSlice<>(
            Type.GAP_END,
            "a",
            MotionDomain.AIR,
            true,
            5,
            cell,
            T0,
            T0,
            T0 + 4_000_000L).withGap(0, true));
    final Map<MotionCellKey, AdditiveVectorValue> values = accumulator.drain();
    assertTrue(accumulator.getValues().isEmpty());
    final AdditiveVectorValue value = values.get(new MotionCellKey(T0, 5, cell));
    assertEquals(90, value.getSum(MotionVectorLayout.MOVING_SECONDS), 0);
    assertEquals(10, value.getSum(MotionVectorLayout.STOPPED_SECONDS), 0);
    // the unstable entity's 40 s count as occupancy, not as dwell
    assertEquals(60, value.getSum(MotionVectorLayout.STABLE_SECONDS), 0);
    assertEquals(18000, value.getSum(MotionVectorLayout.DISTANCE_METERS), 0);
    assertEquals(1, value.getCount(MotionVectorLayout.VISITS));
    assertEquals(1, value.getCount(MotionVectorLayout.SPEED_HISTOGRAM + 5));
    assertEquals(1, value.getCount(MotionVectorLayout.HEADING_ROSE + 2));
    assertEquals(1, value.getCount(MotionVectorLayout.ALTITUDE_HISTOGRAM + 3));
    assertEquals(1, value.getCount(MotionVectorLayout.EXIT_TRAVERSALS + 5));
    assertEquals(60, value.getSum(MotionVectorLayout.EXIT_TRAVERSAL_SECONDS + 5), 0);
    assertEquals(3600, value.getSum(MotionVectorLayout.EXIT_TRAVERSAL_SECONDS_SQUARED + 5), 0);
    assertEquals(0, value.getSum(MotionVectorLayout.HEADING_COS), 1e-12);
    assertEquals(1, value.getSum(MotionVectorLayout.HEADING_SIN), 1e-12);
    assertEquals(-1, value.getSum(MotionVectorLayout.HEADING_COS2), 1e-12);
    assertEquals(1, value.getCount(MotionVectorLayout.flowIndex(2, 5)));
    assertEquals(1, value.getCount(MotionVectorLayout.GAP_DURATION_HISTOGRAM + 3));
  }

  @Test
  public void realValuesStoreCompactly() {
    final MotionVectorAccumulator accumulator =
        new MotionVectorAccumulator(MotionVectorLayout.defaults(MotionDomain.SURFACE));
    final H3MotionSegmenter<String> segmenter =
        H3MotionSegmenter.<String>builder(accumulator).build();
    final Random random = new Random(9);
    for (int v = 0; v < 20; v++) {
      H3MotionSegmenterTest.randomWalk("w" + v, random, T0).forEach(segmenter::accept);
    }
    segmenter.close();
    long bytes = 0;
    long largest = 0;
    for (final AdditiveVectorValue value : accumulator.getValues().values()) {
      final byte[] binary = value.toBinary();
      final AdditiveVectorValue copy = new AdditiveVectorValue();
      copy.fromBinary(binary);
      assertArrayEquals(value.getCounts(), copy.getCounts());
      bytes += binary.length;
      largest = Math.max(largest, binary.length);
    }
    final double mean = (double) bytes / accumulator.getValues().size();
    LOGGER.info(
        "{} values, mean {} bytes, largest {} bytes",
        accumulator.getValues().size(),
        mean,
        largest);
    assertTrue(mean < 200);
  }
}
