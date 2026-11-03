/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.motion;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.statistics.adapter.AdditiveVectorStatistic.AdditiveVectorValue;

/**
 * Sums {@link MotionSlice}s into one {@link AdditiveVectorValue} per (bucket, level, cell), laid
 * out by {@link MotionVectorLayout}. Bridged slices and slices that carry nothing for the vector
 * (visit and kinematic starts) are ignored. Because every position is a plain sum, accumulating two
 * parts of a stream separately and adding the values gives the same result as accumulating the
 * whole, and {@link #drain()} can hand off deltas at any time. Not thread-safe.
 */
public class MotionVectorAccumulator implements Consumer<MotionSlice<?>> {
  private final MotionVectorLayout layout;
  private final Statistic<?> statistic;
  private Map<MotionCellKey, AdditiveVectorValue> values = new HashMap<>();

  public MotionVectorAccumulator(final MotionVectorLayout layout) {
    this(layout, null);
  }

  /** @param statistic the statistic the values will be written to, or null */
  public MotionVectorAccumulator(final MotionVectorLayout layout, final Statistic<?> statistic) {
    this.layout = layout;
    this.statistic = statistic;
    // fails fast when the statistic holds a different layout
    layout.createValue(statistic);
  }

  @Override
  public void accept(final MotionSlice<?> slice) {
    if (slice.isBridged()) {
      return;
    }
    switch (slice.getType()) {
      case OCCUPANCY:
        addOccupancy(slice);
        break;
      case VISIT_END:
        addVisit(slice);
        break;
      case TRANSITION:
        value(slice).addCount(
            MotionVectorLayout.flowIndex(slice.getEntrySlot(), slice.getExitSlot()),
            1);
        break;
      case GAP_START:
        value(slice).addCount(MotionVectorLayout.GAP_STARTS, 1);
        break;
      case GAP_END:
        value(slice).addCount(
            MotionVectorLayout.GAP_DURATION_HISTOGRAM
                + layout.gapDurationBin(slice.getDurationSeconds()),
            1);
        break;
      case STOP:
        value(slice).addCount(MotionVectorLayout.STOP_STARTS, 1);
        break;
      default:
        break;
    }
  }

  private void addOccupancy(final MotionSlice<?> slice) {
    final AdditiveVectorValue value = value(slice);
    value.addSum(MotionVectorLayout.MOVING_SECONDS, slice.getMovingSeconds());
    value.addSum(MotionVectorLayout.STOPPED_SECONDS, slice.getStoppedSeconds());
    if (slice.isStableIdentity()) {
      value.addSum(
          MotionVectorLayout.STABLE_SECONDS,
          slice.getMovingSeconds() + slice.getStoppedSeconds());
    }
    value.addSum(MotionVectorLayout.DISTANCE_METERS, slice.getDistanceMeters());
  }

  private void addVisit(final MotionSlice<?> slice) {
    final AdditiveVectorValue value = value(slice);
    value.addCount(MotionVectorLayout.VISITS, 1);
    if (!Double.isNaN(slice.getSpeed())) {
      value.addCount(MotionVectorLayout.SPEED_HISTOGRAM + layout.speedBin(slice.getSpeed()), 1);
    }
    if (!Double.isNaN(slice.getHeading())) {
      final double theta = Math.toRadians(slice.getHeading());
      value.addCount(
          MotionVectorLayout.HEADING_ROSE + MotionVectorLayout.headingSector(slice.getHeading()),
          1);
      value.addSum(MotionVectorLayout.HEADING_COS, Math.cos(theta));
      value.addSum(MotionVectorLayout.HEADING_SIN, Math.sin(theta));
      value.addSum(MotionVectorLayout.HEADING_COS2, Math.cos(2 * theta));
      value.addSum(MotionVectorLayout.HEADING_SIN2, Math.sin(2 * theta));
    }
    if ((slice.getDomain() == MotionDomain.AIR) && !Double.isNaN(slice.getAltitude())) {
      value.addCount(
          MotionVectorLayout.ALTITUDE_HISTOGRAM + layout.altitudeBin(slice.getAltitude()),
          1);
    }
    final double traversal = slice.getTraversalSeconds();
    if (!Double.isNaN(traversal)) {
      final int exit = slice.getExitSlot();
      value.addCount(MotionVectorLayout.EXIT_TRAVERSALS + exit, 1);
      value.addSum(MotionVectorLayout.EXIT_TRAVERSAL_SECONDS + exit, traversal);
      value.addSum(MotionVectorLayout.EXIT_TRAVERSAL_SECONDS_SQUARED + exit, traversal * traversal);
    }
  }

  private AdditiveVectorValue value(final MotionSlice<?> slice) {
    return values.computeIfAbsent(
        new MotionCellKey(slice.getBucketStart(), slice.getLevel(), slice.getCell()),
        k -> layout.createValue(statistic));
  }

  /** The values accumulated since construction or the last {@link #drain()}. */
  public Map<MotionCellKey, AdditiveVectorValue> getValues() {
    return Collections.unmodifiableMap(values);
  }

  /** Returns the values accumulated so far and starts over. */
  public Map<MotionCellKey, AdditiveVectorValue> drain() {
    final Map<MotionCellKey, AdditiveVectorValue> drained = values;
    values = new HashMap<>();
    return drained;
  }

  public MotionVectorLayout getLayout() {
    return layout;
  }
}
