/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.motion;

/** Everything the segmenter derives from one (domain, source) pair, shared by its entities. */
final class MotionProfile {
  final MotionDomain domain;
  final MotionSource source;
  /** Contributing levels, finest (the crossing level) first. */
  final int[] levels;
  final long windowMillis;
  final long timeoutMillis;
  final long bridgedWindowMillis;
  final double stopRadiusMeters;
  final long speedlessBaselineMillis;
  /**
   * Per level, excursions no deeper than this merge back into the visit they left. At a derived
   * level the depth is measured from the coarse hexagons' shared edge, which a derived cell's
   * jagged boundary strays from by about a third of the next finer level's edge, so the allowance
   * there is the diameter of a cell two levels finer (or of a crossing-level cell, if that is
   * coarser).
   */
  final double[] excursionDepthMeters;
  /**
   * Per level, how long an excursion may stay pending: a continuity window at the crossing level,
   * scaled by the ratio of edge lengths at derived levels, since tracking a jagged coarse boundary
   * takes proportionally longer.
   */
  final long[] excursionMaxMillis;
  /** Visit displacements shorter than this have no heading. */
  final double headingMinMeters;
  /** Chords are sampled at this spacing before bisecting to the crossings. */
  final double stepMeters;
  /** Same-cell samples in a distorted cell are densified to this spacing; 0 at Class II levels. */
  final double distortedStepMeters;

  MotionProfile(
      final MotionDomain domain,
      final MotionSource source,
      final GapPolicy gapPolicy,
      final ResolutionPlan resolutionPlan) {
    this.domain = domain;
    this.source = source;
    levels = resolutionPlan.getContributingLevels(domain, source);
    windowMillis = gapPolicy.getContinuityWindowMillis(source);
    timeoutMillis = gapPolicy.getTimeoutMillis(source);
    bridgedWindowMillis = gapPolicy.getBridgedWindowMillis(domain);
    stopRadiusMeters = gapPolicy.getStopRadiusMeters(domain, source);
    speedlessBaselineMillis = gapPolicy.getSpeedlessBaselineMillis(source);
    final double tolerance = Math.max(source.getToleranceMeters(), source.getPositionSigmaMeters());
    excursionDepthMeters = new double[levels.length];
    excursionMaxMillis = new long[levels.length];
    for (int i = 0; i < levels.length; i++) {
      excursionDepthMeters[i] =
          i == 0 ? tolerance
              : (2 * H3Holder.averageEdgeMeters(Math.min(levels[i] + 2, levels[0]))) + tolerance;
      excursionMaxMillis[i] =
          (long) Math.ceil(
              (windowMillis * H3Holder.averageEdgeMeters(levels[i]))
                  / H3Holder.averageEdgeMeters(levels[0]));
    }
    headingMinMeters = source.getToleranceMeters() + source.getPositionSigmaMeters();
    stepMeters = levels.length == 0 ? 0 : H3Holder.averageEdgeMeters(levels[0]) / 2;
    distortedStepMeters =
        (levels.length == 0) || ((levels[0] % 2) == 0) ? 0
            : Math.max(1, H3Holder.averageEdgeMeters(levels[0]) / 1000);
  }
}
