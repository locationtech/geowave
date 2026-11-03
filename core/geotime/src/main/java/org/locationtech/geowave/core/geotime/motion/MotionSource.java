/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.motion;

import java.util.Objects;

/**
 * How a source's reports were thinned, how accurate they are and how late they can arrive.
 *
 * <p> Reports are expected to have passed dead-reckoning admission: a report was dropped only when
 * it fell within {@code toleranceMeters} of the straight-line prediction from the last admitted
 * report, which is what makes a straight chord between consecutive admitted reports a faithful
 * path. Admission keeps the first report at or after {@code maxIntervalMillis} since the last kept
 * one, so kept reports can be up to the max interval plus the source's raw report interval apart,
 * and one more raw interval when that report was lost. Set {@link #withRawIntervalMillis(long) the
 * raw interval} to the source's slowest regular cadence (for example 3 minutes for a vessel at
 * anchor): the continuity window then covers that spacing with one raw report lost.
 *
 * <p> {@link #withAllowedLatenessMillis(long) Allowed lateness} is how far one entity's reports may
 * trail another's in arrival (for example regions of a feed delivered out of order). An entity is
 * put in a gap only once its continuity window plus this lateness has passed, and the segmenter's
 * watermark backs off by the same amount.
 *
 * <p> For example, a maritime position feed might use a 200 m tolerance, a 10 m sigma, a 10 minute
 * max interval, a 3 minute raw interval and 10 minutes of lateness (a 17 minute continuity window
 * and a 27 minute timeout), and an air feed a 1 km tolerance and a 90 second interval. Sources are
 * value objects: two sources with equal parameters are interchangeable. One entity key should
 * always arrive with one source: a report whose source differs from its entity's ends the entity
 * and starts it again (see {@link H3MotionSegmenter#getRestarts()}), so map a family of feeds to
 * one source.
 */
public final class MotionSource {
  private static final int DERIVED_LEVEL = -1;

  private final String name;
  private final double toleranceMeters;
  private final double positionSigmaMeters;
  private final long maxIntervalMillis;
  private final long rawIntervalMillis;
  private final long allowedLatenessMillis;
  private final int maxLevel;

  public MotionSource(
      final String name,
      final double toleranceMeters,
      final double positionSigmaMeters,
      final long maxIntervalMillis) {
    this(name, toleranceMeters, positionSigmaMeters, maxIntervalMillis, 0, 0, DERIVED_LEVEL);
  }

  private MotionSource(
      final String name,
      final double toleranceMeters,
      final double positionSigmaMeters,
      final long maxIntervalMillis,
      final long rawIntervalMillis,
      final long allowedLatenessMillis,
      final int maxLevel) {
    if (!(toleranceMeters >= 0) || !(positionSigmaMeters >= 0)) {
      throw new IllegalArgumentException("Tolerance and sigma must be non-negative");
    }
    if (maxIntervalMillis <= 0) {
      throw new IllegalArgumentException("The max report interval must be positive");
    }
    if ((rawIntervalMillis < 0) || (allowedLatenessMillis < 0)) {
      throw new IllegalArgumentException("The raw interval and lateness must be non-negative");
    }
    if ((maxLevel != DERIVED_LEVEL) && ((maxLevel < 0) || (maxLevel > 15))) {
      throw new IllegalArgumentException("The max level must be an H3 resolution (0-15)");
    }
    this.name = Objects.requireNonNull(name, "name");
    this.toleranceMeters = toleranceMeters;
    this.positionSigmaMeters = positionSigmaMeters;
    this.maxIntervalMillis = maxIntervalMillis;
    this.rawIntervalMillis = rawIntervalMillis;
    this.allowedLatenessMillis = allowedLatenessMillis;
    this.maxLevel = maxLevel;
  }

  /**
   * Returns a copy that contributes only at {@code maxLevel} and coarser, instead of the cap
   * {@link ResolutionPlan} derives from the tolerance and sigma.
   */
  public MotionSource withMaxLevel(final int level) {
    return new MotionSource(
        name,
        toleranceMeters,
        positionSigmaMeters,
        maxIntervalMillis,
        rawIntervalMillis,
        allowedLatenessMillis,
        level);
  }

  /**
   * Returns a copy whose slowest regular raw report interval is {@code millis}; 0, the default,
   * means unknown, and the continuity window then has only the {@link GapPolicy}'s minimum slack.
   */
  public MotionSource withRawIntervalMillis(final long millis) {
    return new MotionSource(
        name,
        toleranceMeters,
        positionSigmaMeters,
        maxIntervalMillis,
        millis,
        allowedLatenessMillis,
        maxLevel);
  }

  /** Returns a copy that allows its reports to arrive up to {@code millis} late; default 0. */
  public MotionSource withAllowedLatenessMillis(final long millis) {
    return new MotionSource(
        name,
        toleranceMeters,
        positionSigmaMeters,
        maxIntervalMillis,
        rawIntervalMillis,
        millis,
        maxLevel);
  }

  public String getName() {
    return name;
  }

  public double getToleranceMeters() {
    return toleranceMeters;
  }

  public double getPositionSigmaMeters() {
    return positionSigmaMeters;
  }

  public long getMaxIntervalMillis() {
    return maxIntervalMillis;
  }

  /** The slowest regular raw report interval, or 0 when unknown. */
  public long getRawIntervalMillis() {
    return rawIntervalMillis;
  }

  public long getAllowedLatenessMillis() {
    return allowedLatenessMillis;
  }

  public boolean hasMaxLevel() {
    return maxLevel != DERIVED_LEVEL;
  }

  /** The explicit level cap, or -1 when the cap is derived. */
  public int getMaxLevel() {
    return maxLevel;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof MotionSource)) {
      return false;
    }
    final MotionSource other = (MotionSource) obj;
    return name.equals(other.name)
        && (Double.compare(toleranceMeters, other.toleranceMeters) == 0)
        && (Double.compare(positionSigmaMeters, other.positionSigmaMeters) == 0)
        && (maxIntervalMillis == other.maxIntervalMillis)
        && (rawIntervalMillis == other.rawIntervalMillis)
        && (allowedLatenessMillis == other.allowedLatenessMillis)
        && (maxLevel == other.maxLevel);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name,
        toleranceMeters,
        positionSigmaMeters,
        maxIntervalMillis,
        rawIntervalMillis,
        allowedLatenessMillis,
        maxLevel);
  }

  @Override
  public String toString() {
    return name
        + " (tolerance "
        + toleranceMeters
        + " m, sigma "
        + positionSigmaMeters
        + " m, max interval "
        + maxIntervalMillis
        + " ms, raw interval "
        + rawIntervalMillis
        + " ms, lateness "
        + allowedLatenessMillis
        + " ms"
        + (hasMaxLevel() ? ", max level " + maxLevel : "")
        + ")";
  }
}
