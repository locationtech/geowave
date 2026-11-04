/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.motion;

import java.util.Arrays;

/**
 * Decides when consecutive reports form a continuous path, when they are a gap, and when an entity
 * is stopped.
 *
 * <p><b>Continuity.</b> Reports separated by at most the source's max interval plus a slack are
 * joined by a straight chord, provided the chord is plausible. The slack is the largest of 60 s,
 * 20% of the max interval and, when the source declares one, twice its raw report interval plus a
 * 60 s jitter margin: admission can keep reports up to the max interval plus one raw interval
 * apart, and one lost raw report adds another. A source's allowed lateness delays the timeout, not
 * the window.
 *
 * <p><b>Plausibility.</b> A chord is implausible when it implies a speed above the domain's limit,
 * after allowing twice the position sigma for report error over at least one second, or is longer
 * than the domain's max chord. It is measured between the reports' own positions, even while the
 * path is anchored at a stop. An implausible chord breaks continuity like a gap.
 *
 * <p><b>Bridging.</b> A gap no longer than the domain's bridged window whose chord is plausible is
 * additionally emitted on the bridged channel.
 *
 * <p><b>Stops.</b> A report slower than the stop speed (0.5 m/s) anchors the position and locks the
 * cell. The stop ends when at least two consecutive reports faster than the leave speed (1 m/s)
 * span at least 30 s, or on a report farther from the anchor than the stop radius,
 * {@code max(domain minimum, 2 x (tolerance + sigma))}. Reports without a speed enter a stop by
 * their displacement less twice the sigma over a baseline of at least
 * {@code max(60 s, 4 x sigma / stop speed)}, so that the noise allowance costs at most half the
 * stop speed, and leave it only by the radius.
 *
 * <p><b>Domain defaults</b> (bridged window, max speed, max chord, minimum stop radius): SURFACE 2
 * h, 40 m/s, 50 km, 300 m; AIR 10 min, 400 m/s, 300 km, 300 m; GROUND 30 min, 60 m/s, 50 km, 100 m;
 * UNKNOWN 30 min, 400 m/s, 300 km, 300 m. UNKNOWN uses the most permissive plausibility limits, so
 * an unclassified entity is never broken up for moving like an aircraft.
 */
public final class GapPolicy {
  private static final int DOMAINS = MotionDomain.values().length;

  private final long[] bridgedWindowMillis;
  private final double[] maxSpeed;
  private final double[] maxChordMeters;
  private final double[] minStopRadiusMeters;
  private final long continuitySlackMinMillis;
  private final double continuitySlackFraction;
  private final long rawIntervalMarginMillis;
  private final double stopEnterSpeed;
  private final double stopLeaveSpeed;
  private final int stopLeaveReports;
  private final long stopLeaveMinMillis;
  private final long speedlessBaselineMinMillis;

  private GapPolicy(final Builder builder) {
    bridgedWindowMillis = builder.bridgedWindowMillis.clone();
    maxSpeed = builder.maxSpeed.clone();
    maxChordMeters = builder.maxChordMeters.clone();
    minStopRadiusMeters = builder.minStopRadiusMeters.clone();
    continuitySlackMinMillis = builder.continuitySlackMinMillis;
    continuitySlackFraction = builder.continuitySlackFraction;
    rawIntervalMarginMillis = builder.rawIntervalMarginMillis;
    stopEnterSpeed = builder.stopEnterSpeed;
    stopLeaveSpeed = builder.stopLeaveSpeed;
    stopLeaveReports = builder.stopLeaveReports;
    stopLeaveMinMillis = builder.stopLeaveMinMillis;
    speedlessBaselineMinMillis = builder.speedlessBaselineMinMillis;
  }

  public static GapPolicy defaults() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * The longest separation between two reports of {@code source} that is still continuous;
   * Long.MAX_VALUE where the sum overflows.
   */
  public long getContinuityWindowMillis(final MotionSource source) {
    final long interval = source.getMaxIntervalMillis();
    long slack =
        Math.max(continuitySlackMinMillis, (long) Math.ceil(continuitySlackFraction * interval));
    final long raw = source.getRawIntervalMillis();
    if (raw > 0) {
      slack = Math.max(slack, saturatedAdd(saturatedAdd(raw, raw), rawIntervalMarginMillis));
    }
    return saturatedAdd(interval, slack);
  }

  /**
   * How long after its last report an entity of {@code source} is put in a gap: the continuity
   * window plus the source's allowed lateness, or Long.MAX_VALUE where that overflows.
   */
  public long getTimeoutMillis(final MotionSource source) {
    return saturatedAdd(getContinuityWindowMillis(source), source.getAllowedLatenessMillis());
  }

  /** {@code a + b} for a non-negative {@code b}, or Long.MAX_VALUE where that overflows. */
  static long saturatedAdd(final long a, final long b) {
    final long sum = a + b;
    return sum < a ? Long.MAX_VALUE : sum;
  }

  /** {@code a - b} for a non-negative {@code b}, or Long.MIN_VALUE where that overflows. */
  static long saturatedSubtract(final long a, final long b) {
    final long difference = a - b;
    return difference > a ? Long.MIN_VALUE : difference;
  }

  public long getBridgedWindowMillis(final MotionDomain domain) {
    return bridgedWindowMillis[domain.ordinal()];
  }

  /** In m/s. */
  public double getMaxSpeed(final MotionDomain domain) {
    return maxSpeed[domain.ordinal()];
  }

  public double getMaxChordMeters(final MotionDomain domain) {
    return maxChordMeters[domain.ordinal()];
  }

  public double getStopRadiusMeters(final MotionDomain domain, final MotionSource source) {
    return Math.max(
        minStopRadiusMeters[domain.ordinal()],
        2 * (source.getToleranceMeters() + source.getPositionSigmaMeters()));
  }

  /** In m/s. */
  public double getStopEnterSpeed() {
    return stopEnterSpeed;
  }

  /** In m/s. */
  public double getStopLeaveSpeed() {
    return stopLeaveSpeed;
  }

  public int getStopLeaveReports() {
    return stopLeaveReports;
  }

  /** The shortest span of consecutive fast reports that ends a stop. */
  public long getStopLeaveMinMillis() {
    return stopLeaveMinMillis;
  }

  /** The shortest time over which the speed of reports without one is measured. */
  public long getSpeedlessBaselineMillis(final MotionSource source) {
    return Math.max(
        speedlessBaselineMinMillis,
        (long) Math.ceil((4000 * source.getPositionSigmaMeters()) / stopEnterSpeed));
  }

  /** Whether a chord of {@code meters} over {@code millis} could be real movement. */
  public boolean isPlausible(
      final MotionDomain domain,
      final MotionSource source,
      final double meters,
      final long millis) {
    final double seconds = Math.max(millis / 1000.0, 1.0);
    final double speed = Math.max(0, meters - (2 * source.getPositionSigmaMeters())) / seconds;
    return (speed <= getMaxSpeed(domain)) && (meters <= getMaxChordMeters(domain));
  }

  public static final class Builder {
    private final long[] bridgedWindowMillis = new long[DOMAINS];
    private final double[] maxSpeed = new double[DOMAINS];
    private final double[] maxChordMeters = new double[DOMAINS];
    private final double[] minStopRadiusMeters = new double[DOMAINS];
    private long continuitySlackMinMillis = 60_000L;
    private double continuitySlackFraction = 0.2;
    private long rawIntervalMarginMillis = 60_000L;
    private double stopEnterSpeed = 0.5;
    private double stopLeaveSpeed = 1.0;
    private int stopLeaveReports = 2;
    private long stopLeaveMinMillis = 30_000L;
    private long speedlessBaselineMinMillis = 60_000L;

    private Builder() {
      domain(MotionDomain.SURFACE, 2 * 3_600_000L, 40, 50_000, 300);
      domain(MotionDomain.AIR, 10 * 60_000L, 400, 300_000, 300);
      domain(MotionDomain.GROUND, 30 * 60_000L, 60, 50_000, 100);
      domain(MotionDomain.UNKNOWN, 30 * 60_000L, 400, 300_000, 300);
    }

    private void domain(
        final MotionDomain domain,
        final long bridgedWindow,
        final double speed,
        final double chord,
        final double stopRadius) {
      bridgedWindowMillis[domain.ordinal()] = bridgedWindow;
      maxSpeed[domain.ordinal()] = speed;
      maxChordMeters[domain.ordinal()] = chord;
      minStopRadiusMeters[domain.ordinal()] = stopRadius;
    }

    public Builder bridgedWindowMillis(final MotionDomain domain, final long millis) {
      bridgedWindowMillis[domain.ordinal()] = requireNonNegative(millis);
      return this;
    }

    public Builder maxSpeed(final MotionDomain domain, final double metersPerSecond) {
      maxSpeed[domain.ordinal()] = requirePositive(metersPerSecond);
      return this;
    }

    public Builder maxChordMeters(final MotionDomain domain, final double meters) {
      maxChordMeters[domain.ordinal()] = requirePositive(meters);
      return this;
    }

    public Builder minStopRadiusMeters(final MotionDomain domain, final double meters) {
      minStopRadiusMeters[domain.ordinal()] = requireNonNegative(meters);
      return this;
    }

    public Builder continuitySlack(final long minMillis, final double fraction) {
      continuitySlackMinMillis = requireNonNegative(minMillis);
      continuitySlackFraction = requireNonNegative(fraction);
      return this;
    }

    /** The jitter margin added to twice a source's raw report interval; default 60 s. */
    public Builder rawIntervalMarginMillis(final long millis) {
      rawIntervalMarginMillis = requireNonNegative(millis);
      return this;
    }

    public Builder stopSpeeds(final double enter, final double leave, final int leaveReports) {
      if (!(enter > 0) || !(leave >= enter) || (leaveReports < 1)) {
        throw new IllegalArgumentException(
            "Stop speeds need 0 < enter <= leave and at least one leave report");
      }
      stopEnterSpeed = enter;
      stopLeaveSpeed = leave;
      stopLeaveReports = leaveReports;
      return this;
    }

    /** The shortest span of consecutive fast reports that ends a stop; default 30 s. */
    public Builder stopLeaveMinMillis(final long millis) {
      stopLeaveMinMillis = requireNonNegative(millis);
      return this;
    }

    /** The shortest baseline for the speed of reports without one; default 60 s. */
    public Builder speedlessBaselineMinMillis(final long millis) {
      speedlessBaselineMinMillis = requireNonNegative(millis);
      return this;
    }

    public GapPolicy build() {
      return new GapPolicy(this);
    }

    private static long requireNonNegative(final long value) {
      if (value < 0) {
        throw new IllegalArgumentException("Expected a non-negative value");
      }
      return value;
    }

    private static double requireNonNegative(final double value) {
      if (!(value >= 0)) {
        throw new IllegalArgumentException("Expected a non-negative value");
      }
      return value;
    }

    private static double requirePositive(final double value) {
      if (!(value > 0)) {
        throw new IllegalArgumentException("Expected a positive value");
      }
      return value;
    }
  }

  @Override
  public String toString() {
    return "GapPolicy [bridgedWindowMillis="
        + Arrays.toString(bridgedWindowMillis)
        + ", maxSpeed="
        + Arrays.toString(maxSpeed)
        + ", maxChordMeters="
        + Arrays.toString(maxChordMeters)
        + ", minStopRadiusMeters="
        + Arrays.toString(minStopRadiusMeters)
        + ", slack="
        + continuitySlackMinMillis
        + " ms/"
        + continuitySlackFraction
        + "/2 x raw+"
        + rawIntervalMarginMillis
        + " ms, stop="
        + stopEnterSpeed
        + "/"
        + stopLeaveSpeed
        + " m/s x"
        + stopLeaveReports
        + " over "
        + stopLeaveMinMillis
        + " ms, speedless baseline "
        + speedlessBaselineMinMillis
        + " ms]";
  }
}
