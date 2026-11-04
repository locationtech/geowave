/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.motion;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.geotime.motion.MotionSlice.Type;

final class MotionTestSupport {
  /** A maritime-like feed: 200 m tolerance, 10 m sigma, a report at least every 10 minutes. */
  static final MotionSource MARITIME = new MotionSource("maritime", 200, 10, 600_000L);
  /** Half past an hour, so tracks cross hour boundaries early. */
  static final long T0 = 1_700_000_000_000L - (1_700_000_000_000L % 3_600_000L) + 1_800_000L;

  private MotionTestSupport() {}

  static double[] destination(
      final double latitude,
      final double longitude,
      final double bearing,
      final double meters) {
    final double delta = meters / Spherical.EARTH_RADIUS_METERS;
    final double theta = Math.toRadians(bearing);
    final double phi1 = Math.toRadians(latitude);
    final double lambda1 = Math.toRadians(longitude);
    final double phi2 =
        Math.asin(
            (Math.sin(phi1) * Math.cos(delta))
                + (Math.cos(phi1) * Math.sin(delta) * Math.cos(theta)));
    final double lambda2 =
        lambda1
            + Math.atan2(
                Math.sin(theta) * Math.sin(delta) * Math.cos(phi1),
                Math.cos(delta) - (Math.sin(phi1) * Math.sin(phi2)));
    double lon = Math.toDegrees(lambda2);
    lon = ((lon + 540) % 360) - 180;
    return new double[] {Math.toDegrees(phi2), lon};
  }

  static MotionReport<String> report(
      final String id,
      final boolean stable,
      final long time,
      final double latitude,
      final double longitude,
      final double speed,
      final double course) {
    return new MotionReport<>(
        id,
        stable,
        MotionDomain.SURFACE,
        MARITIME,
        time,
        latitude,
        longitude,
        speed,
        course,
        Double.NaN);
  }

  /** A great-circle track at constant speed, one report every {@code intervalMillis}. */
  static List<MotionReport<String>> straightTrack(
      final String id,
      final boolean stable,
      final double latitude,
      final double longitude,
      final double bearing,
      final double speed,
      final long start,
      final long intervalMillis,
      final int count) {
    final List<MotionReport<String>> reports = new ArrayList<>();
    final double[] from = new double[3];
    final double[] far = new double[3];
    final double[] point = new double[3];
    Spherical.toVector(latitude, longitude, from);
    final double total = (speed * (count - 1) * intervalMillis) / 1000.0;
    final double[] end = destination(latitude, longitude, bearing, total);
    Spherical.toVector(end[0], end[1], far);
    final double theta = Spherical.angle(from, far);
    for (int i = 0; i < count; i++) {
      final double s = count == 1 ? 0 : (double) i / (count - 1);
      Spherical.interpolate(from, far, theta, s, point);
      final double lat = Spherical.latitude(point);
      final double lon = Spherical.longitude(point);
      final double course = i + 1 < count ? Spherical.bearing(lat, lon, end[0], end[1]) : bearing;
      reports.add(report(id, stable, start + (i * intervalMillis), lat, lon, speed, course));
    }
    return reports;
  }

  /**
   * Dead-reckoning admission: keeps a report when the prediction from the last kept report (its
   * position moved along its course at its speed) misses it by more than the tolerance, or when the
   * max interval has elapsed. The first and last reports are always kept.
   */
  static List<MotionReport<String>> admit(
      final List<MotionReport<String>> reports,
      final double toleranceMeters,
      final long maxIntervalMillis) {
    final List<MotionReport<String>> kept = new ArrayList<>();
    MotionReport<String> last = null;
    for (int i = 0; i < reports.size(); i++) {
      final MotionReport<String> report = reports.get(i);
      boolean keep = (last == null) || (i == (reports.size() - 1));
      if (!keep) {
        final long elapsed = report.getTime() - last.getTime();
        final double[] predicted =
            destination(
                last.getLatitude(),
                last.getLongitude(),
                last.getCourse(),
                (last.getSpeed() * elapsed) / 1000.0);
        keep =
            (elapsed >= maxIntervalMillis)
                || (Spherical.distance(
                    predicted[0],
                    predicted[1],
                    report.getLatitude(),
                    report.getLongitude()) > toleranceMeters);
      }
      if (keep) {
        kept.add(report);
        last = report;
      }
    }
    return kept;
  }

  static double gaussian(final Random random, final double sigma) {
    return random.nextGaussian() * sigma;
  }

  static final class Recorder implements Consumer<MotionSlice<String>> {
    final List<MotionSlice<String>> slices = new ArrayList<>();

    @Override
    public void accept(final MotionSlice<String> slice) {
      slices.add(slice);
    }

    List<MotionSlice<String>> of(final Type type) {
      return where(s -> s.getType() == type);
    }

    List<MotionSlice<String>> of(final Type type, final int level) {
      return where(s -> (s.getType() == type) && (s.getLevel() == level));
    }

    List<MotionSlice<String>> where(final Predicate<MotionSlice<String>> predicate) {
      return slices.stream().filter(predicate).collect(Collectors.toList());
    }

    double seconds(final int level, final boolean bridged) {
      return of(Type.OCCUPANCY, level).stream().filter(s -> s.isBridged() == bridged).mapToDouble(
          s -> s.getMovingSeconds() + s.getStoppedSeconds()).sum();
    }
  }
}
