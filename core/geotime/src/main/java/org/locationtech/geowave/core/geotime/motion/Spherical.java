/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.motion;

/**
 * Great-circle geometry on unit vectors, which has no special cases at the antimeridian or the
 * poles. A chord between two reports is the great-circle arc, the straight line of any gnomonic
 * (local tangent plane) projection and so of H3's face projections.
 */
final class Spherical {
  static final double EARTH_RADIUS_METERS = 6371008.8;

  private Spherical() {}

  static void toVector(final double latitude, final double longitude, final double[] out) {
    final double lat = Math.toRadians(latitude);
    final double lon = Math.toRadians(longitude);
    final double cosLat = Math.cos(lat);
    out[0] = cosLat * Math.cos(lon);
    out[1] = cosLat * Math.sin(lon);
    out[2] = Math.sin(lat);
  }

  static double latitude(final double[] v) {
    return Math.toDegrees(Math.atan2(v[2], Math.hypot(v[0], v[1])));
  }

  static double longitude(final double[] v) {
    return Math.toDegrees(Math.atan2(v[1], v[0]));
  }

  /** The angle between two unit vectors in radians, accurate for tiny and large angles alike. */
  static double angle(final double[] a, final double[] b) {
    final double cx = (a[1] * b[2]) - (a[2] * b[1]);
    final double cy = (a[2] * b[0]) - (a[0] * b[2]);
    final double cz = (a[0] * b[1]) - (a[1] * b[0]);
    return Math.atan2(
        Math.sqrt((cx * cx) + (cy * cy) + (cz * cz)),
        (a[0] * b[0]) + (a[1] * b[1]) + (a[2] * b[2]));
  }

  /** The point a fraction {@code s} of the way along the arc from a to b, which spans theta. */
  static void interpolate(
      final double[] a,
      final double[] b,
      final double theta,
      final double s,
      final double[] out) {
    final double wa;
    final double wb;
    if (theta < 1e-9) {
      wa = 1 - s;
      wb = s;
    } else {
      final double sinTheta = Math.sin(theta);
      wa = Math.sin((1 - s) * theta) / sinTheta;
      wb = Math.sin(s * theta) / sinTheta;
    }
    final double x = (wa * a[0]) + (wb * b[0]);
    final double y = (wa * a[1]) + (wb * b[1]);
    final double z = (wa * a[2]) + (wb * b[2]);
    final double norm = Math.sqrt((x * x) + (y * y) + (z * z));
    out[0] = x / norm;
    out[1] = y / norm;
    out[2] = z / norm;
  }

  static double distance(
      final double lat1,
      final double lon1,
      final double lat2,
      final double lon2) {
    final double phi1 = Math.toRadians(lat1);
    final double phi2 = Math.toRadians(lat2);
    final double sinDLat = Math.sin((phi2 - phi1) / 2);
    final double sinDLon = Math.sin(Math.toRadians(lon2 - lon1) / 2);
    final double h = (sinDLat * sinDLat) + (Math.cos(phi1) * Math.cos(phi2) * sinDLon * sinDLon);
    return 2 * EARTH_RADIUS_METERS * Math.asin(Math.min(1, Math.sqrt(h)));
  }

  /** The initial great-circle bearing from the first point to the second, in [0, 360). */
  static double bearing(
      final double lat1,
      final double lon1,
      final double lat2,
      final double lon2) {
    final double phi1 = Math.toRadians(lat1);
    final double phi2 = Math.toRadians(lat2);
    final double dLon = Math.toRadians(lon2 - lon1);
    final double y = Math.sin(dLon) * Math.cos(phi2);
    final double x =
        (Math.cos(phi1) * Math.sin(phi2)) - (Math.sin(phi1) * Math.cos(phi2) * Math.cos(dLon));
    final double degrees = Math.toDegrees(Math.atan2(y, x));
    return degrees < 0 ? degrees + 360 : degrees;
  }

  /** The distance from p to the arc from a to b, all unit vectors. */
  static double distanceToArc(final double[] p, final double[] a, final double[] b) {
    double nx = (a[1] * b[2]) - (a[2] * b[1]);
    double ny = (a[2] * b[0]) - (a[0] * b[2]);
    double nz = (a[0] * b[1]) - (a[1] * b[0]);
    final double norm = Math.sqrt((nx * nx) + (ny * ny) + (nz * nz));
    final double toEnds = Math.min(angle(p, a), angle(p, b)) * EARTH_RADIUS_METERS;
    if (norm < 1e-15) {
      return toEnds;
    }
    nx /= norm;
    ny /= norm;
    nz /= norm;
    final double offPlane = (p[0] * nx) + (p[1] * ny) + (p[2] * nz);
    // the foot of the perpendicular lies within the arc when it is on the inner side of both ends
    final double cx = p[0] - (offPlane * nx);
    final double cy = p[1] - (offPlane * ny);
    final double cz = p[2] - (offPlane * nz);
    final double fromA =
        (((a[1] * cz) - (a[2] * cy)) * nx)
            + (((a[2] * cx) - (a[0] * cz)) * ny)
            + (((a[0] * cy) - (a[1] * cx)) * nz);
    final double toB =
        (((cy * b[2]) - (cz * b[1])) * nx)
            + (((cz * b[0]) - (cx * b[2])) * ny)
            + (((cx * b[1]) - (cy * b[0])) * nz);
    if ((fromA >= 0) && (toB >= 0)) {
      return Math.asin(Math.min(1, Math.abs(offPlane))) * EARTH_RADIUS_METERS;
    }
    return toEnds;
  }
}
