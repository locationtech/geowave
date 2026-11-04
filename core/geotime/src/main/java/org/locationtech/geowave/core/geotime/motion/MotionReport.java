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
 * One admitted position report for an entity.
 *
 * <p> The entity key is opaque; it only needs {@code equals} and {@code hashCode}. An entity has a
 * stable identity when its key always denotes the same physical object (a hull or airframe
 * identifier). Keys that rotate or get reassigned are unstable, and such entities contribute
 * occupancy and neighbor-to-neighbor transitions only. Stability, domain and source are taken from
 * an entity's first report; a later report with a different domain or source ends the entity and
 * starts it again (counted by {@link H3MotionSegmenter#getRestarts()}), so one key should always
 * arrive with one domain and one {@link MotionSource}.
 *
 * <p> Speed is in m/s, course in degrees clockwise from north and altitude in meters; each is NaN
 * when unknown. Without a speed, stops are judged from displacement over a baseline (see
 * {@link GapPolicy}). Course is not used by the current mechanics.
 */
public final class MotionReport<K> {
  private final K entityKey;
  private final boolean stableIdentity;
  private final MotionDomain domain;
  private final MotionSource source;
  private final long time;
  private final double latitude;
  private final double longitude;
  private final double speed;
  private final double course;
  private final double altitude;

  public MotionReport(
      final K entityKey,
      final boolean stableIdentity,
      final MotionDomain domain,
      final MotionSource source,
      final long time,
      final double latitude,
      final double longitude) {
    this(
        entityKey,
        stableIdentity,
        domain,
        source,
        time,
        latitude,
        longitude,
        Double.NaN,
        Double.NaN,
        Double.NaN);
  }

  public MotionReport(
      final K entityKey,
      final boolean stableIdentity,
      final MotionDomain domain,
      final MotionSource source,
      final long time,
      final double latitude,
      final double longitude,
      final double speed,
      final double course,
      final double altitude) {
    this.entityKey = Objects.requireNonNull(entityKey, "entityKey");
    this.stableIdentity = stableIdentity;
    this.domain = Objects.requireNonNull(domain, "domain");
    this.source = Objects.requireNonNull(source, "source");
    this.time = time;
    this.latitude = latitude;
    this.longitude = longitude;
    this.speed = speed;
    this.course = course;
    this.altitude = altitude;
  }

  public K getEntityKey() {
    return entityKey;
  }

  public boolean isStableIdentity() {
    return stableIdentity;
  }

  public MotionDomain getDomain() {
    return domain;
  }

  public MotionSource getSource() {
    return source;
  }

  /** Epoch milliseconds. */
  public long getTime() {
    return time;
  }

  public double getLatitude() {
    return latitude;
  }

  public double getLongitude() {
    return longitude;
  }

  public double getSpeed() {
    return speed;
  }

  public boolean hasSpeed() {
    return Double.isFinite(speed);
  }

  public double getCourse() {
    return course;
  }

  public double getAltitude() {
    return altitude;
  }

  @Override
  public String toString() {
    return entityKey
        + "@"
        + time
        + " ("
        + latitude
        + ", "
        + longitude
        + ") speed "
        + speed
        + " course "
        + course
        + " altitude "
        + altitude;
  }
}
