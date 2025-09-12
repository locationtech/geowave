/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.examples.stats;

import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.TDigestNumericHistogram;
import org.locationtech.geowave.core.store.statistics.field.FieldStatisticType;
import org.locationtech.geowave.core.store.statistics.field.StatsAccumulator;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.geowave.examples.stats.TrackDataModel.TrackPoint;

/**
 * Tracks twister statistics for track points within spatial bins using TDigest and
 * StatsAccumulator. Each spatial bin maintains separate twister statistics for track segments that
 * intersect that bin. Twister represents the rate of change in heading (angular velocity).
 */
public class TrackTwisterStatistic extends
    AbstractTrackFieldStatistic<TrackTwisterStatistic.TrackTwisterValue> {

  public static final FieldStatisticType<TrackTwisterValue> STATS_TYPE =
      new FieldStatisticType<>("TRACK_TWISTER");

  public TrackTwisterStatistic() {
    super(STATS_TYPE);
  }

  public TrackTwisterStatistic(final String typeName, final String fieldName) {
    super(STATS_TYPE, typeName, fieldName);
  }

  public TrackTwisterStatistic(
      final String typeName,
      final String fieldName,
      final SpatialBinningType spatialType,
      final int spatialPrecision) {
    super(STATS_TYPE, typeName, fieldName, spatialType, spatialPrecision);
  }

  @Override
  public TrackTwisterValue createEmpty() {
    return new TrackTwisterValue(this);
  }

  public static class TrackTwisterValue extends AbstractTrackFieldValue {

    public TrackTwisterValue() {
      this(null);
    }

    public TrackTwisterValue(final TrackTwisterStatistic statistic) {
      super(statistic);
    }

    @Override
    protected void calculateFieldValueInBin(
        final TrackPoint[] points,
        final LineString trackGeometry,
        final Geometry binGeometry) {

      if (!trackGeometry.intersects(binGeometry)) {
        return; // No intersection
      }

      // Find all track segments that intersect the bin and calculate their twister values
      for (int i = 0; i < points.length - 2; i++) { // Need 3 points to calculate twister
        final TrackPoint p1 = points[i];
        final TrackPoint p2 = points[i + 1];
        final TrackPoint p3 = points[i + 2];

        // Create line segment between consecutive points
        final LineString segment =
            GEOMETRY_FACTORY.createLineString(
                new Coordinate[] {
                    new Coordinate(p2.longitude, p2.latitude),
                    new Coordinate(p3.longitude, p3.latitude)});

        // Check if this segment intersects the bin
        if (binGeometry.intersects(segment)) {
          // Calculate the portion of the segment that's within the bin
          try {
            final Geometry intersection = binGeometry.intersection(segment);

            if (intersection instanceof LineString || intersection.getLength() > 0) {
              // Calculate twister for this segment (rate of heading change)
              final double twister = calculateSegmentTwister(p1, p2, p3);

              // Add to the unified statistics
              getFieldStatistics().add(twister);
            }
          } catch (final Exception e) {
            // If intersection calculation fails, fall back to simple segment calculation
            final double twister = calculateSegmentTwister(p1, p2, p3);

            // Add to the unified statistics
            getFieldStatistics().add(twister);
          }
        }
      }
    }

    /**
     * Calculate the twister (rate of heading change) for three consecutive track points.
     * 
     * @param p1 First point
     * @param p2 Middle point
     * @param p3 Last point
     * @return Twister in degrees per second
     */
    private double calculateSegmentTwister(
        final TrackPoint p1,
        final TrackPoint p2,
        final TrackPoint p3) {
      // Calculate heading from p1 to p2
      final double heading1 = calculateHeading(p1, p2);

      // Calculate heading from p2 to p3
      final double heading2 = calculateHeading(p2, p3);

      // Calculate heading change
      double headingChange = heading2 - heading1;

      // Normalize heading change to [-180, 180] degrees
      while (headingChange > 180) {
        headingChange -= 360;
      }
      while (headingChange < -180) {
        headingChange += 360;
      }

      // Calculate time difference
      final double timeDiff = (p3.timestamp - p1.timestamp) / 1000.0; // Convert to seconds

      // Calculate twister (degrees per second)
      if (timeDiff > 0) {
        return Math.abs(headingChange) / timeDiff;
      } else {
        return 0.0;
      }
    }

    /**
     * Calculate the heading (bearing) between two track points.
     * 
     * @param p1 Starting point
     * @param p2 Ending point
     * @return Heading in degrees (0-360)
     */
    private double calculateHeading(final TrackPoint p1, final TrackPoint p2) {
      final double lat1 = Math.toRadians(p1.latitude);
      final double lat2 = Math.toRadians(p2.latitude);
      final double deltaLon = Math.toRadians(p2.longitude - p1.longitude);

      final double y = Math.sin(deltaLon) * Math.cos(lat2);
      final double x =
          Math.cos(lat1) * Math.sin(lat2) - Math.sin(lat1) * Math.cos(lat2) * Math.cos(deltaLon);

      double heading = Math.toDegrees(Math.atan2(y, x));

      // Normalize to 0-360 degrees
      if (heading < 0) {
        heading += 360;
      }

      return heading;
    }

    @Override
    protected double extractPointFieldValue(final TrackPoint point) {
      return point.twister;
    }

    // Convenience methods for backward compatibility
    public TDigestNumericHistogram getTwisterDigest() {
      return getFieldDigest();
    }

    public StatsAccumulator getTwisterStats() {
      return getFieldStats();
    }

    public String getTwisterSummary() {
      return getFieldSummary();
    }
  }
}
