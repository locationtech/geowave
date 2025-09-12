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
 * Tracks acceleration statistics for track points within spatial bins using TDigest and
 * StatsAccumulator. Each spatial bin maintains separate acceleration statistics for track segments
 * that intersect that bin.
 */
public class TrackAccelerationStatistic extends
    AbstractTrackFieldStatistic<TrackAccelerationStatistic.TrackAccelerationValue> {

  public static final FieldStatisticType<TrackAccelerationValue> STATS_TYPE =
      new FieldStatisticType<>("TRACK_ACCELERATION");

  public TrackAccelerationStatistic() {
    super(STATS_TYPE);
  }

  public TrackAccelerationStatistic(final String typeName, final String fieldName) {
    super(STATS_TYPE, typeName, fieldName);
  }

  public TrackAccelerationStatistic(
      final String typeName,
      final String fieldName,
      final SpatialBinningType spatialType,
      final int spatialPrecision) {
    super(STATS_TYPE, typeName, fieldName, spatialType, spatialPrecision);
  }

  @Override
  public TrackAccelerationValue createEmpty() {
    return new TrackAccelerationValue(this);
  }

  public static class TrackAccelerationValue extends AbstractTrackFieldValue {

    public TrackAccelerationValue() {
      this(null);
    }

    public TrackAccelerationValue(final TrackAccelerationStatistic statistic) {
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

      // Find all track segments that intersect the bin and calculate their accelerations
      double totalDistance = 0.0;
      double totalTime = 0.0;
      double totalAcceleration = 0.0;
      int segmentCount = 0;

      for (int i = 0; i < points.length - 1; i++) {
        final TrackPoint p1 = points[i];
        final TrackPoint p2 = points[i + 1];

        // Create line segment between consecutive points
        final LineString segment =
            GEOMETRY_FACTORY.createLineString(
                new Coordinate[] {
                    new Coordinate(p1.longitude, p1.latitude),
                    new Coordinate(p2.longitude, p2.latitude)});

        // Check if this segment intersects the bin
        if (binGeometry.intersects(segment)) {
          // Calculate the portion of the segment that's within the bin
          try {
            final Geometry intersection = binGeometry.intersection(segment);

            if (intersection instanceof LineString) {
              // Get the actual distance of the intersection
              final double segmentDistance = calculateGeometryLength((LineString) intersection);
              final double segmentTime = (p2.timestamp - p1.timestamp) / 1000.0; // Convert to
                                                                                 // seconds

              if (segmentTime > 0 && segmentDistance > 0) {
                // Calculate acceleration for this segment
                final double speedChange = p2.speed - p1.speed; // km/h
                final double speedChangeMs = speedChange / 3.6; // Convert to m/s
                final double acceleration = speedChangeMs / segmentTime; // m/s²

                totalDistance += segmentDistance;
                totalTime += segmentTime;
                totalAcceleration += acceleration;
                segmentCount++;
              }
            } else if (intersection.getLength() > 0) {
              // Handle other geometry types (MultiLineString, etc.)
              final double segmentDistance = intersection.getLength() * 111320; // Rough conversion
                                                                                // from degrees to
                                                                                // meters
              final double segmentTime = (p2.timestamp - p1.timestamp) / 1000.0;

              if (segmentTime > 0 && segmentDistance > 0) {
                final double speedChange = p2.speed - p1.speed; // km/h
                final double speedChangeMs = speedChange / 3.6; // Convert to m/s
                final double acceleration = speedChangeMs / segmentTime; // m/s²

                totalDistance += segmentDistance;
                totalTime += segmentTime;
                totalAcceleration += acceleration;
                segmentCount++;
              }
            }
          } catch (final Exception e) {
            // If intersection calculation fails, fall back to simple segment calculation
            final double segmentDistance =
                calculateDistance(p1.latitude, p1.longitude, p2.latitude, p2.longitude);
            final double segmentTime = (p2.timestamp - p1.timestamp) / 1000.0;

            if (segmentTime > 0 && segmentDistance > 0) {
              final double speedChange = p2.speed - p1.speed; // km/h
              final double speedChangeMs = speedChange / 3.6; // Convert to m/s
              final double acceleration = speedChangeMs / segmentTime; // m/s²

              totalDistance += segmentDistance;
              totalTime += segmentTime;
              totalAcceleration += acceleration;
              segmentCount++;
            }
          }
        }
      }

      // Calculate average acceleration for the track within this bin
      if (segmentCount > 0) {
        final double avgAcceleration = totalAcceleration / segmentCount; // m/s²

        // Add to the unified statistics
        getFieldStatistics().add(avgAcceleration);
      }
    }

    @Override
    protected double extractPointFieldValue(final TrackPoint point) {
      return point.acceleration;
    }



    // Convenience methods for backward compatibility
    public TDigestNumericHistogram getAccelerationDigest() {
      return getFieldDigest();
    }

    public StatsAccumulator getAccelerationStats() {
      return getFieldStats();
    }

    public String getAccelerationSummary() {
      return getFieldSummary();
    }
  }
}
