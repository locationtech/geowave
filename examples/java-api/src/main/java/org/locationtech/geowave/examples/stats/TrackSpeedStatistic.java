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
 * Tracks speed statistics for track points within spatial bins using TDigest and StatsAccumulator.
 * Each spatial bin maintains separate speed statistics for track segments that intersect that bin.
 */
public class TrackSpeedStatistic extends
    AbstractTrackFieldStatistic<TrackSpeedStatistic.TrackSpeedValue> {

  public static final FieldStatisticType<TrackSpeedValue> STATS_TYPE =
      new FieldStatisticType<>("TRACK_SPEED");

  public TrackSpeedStatistic() {
    super(STATS_TYPE);
  }

  public TrackSpeedStatistic(final String typeName, final String fieldName) {
    super(STATS_TYPE, typeName, fieldName);
  }

  public TrackSpeedStatistic(
      final String typeName,
      final String fieldName,
      final SpatialBinningType spatialType,
      final int spatialPrecision) {
    super(STATS_TYPE, typeName, fieldName, spatialType, spatialPrecision);
  }

  @Override
  public TrackSpeedValue createEmpty() {
    return new TrackSpeedValue(this);
  }

  public SpatialBinningType getSpatialType() {
    return spatialType;
  }

  public int getSpatialPrecision() {
    return spatialPrecision;
  }

  public static class TrackSpeedValue extends AbstractTrackFieldValue {

    public TrackSpeedValue() {
      this(null);
    }

    public TrackSpeedValue(final TrackSpeedStatistic statistic) {
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

      // Find all track segments that intersect the bin and calculate their speeds
      double totalDistance = 0.0;
      double totalTime = 0.0;

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
                totalDistance += segmentDistance;
                totalTime += segmentTime;
              }
            } else if (intersection.getLength() > 0) {
              // Handle other geometry types (MultiLineString, etc.)
              final double segmentDistance = intersection.getLength() * 111320; // Rough conversion
                                                                                // from degrees to
                                                                                // meters
              final double segmentTime = (p2.timestamp - p1.timestamp) / 1000.0;

              if (segmentTime > 0 && segmentDistance > 0) {
                totalDistance += segmentDistance;
                totalTime += segmentTime;
              }
            }
          } catch (final Exception e) {
            // If intersection calculation fails, fall back to simple segment calculation
            final double segmentDistance =
                calculateDistance(p1.latitude, p1.longitude, p2.latitude, p2.longitude);
            final double segmentTime = (p2.timestamp - p1.timestamp) / 1000.0;

            if (segmentTime > 0 && segmentDistance > 0) {
              totalDistance += segmentDistance;
              totalTime += segmentTime;
            }
          }
        }
      }

      // Calculate average speed for the track within this bin
      if (totalTime > 0 && totalDistance > 0) {
        final double speedMps = totalDistance / totalTime; // m/s
        final double speedKmh = speedMps * 3.6; // Convert to km/h

        // Add to the unified statistics
        getFieldStatistics().add(speedKmh);
      }
    }

    @Override
    protected double extractPointFieldValue(final TrackPoint point) {
      return point.speed;
    }

    @Override
    public String getFieldSummary() {
      final StatsAccumulator stats = getFieldStats();
      final TDigestNumericHistogram digest = getFieldDigest();

      if (stats.count() == 0) {
        return "No speed data";
      }

      final StringBuilder summary = new StringBuilder();
      summary.append("Speed statistics: ");
      summary.append("Count=").append(stats.count());

      // Use StatsAccumulator for basic statistics (more reliable than TDigest min/max)
      summary.append(", Min=").append(String.format("%.1f", stats.min())).append(" km/h");
      summary.append(", Max=").append(String.format("%.1f", stats.max())).append(" km/h");
      summary.append(", Mean=").append(String.format("%.1f", stats.mean())).append(" km/h");

      // Use TDigest for quantiles (if it has data)
      if (digest.getTotalCount() > 0) {
        summary.append(", Median=").append(String.format("%.1f", digest.quantile(0.5))).append(
            " km/h");
        summary.append(", P95=").append(String.format("%.1f", digest.quantile(0.95))).append(
            " km/h");
      }

      return summary.toString();
    }

    // Convenience methods for backward compatibility
    public TDigestNumericHistogram getSpeedDigest() {
      return getFieldDigest();
    }

    public StatsAccumulator getSpeedStats() {
      return getFieldStats();
    }

    public String getSpeedSummary() {
      return getFieldSummary();
    }
  }
}
