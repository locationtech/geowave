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
 * Tracks heading statistics for track points within spatial bins using TDigest and
 * StatsAccumulator. Each spatial bin maintains separate heading statistics for track segments that
 * intersect that bin.
 */
public class TrackHeadingStatistic extends
    AbstractTrackFieldStatistic<TrackHeadingStatistic.TrackHeadingValue> {

  public static final FieldStatisticType<TrackHeadingValue> STATS_TYPE =
      new FieldStatisticType<>("TRACK_HEADING");

  public TrackHeadingStatistic() {
    super(STATS_TYPE);
  }

  public TrackHeadingStatistic(final String typeName, final String fieldName) {
    super(STATS_TYPE, typeName, fieldName);
  }

  public TrackHeadingStatistic(
      final String typeName,
      final String fieldName,
      final SpatialBinningType spatialType,
      final int spatialPrecision) {
    super(STATS_TYPE, typeName, fieldName, spatialType, spatialPrecision);
  }

  @Override
  public TrackHeadingValue createEmpty() {
    return new TrackHeadingValue(this);
  }

  public static class TrackHeadingValue extends AbstractTrackFieldValue {

    public TrackHeadingValue() {
      this(null);
    }

    public TrackHeadingValue(final TrackHeadingStatistic statistic) {
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

      // Find all track segments that intersect the bin and calculate their headings
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

            if (intersection instanceof LineString || intersection.getLength() > 0) {
              // Calculate heading for this segment
              final double heading = calculateSegmentHeading(p1, p2);

              // Add to the unified statistics
              getFieldStatistics().add(heading);
            }
          } catch (final Exception e) {
            // If intersection calculation fails, fall back to simple segment calculation
            final double heading = calculateSegmentHeading(p1, p2);

            // Add to the unified statistics
            getFieldStatistics().add(heading);
          }
        }
      }
    }

    /**
     * Calculate the heading (bearing) between two track points.
     * 
     * @param p1 Starting point
     * @param p2 Ending point
     * @return Heading in degrees (0-360)
     */
    private double calculateSegmentHeading(final TrackPoint p1, final TrackPoint p2) {
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
      return point.heading;
    }



    // Convenience methods for backward compatibility
    public TDigestNumericHistogram getHeadingDigest() {
      return getFieldDigest();
    }

    public StatsAccumulator getHeadingStats() {
      return getFieldStats();
    }

    public String getHeadingSummary() {
      return getFieldSummary();
    }
  }
}
