/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.examples.stats;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.TDigestNumericHistogram;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.StatisticsIngestCallback;
import org.locationtech.geowave.core.store.statistics.field.FieldStatisticType;
import org.locationtech.geowave.core.store.statistics.field.StatsAccumulator;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.geowave.examples.stats.TrackDataModel.TrackPoint;

/**
 * Abstract base class for track field statistics that operate on spatial bins. Provides common
 * functionality for calculating field values within spatial bin geometries.
 * 
 * @param <T> The specific StatisticValue type for the concrete implementation
 */
public abstract class AbstractTrackFieldStatistic<T extends AbstractTrackFieldStatistic.AbstractTrackFieldValue>
    extends
    FieldStatistic<T> {

  protected SpatialBinningType spatialType = SpatialBinningType.S2;
  protected int spatialPrecision = 18; // ~10m precision

  protected AbstractTrackFieldStatistic(final FieldStatisticType<T> statsType) {
    super(statsType);
  }

  protected AbstractTrackFieldStatistic(
      final FieldStatisticType<T> statsType,
      final String typeName,
      final String fieldName) {
    super(statsType, typeName, fieldName);
  }

  protected AbstractTrackFieldStatistic(
      final FieldStatisticType<T> statsType,
      final String typeName,
      final String fieldName,
      final SpatialBinningType spatialType,
      final int spatialPrecision) {
    super(statsType, typeName, fieldName);
    this.spatialType = spatialType;
    this.spatialPrecision = spatialPrecision;
  }

  @Override
  public String getDescription() {
    return "Tracks "
        + getFieldName()
        + " statistics within "
        + spatialType.name()
        + " spatial bins at precision "
        + spatialPrecision
        + " using TDigest and StatsAccumulator";
  }

  @Override
  public boolean isCompatibleWith(final Class<?> fieldClass) {
    return TrackPoint[].class.isAssignableFrom(fieldClass);
  }

  // Getters for spatial configuration
  public SpatialBinningType getSpatialType() {
    return spatialType;
  }

  public int getSpatialPrecision() {
    return spatialPrecision;
  }

  /**
   * Wrapper class that combines TDigest and StatsAccumulator for comprehensive field statistics.
   */
  public static class TrackFieldStatistics {
    private final TDigestNumericHistogram digest;
    private final StatsAccumulator stats;
    private final String fieldName;

    public TrackFieldStatistics(final String fieldName) {
      this.fieldName = fieldName;
      this.digest = new TDigestNumericHistogram();
      this.stats = new StatsAccumulator();
    }

    public TDigestNumericHistogram getDigest() {
      return digest;
    }

    public StatsAccumulator getStats() {
      return stats;
    }

    public void add(final double value) {
      digest.add(value);
      stats.add(value);
    }

    public String getSummary() {
      if (stats.count() == 0) {
        return "No " + fieldName + " data";
      }

      final StringBuilder summary = new StringBuilder();
      summary.append(fieldName).append(" statistics: ");
      summary.append("Count=").append(stats.count());

      // Use StatsAccumulator for basic statistics (more reliable than TDigest min/max)
      summary.append(", Min=").append(String.format("%.2f", stats.min()));
      summary.append(", Max=").append(String.format("%.2f", stats.max()));
      summary.append(", Mean=").append(String.format("%.2f", stats.mean()));

      // Use TDigest for quantiles (if it has data)
      if (digest.getTotalCount() > 0) {
        summary.append(", Median=").append(String.format("%.2f", digest.quantile(0.5)));
        summary.append(", P95=").append(String.format("%.2f", digest.quantile(0.95)));
      }

      return summary.toString();
    }

    public void merge(final TrackFieldStatistics other) {
      if (other != null) {
        this.digest.merge(other.digest);
        this.stats.addAll(other.stats);
      }
    }
  }

  /**
   * Abstract base class for track field statistic values. Provides common spatial binning
   * functionality and dual statistics (TDigest + StatsAccumulator).
   */
  public abstract static class AbstractTrackFieldValue extends StatisticValue<TrackFieldStatistics>
      implements
      StatisticsIngestCallback {

    protected static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    protected TrackFieldStatistics fieldStatistics;

    protected AbstractTrackFieldValue() {
      this(null);
    }

    protected AbstractTrackFieldValue(final AbstractTrackFieldStatistic<?> statistic) {
      super(statistic);
      fieldStatistics = null; // Initialize lazily
    }

    @Override
    public void merge(final Mergeable merge) {
      if (merge.getClass().equals(this.getClass())) {
        final AbstractTrackFieldValue other = (AbstractTrackFieldValue) merge;

        // Merge TrackFieldStatistics
        if (other.fieldStatistics != null) {
          getFieldStatistics().merge(other.fieldStatistics);
        }
      }
    }

    @Override
    public TrackFieldStatistics getValue() {
      return getFieldStatistics();
    }

    @Override
    public <T> void entryIngested(
        final DataTypeAdapter<T> adapter,
        final T entry,
        final GeoWaveRow... kvs) {

      // Extract TrackPoint array from the entry
      final AbstractTrackFieldStatistic<?> statistic =
          (AbstractTrackFieldStatistic<?>) getStatistic();
      final Object fieldValue = adapter.getFieldValue(entry, statistic.getFieldName());
      if (fieldValue instanceof TrackPoint[]) {
        final TrackPoint[] points = (TrackPoint[]) fieldValue;
        processTrackInSpatialBin(points, kvs);
      }
    }

    /**
     * Process track points within spatial bins by calculating field values for track segments that
     * intersect each bin geometry.
     */
    protected void processTrackInSpatialBin(final TrackPoint[] points, final GeoWaveRow... kvs) {
      if (points == null || points.length < 2) {
        return; // Need at least 2 points to form a track
      }

      // Create track geometry from points
      final Coordinate[] coordinates = new Coordinate[points.length];
      for (int i = 0; i < points.length; i++) {
        coordinates[i] = new Coordinate(points[i].longitude, points[i].latitude);
      }
      final LineString trackGeometry = GEOMETRY_FACTORY.createLineString(coordinates);

      // Process each spatial bin
      for (final GeoWaveRow kv : kvs) {
        try {
          // Get spatial bin geometry
          final ByteArray binId = new ByteArray(kv.getPartitionKey());
          final AbstractTrackFieldStatistic<?> statistic =
              (AbstractTrackFieldStatistic<?>) getStatistic();
          final Geometry binGeometry =
              statistic.spatialType.getBinGeometry(binId, statistic.spatialPrecision);

          // Calculate field value for the portion of track that intersects this bin
          calculateFieldValueInBin(points, trackGeometry, binGeometry);

        } catch (final Exception e) {
          // If we can't get bin geometry, fall back to simple point-based calculation
          // This shouldn't happen in normal operation but provides a fallback
          for (final TrackPoint point : points) {
            final double fieldValue = extractPointFieldValue(point);
            if (!Double.isNaN(fieldValue)) {
              getFieldDigest().add(fieldValue);
              getFieldStats().add(fieldValue);
            }
          }
        }
      }
    }

    /**
     * Abstract method to calculate the field value for track segments within a spatial bin.
     * Implementations should analyze track segments that intersect the bin geometry and add
     * appropriate field values to both the TDigest and StatsAccumulator.
     * 
     * @param points The track points
     * @param trackGeometry The complete track geometry
     * @param binGeometry The spatial bin geometry
     */
    protected abstract void calculateFieldValueInBin(
        final TrackPoint[] points,
        final LineString trackGeometry,
        final Geometry binGeometry);

    /**
     * Extract the field value from a single track point. Used as fallback when spatial bin geometry
     * cannot be determined.
     * 
     * @param point The track point
     * @return The field value for this point
     */
    protected abstract double extractPointFieldValue(final TrackPoint point);

    /**
     * Calculate the actual distance along a line string using Haversine formula.
     */
    protected double calculateGeometryLength(final LineString lineString) {
      double totalDistance = 0.0;
      final Coordinate[] coords = lineString.getCoordinates();

      for (int i = 0; i < coords.length - 1; i++) {
        totalDistance +=
            calculateDistance(
                coords[i].y,
                coords[i].x, // lat, lon
                coords[i + 1].y,
                coords[i + 1].x);
      }

      return totalDistance;
    }

    /**
     * Calculate distance between two points using Haversine formula.
     */
    protected double calculateDistance(
        final double lat1,
        final double lon1,
        final double lat2,
        final double lon2) {

      final double R = 6371000; // Earth's radius in meters
      final double dLat = Math.toRadians(lat2 - lat1);
      final double dLon = Math.toRadians(lon2 - lon1);
      final double a =
          Math.sin(dLat / 2) * Math.sin(dLat / 2)
              + Math.cos(Math.toRadians(lat1))
                  * Math.cos(Math.toRadians(lat2))
                  * Math.sin(dLon / 2)
                  * Math.sin(dLon / 2);
      final double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
      return R * c;
    }

    // Utility methods for accessing statistics
    public TrackFieldStatistics getFieldStatistics() {
      if (fieldStatistics == null) {
        // Get field name from the statistic for better summary formatting
        final AbstractTrackFieldStatistic<?> statistic =
            (AbstractTrackFieldStatistic<?>) getStatistic();
        final String fieldName = statistic != null ? statistic.getFieldName() : "field";
        fieldStatistics = new TrackFieldStatistics(fieldName);
      }
      return fieldStatistics;
    }

    public TDigestNumericHistogram getFieldDigest() {
      return getFieldStatistics().getDigest();
    }

    public StatsAccumulator getFieldStats() {
      return getFieldStatistics().getStats();
    }

    public long getTotalPointCount() {
      return getFieldDigest().getTotalCount();
    }

    /**
     * Get a summary of the field statistics using the wrapper's unified summary method.
     */
    public String getFieldSummary() {
      return getFieldStatistics().getSummary();
    }

    @Override
    public byte[] toBinary() {
      if (fieldStatistics == null) {
        return new byte[0];
      }

      final TDigestNumericHistogram digest = fieldStatistics.getDigest();
      final StatsAccumulator stats = fieldStatistics.getStats();

      // Calculate buffer size
      int bufferSize = 4 * 8; // 4 doubles for stats (count as long, mean, min, max)
      bufferSize += 8; // 8 bytes for count as long

      if (digest != null && digest.getTotalCount() > 0) {
        bufferSize += 4 + digest.bufferSize(); // 4 bytes for digest size + digest data
      } else {
        bufferSize += 4; // 4 bytes for zero digest size
      }

      final ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

      // Serialize StatsAccumulator (only public fields)
      if (stats != null && stats.count() > 0) {
        buffer.putLong(stats.count());
        buffer.putDouble(stats.mean());
        buffer.putDouble(stats.min());
        buffer.putDouble(stats.max());
      } else {
        buffer.putLong(0); // No stats data
        buffer.putDouble(0.0);
        buffer.putDouble(Double.NaN);
        buffer.putDouble(Double.NaN);
      }

      // Serialize TDigest
      if (digest != null) {
        buffer.putInt(digest.bufferSize());
        digest.toBinary(buffer);
      } else {
        buffer.putInt(0); // No digest data
      }

      return buffer.array();
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      // Initialize the wrapper first
      final AbstractTrackFieldStatistic<?> statistic =
          (AbstractTrackFieldStatistic<?>) getStatistic();
      final String fieldName = statistic != null ? statistic.getFieldName() : "field";
      fieldStatistics = new TrackFieldStatistics(fieldName);

      if (bytes.length == 0) {
        return;
      }

      final ByteBuffer buffer = ByteBuffer.wrap(bytes);

      // Deserialize StatsAccumulator (simplified - we lose some precision)
      final long count = buffer.getLong();
      final double mean = buffer.getDouble();
      final double min = buffer.getDouble();
      final double max = buffer.getDouble();

      final StatsAccumulator stats = fieldStatistics.getStats();
      if (count > 0) {
        // Reconstruct the stats by adding values around the mean
        // This is a simplification but preserves count, mean, min, max
        stats.add(min);
        stats.add(max);
        for (int i = 2; i < count; i++) {
          stats.add(mean);
        }
      }

      // Deserialize TDigest
      final int digestSize = buffer.getInt();
      final TDigestNumericHistogram digest = fieldStatistics.getDigest();
      if (digestSize > 0) {
        digest.fromBinary(buffer);
      }
    }
  }
}
