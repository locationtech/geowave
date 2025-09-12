package org.locationtech.geowave.examples.stats;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveDataType;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveField;
import org.locationtech.geowave.core.geotime.adapter.annotation.GeoWaveSpatialField;
import org.locationtech.geowave.core.geotime.adapter.annotation.GeoWaveTemporalField;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;

/**
 * Custom data model for track data that supports spatial binning statistics for speed, heading, and
 * acceleration. This model stores the entire track as a single entity with an array of track
 * points, enabling computation of movement statistics within spatial bins.
 *
 * <p>Key features: - Track-level metadata (ID, start/end times, overall geometry) - Array of
 * detailed track points with movement attributes - Support for spatial binning at track level -
 * Computed statistics for speed, acceleration, heading, and twister
 */
public class TrackDataModel {

  /**
   * Represents a single point within a track with GPS coordinates and computed movement attributes.
   */
  public static class TrackPoint {
    public final double latitude;
    public final double longitude;
    public final long timestamp; // Unix timestamp in milliseconds
    public final double speed; // km/h
    public final double acceleration; // m/s²
    public final double heading; // degrees (0-360)
    public final double twister; // heading change from previous point (degrees)
    public final double timeOfDay; // hours since midnight

    public TrackPoint(
        final double latitude,
        final double longitude,
        final long timestamp,
        final double speed,
        final double acceleration,
        final double heading,
        final double twister) {
      this.latitude = latitude;
      this.longitude = longitude;
      this.timestamp = timestamp;
      this.speed = speed;
      this.acceleration = acceleration;
      this.heading = heading;
      this.twister = twister;

      // Calculate time of day in hours since midnight
      this.timeOfDay = ((timestamp % (24 * 60 * 60 * 1000)) / (60.0 * 60.0 * 1000.0));
    }

    public Point getGeometry() {
      final GeometryFactory geometryFactory = new GeometryFactory();
      return geometryFactory.createPoint(new Coordinate(longitude, latitude));
    }
  }

  /**
   * Complete track data model with GeoWave annotations for proper indexing and spatial binning.
   * This POJO stores the entire track as a single entity, enabling statistics computation across
   * the full track context within spatial bins.
   */
  @GeoWaveDataType
  public static class Track {

    @GeoWaveField(name = "trackId")
    private final String trackId;

    @GeoWaveSpatialField(spatialIndexHint = true, crs = "EPSG:4326")
    private final LineString geometry; // Overall track geometry

    @GeoWaveTemporalField(timeIndexHint = true)
    private final Date startTime;

    @GeoWaveTemporalField
    private final Date endTime;

    @GeoWaveField(name = "duration")
    private final long duration; // Track duration in milliseconds

    @GeoWaveField(name = "totalDistance")
    private final double totalDistance; // Total track distance in meters

    @GeoWaveField(name = "averageSpeed")
    private final double averageSpeed; // Average speed across entire track

    @GeoWaveField(name = "maxSpeed")
    private final double maxSpeed; // Maximum speed in track

    @GeoWaveField(name = "averageAcceleration")
    private final double averageAcceleration; // Average acceleration

    @GeoWaveField(name = "maxAcceleration")
    private final double maxAcceleration; // Maximum acceleration

    @GeoWaveField(name = "totalTwister")
    private final double totalTwister; // Total heading changes

    @GeoWaveField(name = "pointCount")
    private final int pointCount; // Number of points in track

    // Array of track points - this is the key for enabling detailed statistics within spatial bins
    @GeoWaveField(name = "trackPoints")
    private final TrackPoint[] trackPoints;

    /**
     * No-args constructor required for GeoWave BasicDataTypeAdapter.
     */
    protected Track() {
      this.trackId = null;
      this.geometry = null;
      this.startTime = null;
      this.endTime = null;
      this.duration = 0;
      this.totalDistance = 0.0;
      this.averageSpeed = 0.0;
      this.maxSpeed = 0.0;
      this.averageAcceleration = 0.0;
      this.maxAcceleration = 0.0;
      this.totalTwister = 0.0;
      this.pointCount = 0;
      this.trackPoints = new TrackPoint[0];
    }

    /**
     * Constructor for creating a Track from a list of TrackPoints.
     */
    public Track(final String trackId, final List<TrackPoint> points) {
      this.trackId = trackId;
      this.trackPoints = points.toArray(new TrackPoint[0]);
      this.pointCount = points.size();

      if (points.isEmpty()) {
        this.geometry = null;
        this.startTime = null;
        this.endTime = null;
        this.duration = 0;
        this.totalDistance = 0.0;
        this.averageSpeed = 0.0;
        this.maxSpeed = 0.0;
        this.averageAcceleration = 0.0;
        this.maxAcceleration = 0.0;
        this.totalTwister = 0.0;
        return;
      }

      // Compute track-level statistics
      final TrackPoint firstPoint = points.get(0);
      final TrackPoint lastPoint = points.get(points.size() - 1);

      this.startTime = new Date(firstPoint.timestamp);
      this.endTime = new Date(lastPoint.timestamp);
      this.duration = lastPoint.timestamp - firstPoint.timestamp;

      // Create LineString geometry from all points
      final GeometryFactory geometryFactory = new GeometryFactory();
      final Coordinate[] coordinates = new Coordinate[points.size()];
      for (int i = 0; i < points.size(); i++) {
        final TrackPoint point = points.get(i);
        coordinates[i] = new Coordinate(point.longitude, point.latitude);
      }
      this.geometry = geometryFactory.createLineString(coordinates);

      // Compute aggregate statistics
      double totalDist = 0.0;
      double speedSum = 0.0;
      double maxSpd = 0.0;
      double accelSum = 0.0;
      double maxAccel = Double.NEGATIVE_INFINITY;
      double twisterSum = 0.0;

      for (final TrackPoint point : points) {
        speedSum += point.speed;
        maxSpd = Math.max(maxSpd, point.speed);
        accelSum += point.acceleration;
        maxAccel = Math.max(maxAccel, Math.abs(point.acceleration));
        twisterSum += point.twister;
      }

      // Calculate total distance (simplified - could use more sophisticated calculation)
      for (int i = 1; i < points.size(); i++) {
        final TrackPoint prev = points.get(i - 1);
        final TrackPoint curr = points.get(i);
        totalDist +=
            calculateDistance(prev.latitude, prev.longitude, curr.latitude, curr.longitude);
      }

      this.totalDistance = totalDist;
      this.averageSpeed = speedSum / points.size();
      this.maxSpeed = maxSpd;
      this.averageAcceleration = accelSum / points.size();
      this.maxAcceleration = maxAccel;
      this.totalTwister = twisterSum;
    }

    /**
     * Calculate distance between two points using Haversine formula.
     */
    private static double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
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

    // Getters for all fields
    public String getTrackId() {
      return trackId;
    }

    public LineString getGeometry() {
      return geometry;
    }

    public Date getStartTime() {
      return startTime;
    }

    public Date getEndTime() {
      return endTime;
    }

    public long getDuration() {
      return duration;
    }

    public double getTotalDistance() {
      return totalDistance;
    }

    public double getAverageSpeed() {
      return averageSpeed;
    }

    public double getMaxSpeed() {
      return maxSpeed;
    }

    public double getAverageAcceleration() {
      return averageAcceleration;
    }

    public double getMaxAcceleration() {
      return maxAcceleration;
    }

    public double getTotalTwister() {
      return totalTwister;
    }

    public int getPointCount() {
      return pointCount;
    }

    public TrackPoint[] getTrackPoints() {
      return trackPoints;
    }
  }

  /**
   * Helper method to create a track from raw coordinate and movement data.
   */
  public static Track createTrack(
      final String trackId,
      final double[][] points,
      final long startTimestamp,
      final int intervalSeconds) {

    final List<TrackPoint> trackPoints = new ArrayList<>();

    for (int i = 0; i < points.length; i++) {
      final double[] point = points[i];
      final double lon = point[0];
      final double lat = point[1];
      final double speed = point[2];
      final double acceleration = point[3];
      final double heading = point[4];

      // Calculate twister (heading change from previous point)
      double twister = 0.0;
      if (i > 0) {
        final double prevHeading = points[i - 1][4];
        twister = Math.abs(heading - prevHeading);
        // Handle wraparound (e.g., 359° to 1°)
        if (twister > 180) {
          twister = 360 - twister;
        }
      }

      final long timestamp = startTimestamp + (i * intervalSeconds * 1000L);
      trackPoints.add(new TrackPoint(lat, lon, timestamp, speed, acceleration, heading, twister));
    }

    return new Track(trackId, trackPoints);
  }
}
