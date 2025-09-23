/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.examples.stats;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.binning.ComplexGeometryBinningOption;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.statistics.binning.SpatialFieldValueBinningStrategy;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericHistogramStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericStatsStatistic;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.NumericHistogram;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.geotools.feature.simple.SimpleFeatureBuilder;

/**
 * This example demonstrates spatial binning statistics for track data using individual track points
 * with latitude, longitude, timestamp, speed, acceleration, and heading. Statistics are computed
 * and stored without ingesting the actual track data.
 *
 * Key features demonstrated: - TDigest statistics for efficient quantile estimation of track
 * attributes - Spatial binning with ~10 meter precision using S2 level 18 - Multiple tracks
 * intersecting each spatial bin for realistic scenarios - Bounding box queries using
 * BinConstraints.ofObject(bbox) - Statistics-only storage (no actual track data ingestion) -
 * Derived twister calculation from consecutive heading changes
 *
 * Track point data includes: - Latitude/Longitude: GPS coordinates - Timestamp: When the point was
 * recorded - Speed: Instantaneous speed at the point - Acceleration: Instantaneous acceleration at
 * the point - Heading: Direction of travel at the point - Twister: Calculated heading change from
 * previous point - Time of Day: Derived from timestamp (hours since midnight)
 */
public class TrackSpatialBinningStatisticExample {

  /**
   * Represents a single track point with GPS coordinates and movement attributes.
   */
  public static class TrackPoint {
    public final String trackId;
    public final double latitude;
    public final double longitude;
    public final long timestamp; // Unix timestamp in milliseconds
    public final double speed; // km/h
    public final double acceleration; // m/s²
    public final double heading; // degrees (0-360)
    public final double twister; // heading change from previous point (degrees)
    public final double timeOfDay; // hours since midnight

    public TrackPoint(
        String trackId,
        double latitude,
        double longitude,
        long timestamp,
        double speed,
        double acceleration,
        double heading,
        double twister) {
      this.trackId = trackId;
      this.latitude = latitude;
      this.longitude = longitude;
      this.timestamp = timestamp;
      this.speed = speed;
      this.acceleration = acceleration;
      this.heading = heading;
      this.twister = twister;

      // Calculate time of day from timestamp
      final java.time.Instant instant = java.time.Instant.ofEpochMilli(timestamp);
      final java.time.LocalTime localTime = instant.atZone(java.time.ZoneOffset.UTC).toLocalTime();
      this.timeOfDay = localTime.toSecondOfDay() / 3600.0;
    }
  }

  /**
   * Run this example to demonstrate track data spatial binning statistics.
   *
   * To run from command line: mvn exec:java
   * -Dexec.mainClass="org.locationtech.geowave.examples.stats.TrackSpatialBinningStatisticExample"
   *
   * Or compile and run directly: javac -cp "target/classes:target/dependency/*"
   * src/main/java/org/locationtech/geowave/examples/stats/TrackSpatialBinningStatisticExample.java
   * java -cp "target/classes:target/dependency/*"
   * org.locationtech.geowave.examples.stats.TrackSpatialBinningStatisticExample
   */
  public static void main(final String[] args) {
    // Create sample track point data (individual GPS points with attributes)
    final List<TrackPoint> trackPoints = createSampleTrackPoints();

    // Create data store and spatial index
    final DataStore dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());
    final Index index =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());

    // Create feature type and adapter for track point data
    final SimpleFeatureType featureType = getTrackPointFeatureType();
    final FeatureDataAdapter adapter = new FeatureDataAdapter(featureType);

    // Register the adapter and index
    dataStore.addType(adapter, index);

    // Create spatial binning strategy for ~10 meter precision
    // S2 level 18 provides approximately 10-meter resolution
    // Alternative precisions: S2 level 16 (~40m), level 17 (~20m), level 19 (~5m), level 20 (~2.5m)
    // For H3: resolution 10 (~15m), resolution 11 (~7m), resolution 12 (~3m)
    // For GeoHash: precision 7 (~76m), precision 8 (~19m), precision 9 (~4.8m)
    final SpatialFieldValueBinningStrategy spatialBinning =
        new SpatialFieldValueBinningStrategy(featureType.getGeometryDescriptor().getLocalName());
    spatialBinning.setType(SpatialBinningType.S2);
    spatialBinning.setPrecision(18); // ~10 meter resolution
    // Use weighted binning: scale contributions based on geometry overlap percentage
    // This prevents double-counting when tracks intersect multiple spatial bins
    spatialBinning.setComplexGeometry(
        ComplexGeometryBinningOption.USE_FULL_GEOMETRY_SCALE_BY_OVERLAP);

    // Track count statistic
    final CountStatistic trackCount = new CountStatistic(featureType.getTypeName());
    trackCount.setTag("Track-Count");
    trackCount.setBinningStrategy(spatialBinning);

    // TDigest statistics for various track attributes
    final NumericHistogramStatistic speedTDigest =
        new NumericHistogramStatistic(featureType.getTypeName(), "speed", 100.0);
    speedTDigest.setTag("Speed-TDigest");
    speedTDigest.setBinningStrategy(spatialBinning);

    final NumericHistogramStatistic accelerationTDigest =
        new NumericHistogramStatistic(featureType.getTypeName(), "acceleration", 100.0);
    accelerationTDigest.setTag("Acceleration-TDigest");
    accelerationTDigest.setBinningStrategy(spatialBinning);

    final NumericHistogramStatistic headingTDigest =
        new NumericHistogramStatistic(featureType.getTypeName(), "heading", 100.0);
    headingTDigest.setTag("Heading-TDigest");
    headingTDigest.setBinningStrategy(spatialBinning);

    final NumericHistogramStatistic twisterTDigest =
        new NumericHistogramStatistic(featureType.getTypeName(), "twister", 100.0);
    twisterTDigest.setTag("Twister-TDigest");
    twisterTDigest.setBinningStrategy(spatialBinning);

    final NumericHistogramStatistic timeOfDayTDigest =
        new NumericHistogramStatistic(featureType.getTypeName(), "timeOfDay", 100.0);
    timeOfDayTDigest.setTag("TimeOfDay-TDigest");
    timeOfDayTDigest.setBinningStrategy(spatialBinning);

    // Sum statistics for acceleration and speed
    final NumericStatsStatistic accelerationSum =
        new NumericStatsStatistic(featureType.getTypeName(), "acceleration");
    accelerationSum.setTag("Acceleration-Stats");
    accelerationSum.setBinningStrategy(spatialBinning);

    final NumericStatsStatistic speedSum =
        new NumericStatsStatistic(featureType.getTypeName(), "speed");
    speedSum.setTag("Speed-Stats");
    speedSum.setBinningStrategy(spatialBinning);



    // Add all statistics to the data store
    dataStore.addEmptyStatistic(
        trackCount,
        speedTDigest,
        accelerationTDigest,
        headingTDigest,
        twisterTDigest,
        timeOfDayTDigest,
        accelerationSum,
        speedSum);

    // Convert track points to SimpleFeatures and ingest them
    final List<SimpleFeature> trackFeatures =
        convertTrackPointsToFeatures(trackPoints, featureType);

    // Ingest track point data
    try (Writer<SimpleFeature> indexWriter = dataStore.createWriter(adapter.getTypeName())) {
      for (final SimpleFeature feature : trackFeatures) {
        indexWriter.write(feature);
      }
    }

    // Display results
    displayTrackStatistics(
        dataStore,
        trackCount,
        speedTDigest,
        accelerationTDigest,
        headingTDigest,
        twisterTDigest,
        timeOfDayTDigest,
        accelerationSum,
        speedSum,
        spatialBinning);

    // Show precision options for reference
    demonstratePrecisionOptions();
  }

  /**
   * Creates the SimpleFeatureType for track point data with all required attributes. This is used
   * only for statistics registration, not for actual data storage.
   */
  private static SimpleFeatureType getTrackPointFeatureType() {
    final String name = "TrackPointData";
    final SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
    final AttributeTypeBuilder atBuilder = new AttributeTypeBuilder();

    sftBuilder.setName(name);
    sftBuilder.add(atBuilder.binding(String.class).nillable(false).buildDescriptor("trackId"));
    sftBuilder.add(atBuilder.binding(Geometry.class).nillable(false).buildDescriptor("geometry"));
    sftBuilder.add(atBuilder.binding(Double.class).nillable(false).buildDescriptor("speed"));
    sftBuilder.add(atBuilder.binding(Double.class).nillable(false).buildDescriptor("acceleration"));
    sftBuilder.add(atBuilder.binding(Double.class).nillable(false).buildDescriptor("heading"));
    sftBuilder.add(atBuilder.binding(Double.class).nillable(false).buildDescriptor("twister"));
    sftBuilder.add(atBuilder.binding(Double.class).nillable(false).buildDescriptor("timeOfDay"));

    return sftBuilder.buildFeatureType();
  }

  /**
   * Creates sample track point data with multiple tracks intersecting each spatial bin. Each track
   * point represents an individual GPS measurement with movement attributes.
   */
  private static List<TrackPoint> createSampleTrackPoints() {
    final List<TrackPoint> trackPoints = new ArrayList<>();
    long baseTimestamp = System.currentTimeMillis() - (24 * 60 * 60 * 1000); // 24 hours ago

    // Create dense track points in Washington DC area to ensure multiple tracks per spatial bin
    // Track 1: Urban commute route with multiple points in same area
    trackPoints.addAll(
        createTrackSequence(
            "track1",
            new double[][] {
                {-77.0352, 38.8895, 25.5, 2.1, 45.0}, // White House area
                {-77.0354, 38.8897, 28.2, 1.8, 47.0}, // Very close to previous point
                {-77.0356, 38.8899, 22.1, -0.5, 49.0}, // Same spatial bin
                {-77.0366, 38.8977, 35.0, 3.2, 52.0},
                {-77.0368, 38.8979, 31.5, -1.1, 48.0}, // Overlapping area
                {-77.0380, 38.9058, 18.7, -2.8, 45.0}},
            baseTimestamp,
            30)); // 30 second intervals

    // Track 2: Delivery route overlapping with track 1
    trackPoints.addAll(
        createTrackSequence(
            "track2",
            new double[][] {
                {-77.0350, 38.8893, 15.3, 1.2, 90.0}, // Same area as track 1
                {-77.0352, 38.8895, 18.7, 2.1, 95.0}, // Exact same location as track 1 point
                {-77.0355, 38.8898, 22.1, 1.8, 88.0}, // Very close overlap
                {-77.0365, 38.8975, 25.4, 0.9, 85.0},
                {-77.0367, 38.8977, 19.8, -1.5, 82.0}, // Overlapping with track 1
                {-77.0378, 38.9055, 16.2, -0.8, 80.0}},
            baseTimestamp + 300,
            45)); // 5 minutes later, 45 second intervals

    // Track 3: Highway route with some overlap
    trackPoints.addAll(
        createTrackSequence(
            "track3",
            new double[][] {
                {-76.8644, 38.9078, 65.8, -1.5, 90.0},
                {-76.8500, 38.9150, 72.3, 2.1, 88.0},
                {-76.8356, 38.9222, 68.9, -0.8, 85.0},
                {-76.8212, 38.9294, 75.2, 1.9, 87.0}},
            baseTimestamp + 600,
            60)); // 10 minutes later, 60 second intervals

    // Track 4: Another urban route with heavy overlap in same bins
    trackPoints.addAll(
        createTrackSequence(
            "track4",
            new double[][] {
                {-77.0348, 38.8891, 32.1, 2.8, 135.0}, // Very close to track 1 & 2 points
                {-77.0351, 38.8894, 29.5, -0.7, 138.0}, // Same spatial bin as others
                {-77.0353, 38.8896, 35.2, 1.9, 142.0}, // Heavy overlap
                {-77.0362, 38.8973, 28.7, -1.2, 140.0},
                {-77.0364, 38.8975, 31.8, 0.8, 145.0}, // Overlapping area
                {-77.0376, 38.9052, 26.3, -1.5, 143.0}},
            baseTimestamp + 900,
            40)); // 15 minutes later, 40 second intervals

    // Track 5: Racing circuit with high speeds
    trackPoints.addAll(
        createTrackSequence(
            "track5",
            new double[][] {
                {-76.9000, 38.9500, 85.7, 5.8, 270.0},
                {-76.9050, 38.9550, 92.1, 3.2, 275.0},
                {-76.9100, 38.9600, 88.5, -1.8, 278.0},
                {-76.9150, 38.9650, 95.3, 2.7, 280.0}},
            baseTimestamp + 1200,
            20)); // 20 minutes later, 20 second intervals

    // Track 6: More overlap in the dense urban area
    trackPoints.addAll(
        createTrackSequence(
            "track6",
            new double[][] {
                {-77.0349, 38.8892, 41.2, 3.5, 180.0}, // Same spatial bins as tracks 1,2,4
                {-77.0352, 38.8895, 38.7, -1.1, 185.0}, // Exact overlap with multiple tracks
                {-77.0354, 38.8897, 44.1, 2.3, 182.0}, // Dense overlap
                {-77.0363, 38.8974, 36.8, -0.9, 178.0},
                {-77.0365, 38.8976, 42.5, 1.7, 180.0}, // More overlap
                {-77.0377, 38.9053, 39.2, -1.4, 175.0}},
            baseTimestamp + 1500,
            35)); // 25 minutes later, 35 second intervals

    return trackPoints;
  }

  /**
   * Helper method to create a sequence of track points for a single track.
   */
  private static List<TrackPoint> createTrackSequence(
      String trackId,
      double[][] points,
      long startTimestamp,
      int intervalSeconds) {
    final List<TrackPoint> trackPoints = new ArrayList<>();

    for (int i = 0; i < points.length; i++) {
      double[] point = points[i];
      double lon = point[0];
      double lat = point[1];
      double speed = point[2];
      double acceleration = point[3];
      double heading = point[4];

      // Calculate twister (heading change from previous point)
      double twister = 0.0;
      if (i > 0) {
        double prevHeading = points[i - 1][4];
        twister = Math.abs(heading - prevHeading);
        // Handle wraparound (e.g., 359° to 1°)
        if (twister > 180) {
          twister = 360 - twister;
        }
      }

      long timestamp = startTimestamp + (((long) i) * intervalSeconds * 1000L);
      trackPoints.add(
          new TrackPoint(trackId, lat, lon, timestamp, speed, acceleration, heading, twister));
    }

    return trackPoints;
  }

  /**
   * Converts track points to SimpleFeatures for ingestion.
   */
  private static List<SimpleFeature> convertTrackPointsToFeatures(
      List<TrackPoint> trackPoints,
      SimpleFeatureType featureType) {

    final List<SimpleFeature> features = new ArrayList<>();
    final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(featureType);
    final org.locationtech.jts.geom.GeometryFactory geometryFactory =
        new org.locationtech.jts.geom.GeometryFactory();

    System.out.println("Converting " + trackPoints.size() + " track points to features...");

    for (int i = 0; i < trackPoints.size(); i++) {
      TrackPoint point = trackPoints.get(i);

      // Create point geometry
      final org.locationtech.jts.geom.Point pointGeom =
          geometryFactory.createPoint(new Coordinate(point.longitude, point.latitude));

      // Build feature
      builder.set("trackId", point.trackId);
      builder.set("geometry", pointGeom);
      builder.set("speed", point.speed);
      builder.set("acceleration", point.acceleration);
      builder.set("heading", point.heading);
      builder.set("twister", point.twister);
      builder.set("timeOfDay", point.timeOfDay);

      final SimpleFeature feature = builder.buildFeature(point.trackId + "_" + i);
      features.add(feature);
    }

    System.out.println("Created " + features.size() + " features for ingestion");
    return features;
  }

  /**
   * Display the track statistics results.
   */
  private static void displayTrackStatistics(
      final DataStore dataStore,
      final CountStatistic trackCount,
      final NumericHistogramStatistic speedTDigest,
      final NumericHistogramStatistic accelerationTDigest,
      final NumericHistogramStatistic headingTDigest,
      final NumericHistogramStatistic twisterTDigest,
      final NumericHistogramStatistic timeOfDayTDigest,
      final NumericStatsStatistic accelerationSum,
      final NumericStatsStatistic speedSum,
      final SpatialFieldValueBinningStrategy spatialBinning) {

    System.out.println("***** Track Spatial Binning Statistics *****");
    System.out.println("Using S2 Level 18 (~10 meter resolution) spatial binning");
    System.out.println(
        "Using weighted binning: track contributions are scaled by geometry overlap percentage");
    System.out.println("This prevents double-counting when tracks intersect multiple spatial bins");

    System.out.println("\n** Track Count by Spatial Bin **");
    try (CloseableIterator<Pair<ByteArray, Long>> it =
        dataStore.getBinnedStatisticValues(trackCount)) {
      while (it.hasNext()) {
        final Pair<ByteArray, Long> pair = it.next();
        System.out.println(
            String.format(
                "Track Count: %d, Bin: %s, Bin Geometry: %s",
                pair.getRight(),
                spatialBinning.binToString(pair.getLeft()),
                spatialBinning.getType().getBinGeometry(pair.getLeft(), 18)));
      }
    }

    System.out.println("\n** Speed TDigest by Spatial Bin **");
    try (CloseableIterator<Pair<ByteArray, NumericHistogram>> it =
        dataStore.getBinnedStatisticValues(speedTDigest)) {
      while (it.hasNext()) {
        final Pair<ByteArray, NumericHistogram> pair = it.next();
        final NumericHistogram histogram = pair.getRight();
        System.out.println(
            String.format(
                "Speed TDigest - Bin: %s, Count: %d, Min: %.2f, Max: %.2f, Median: %.2f, 95th Percentile: %.2f",
                spatialBinning.binToString(pair.getLeft()),
                histogram.getTotalCount(),
                histogram.getMinValue(),
                histogram.getMaxValue(),
                histogram.quantile(0.5),
                histogram.quantile(0.95)));
      }
    }

    System.out.println("\n** Heading TDigest by Spatial Bin **");
    try (CloseableIterator<Pair<ByteArray, NumericHistogram>> it =
        dataStore.getBinnedStatisticValues(headingTDigest)) {
      while (it.hasNext()) {
        final Pair<ByteArray, NumericHistogram> pair = it.next();
        final NumericHistogram histogram = pair.getRight();
        System.out.println(
            String.format(
                "Heading TDigest - Bin: %s, Count: %d, Min: %.1f°, Max: %.1f°, Median: %.1f°",
                spatialBinning.binToString(pair.getLeft()),
                histogram.getTotalCount(),
                histogram.getMinValue(),
                histogram.getMaxValue(),
                histogram.quantile(0.5)));
      }
    }

    System.out.println("\n** Twister TDigest by Spatial Bin **");
    try (CloseableIterator<Pair<ByteArray, NumericHistogram>> it =
        dataStore.getBinnedStatisticValues(twisterTDigest)) {
      while (it.hasNext()) {
        final Pair<ByteArray, NumericHistogram> pair = it.next();
        final NumericHistogram histogram = pair.getRight();
        System.out.println(
            String.format(
                "Twister TDigest - Bin: %s, Count: %d, Min: %.1f°, Max: %.1f°, Mean: %.1f°",
                spatialBinning.binToString(pair.getLeft()),
                histogram.getTotalCount(),
                histogram.getMinValue(),
                histogram.getMaxValue(),
                (histogram.getMinValue() + histogram.getMaxValue()) / 2.0));
      }
    }

    System.out.println("\n** Time of Day TDigest by Spatial Bin **");
    try (CloseableIterator<Pair<ByteArray, NumericHistogram>> it =
        dataStore.getBinnedStatisticValues(timeOfDayTDigest)) {
      while (it.hasNext()) {
        final Pair<ByteArray, NumericHistogram> pair = it.next();
        final NumericHistogram histogram = pair.getRight();
        System.out.println(
            String.format(
                "Time of Day TDigest - Bin: %s, Count: %d, Min: %.1fh, Max: %.1fh, Median: %.1fh",
                spatialBinning.binToString(pair.getLeft()),
                histogram.getTotalCount(),
                histogram.getMinValue(),
                histogram.getMaxValue(),
                histogram.quantile(0.5)));
      }
    }

    System.out.println("\n** Acceleration Sum by Spatial Bin **");
    try (
        CloseableIterator<Pair<ByteArray, org.locationtech.geowave.core.store.statistics.field.Stats>> it =
            dataStore.getBinnedStatisticValues(accelerationSum)) {
      while (it.hasNext()) {
        final Pair<ByteArray, org.locationtech.geowave.core.store.statistics.field.Stats> pair =
            it.next();
        final org.locationtech.geowave.core.store.statistics.field.Stats stats = pair.getRight();
        System.out.println(
            String.format(
                "Acceleration Stats - Bin: %s, Count: %d, Sum: %.2f, Mean: %.2f",
                spatialBinning.binToString(pair.getLeft()),
                stats.count(),
                stats.sum(),
                stats.mean()));
      }
    }

    System.out.println("\n** Speed Sum by Spatial Bin **");
    try (
        CloseableIterator<Pair<ByteArray, org.locationtech.geowave.core.store.statistics.field.Stats>> it =
            dataStore.getBinnedStatisticValues(speedSum)) {
      while (it.hasNext()) {
        final Pair<ByteArray, org.locationtech.geowave.core.store.statistics.field.Stats> pair =
            it.next();
        final org.locationtech.geowave.core.store.statistics.field.Stats stats = pair.getRight();
        System.out.println(
            String.format(
                "Speed Stats - Bin: %s, Count: %d, Sum: %.2f, Mean: %.2f",
                spatialBinning.binToString(pair.getLeft()),
                stats.count(),
                stats.sum(),
                stats.mean()));
      }
    }

    // Demonstrate querying within specific bounding boxes
    System.out.println("\n***** Bounding Box Queries *****");
    // Query 1: Dense urban area where multiple tracks overlap
    final Envelope urbanArea = new Envelope(-77.040, -77.030, 38.885, 38.905);
    System.out.println(String.format("%n** Urban Area Query: %s **", urbanArea));
    final Long urbanTracks =
        dataStore.getStatisticValue(trackCount, BinConstraints.ofObject(urbanArea));
    System.out.println(String.format("Track count in dense urban area: %d", urbanTracks));

    // Query 2: Broader Washington DC area
    final Envelope dcArea = new Envelope(-77.1, -76.8, 38.85, 39.0);
    System.out.println(String.format("%n** Washington DC Area Query: %s **", dcArea));
    final Long dcTracks = dataStore.getStatisticValue(trackCount, BinConstraints.ofObject(dcArea));
    System.out.println(String.format("Track count in DC area: %d", dcTracks));

    // Query 3: Small focused area around White House coordinates
    final Envelope whiteHouseArea = new Envelope(-77.037, -77.033, 38.888, 38.892);
    System.out.println(String.format("%n** White House Area Query: %s **", whiteHouseArea));
    final Long whiteHouseTracks =
        dataStore.getStatisticValue(trackCount, BinConstraints.ofObject(whiteHouseArea));
    System.out.println(String.format("Track count near White House: %d", whiteHouseTracks));
  }

  /**
   * Calculates the twister (heading change) between consecutive track points. This would typically
   * be calculated from the actual track point sequence.
   */
  private static double calculateAverageTwister(
      final Coordinate[] coordinates,
      final double[] headings) {
    if (headings.length < 2) {
      return 0.0;
    }

    double totalTwister = 0.0;
    int count = 0;

    for (int i = 1; i < headings.length; i++) {
      double headingDiff = Math.abs(headings[i] - headings[i - 1]);
      // Handle wraparound (e.g., 359° to 1°)
      if (headingDiff > 180) {
        headingDiff = 360 - headingDiff;
      }
      totalTwister += headingDiff;
      count++;
    }

    return count > 0 ? totalTwister / count : 0.0;
  }

  /**
   * Converts time of day to hours since midnight for TDigest analysis.
   */
  private static double timeToHours(final LocalTime time) {
    return time.toSecondOfDay() / 3600.0;
  }

  /**
   * Demonstrates different spatial binning precision options for track data. This method shows how
   * different precision levels affect the granularity of spatial bins.
   */
  private static void demonstratePrecisionOptions() {
    System.out.println("\n***** Spatial Binning Precision Guide for ~10 meter resolution *****");
    System.out.println("S2 Binning:");
    System.out.println("  Level 16: ~40 meter resolution");
    System.out.println("  Level 17: ~20 meter resolution");
    System.out.println("  Level 18: ~10 meter resolution (recommended)");
    System.out.println("  Level 19: ~5 meter resolution");
    System.out.println("  Level 20: ~2.5 meter resolution");

    System.out.println("\nH3 Binning:");
    System.out.println("  Resolution 9:  ~174 meter resolution");
    System.out.println("  Resolution 10: ~65 meter resolution");
    System.out.println("  Resolution 11: ~24 meter resolution");
    System.out.println("  Resolution 12: ~9 meter resolution (closest to 10m)");
    System.out.println("  Resolution 13: ~3 meter resolution");

    System.out.println("\nGeoHash Binning:");
    System.out.println("  Precision 7: ~76 meter resolution");
    System.out.println("  Precision 8: ~19 meter resolution");
    System.out.println("  Precision 9: ~4.8 meter resolution (closest to 10m)");
    System.out.println("  Precision 10: ~1.2 meter resolution");

    System.out.println(
        "\nNote: S2 level 18 provides the closest approximation to 10-meter resolution");
    System.out.println(
        "while maintaining good performance characteristics for track data analysis.");
  }


}
