package org.locationtech.geowave.examples.stats;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.binning.ComplexGeometryBinningOption;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.geotime.index.api.SpatialIndexBuilder;
import org.locationtech.geowave.core.geotime.store.statistics.binning.SpatialFieldValueBinningStrategy;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.examples.stats.TrackDataModel.Track;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;

/**
 * Example demonstrating spatial binning statistics for track data using a custom data model that
 * stores entire tracks with arrays of track points. This enables computation of movement statistics
 * (speed, acceleration, heading, twister) within spatial bins based on the full track context
 * rather than individual points.
 *
 * <p>Key improvements over point-based approach: - Track-level context for computing statistics
 * within spatial bins - Ability to analyze movement patterns across track segments - More accurate
 * representation of vehicle/object behavior - Support for track-level metadata and aggregate
 * statistics
 */
public class TrackSpatialBinningWithCustomStatisticExample {

    public static void main(final String[] args) {
        new TrackSpatialBinningWithCustomStatisticExample().run();
    }

    public void run() {
        // Create an in-memory data store
        final DataStore dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());

        // Create adapter for our Track data model
        final BasicDataTypeAdapter<Track> adapter =
                BasicDataTypeAdapter.newAdapter("Track", Track.class, "trackId");

        // Create spatial index
        final Index spatialIndex = new SpatialIndexBuilder().createIndex();

        // Add the type to the data store
        dataStore.addType(adapter, spatialIndex);

        // Create spatial binning strategy with ~10 meter precision (S2 level 18)
        final SpatialFieldValueBinningStrategy spatialBinning =
                new SpatialFieldValueBinningStrategy(
                        SpatialBinningType.S2,
                        18,
                        ComplexGeometryBinningOption.USE_FULL_GEOMETRY_SCALE_BY_OVERLAP,
                        new String[]{"geometry"});

        // Create statistics for track-level attributes
        final List<Object> statistics =
                setupTrackStatistics(dataStore, adapter.getTypeName(), spatialBinning);

        // Generate sample track data
        final List<Track> tracks = generateSampleTracks();

        // Ingest track data
        ingestTracks(dataStore, adapter.getTypeName(), tracks);

        // Display results
        displayTrackStatistics(dataStore, statistics, spatialBinning);

        System.out.println("\nTrack spatial binning statistics example completed.");
        System.out.println(
                "Ingested " + tracks.size() + " tracks with spatial binning at ~10m precision.");
    }

    /**
     * Set up statistics for track-level attributes with spatial binning using our custom track
     * statistics.
     */
    private List<Object> setupTrackStatistics(
            final DataStore dataStore,
            final String typeName,
            final SpatialFieldValueBinningStrategy spatialBinning) {

        final List<Object> statistics = new ArrayList<>();

        // Track count statistic
        final CountStatistic trackCount = new CountStatistic(typeName);
        trackCount.setTag("Track-Count");
        trackCount.setBinningStrategy(spatialBinning);
        statistics.add(trackCount);

        // Custom track statistics that analyze track segments within spatial bins
        final TrackSpeedStatistic speedStats =
                new TrackSpeedStatistic(typeName, "trackPoints", SpatialBinningType.S2, 18);
        speedStats.setTag("Track-Speed-Analysis");
        speedStats.setBinningStrategy(spatialBinning);
        statistics.add(speedStats);

        final TrackAccelerationStatistic accelerationStats =
                new TrackAccelerationStatistic(typeName, "trackPoints", SpatialBinningType.S2, 18);
        accelerationStats.setTag("Track-Acceleration-Analysis");
        accelerationStats.setBinningStrategy(spatialBinning);
        statistics.add(accelerationStats);

        final TrackHeadingStatistic headingStats =
                new TrackHeadingStatistic(typeName, "trackPoints", SpatialBinningType.S2, 18);
        headingStats.setTag("Track-Heading-Analysis");
        headingStats.setBinningStrategy(spatialBinning);
        statistics.add(headingStats);

        final TrackTwisterStatistic twisterStats =
                new TrackTwisterStatistic(typeName, "trackPoints", SpatialBinningType.S2, 18);
        twisterStats.setTag("Track-Twister-Analysis");
        twisterStats.setBinningStrategy(spatialBinning);
        statistics.add(twisterStats);

        // Add all statistics to the data store
        dataStore.addEmptyStatistic(
                trackCount,
                speedStats,
                accelerationStats,
                headingStats,
                twisterStats);

        return statistics;
    }

    /**
     * Generate sample track data for testing.
     */
    private List<Track> generateSampleTracks() {
        final List<Track> tracks = new ArrayList<>();
        final long baseTimestamp = System.currentTimeMillis() - (24 * 60 * 60 * 1000); // 24 hours ago

        // Track 1: Urban commute route with varying speeds
        tracks.add(
                TrackDataModel.createTrack(
                        "track1",
                        new double[][]{
                                {-77.0352, 38.8895, 25.5, 2.1, 45.0}, // White House area
                                {-77.0354, 38.8897, 28.2, 1.8, 47.0}, // Accelerating
                                {-77.0356, 38.8899, 22.1, -0.5, 49.0}, // Slowing down
                                {-77.0366, 38.8977, 35.0, 3.2, 52.0}, // Speeding up
                                {-77.0368, 38.8979, 31.5, -1.1, 48.0}, // Slight deceleration
                                {-77.0380, 38.9058, 18.7, -2.8, 45.0} // Significant slowdown
                        },
                        baseTimestamp,
                        30)); // 30 second intervals

        // Track 2: Highway route with higher speeds
        tracks.add(
                TrackDataModel.createTrack(
                        "track2",
                        new double[][]{
                                {-77.0400, 38.8800, 65.0, 1.5, 90.0}, // Highway entrance
                                {-77.0420, 38.8820, 75.0, 2.0, 92.0}, // Accelerating to highway speed
                                {-77.0450, 38.8850, 80.0, 0.5, 95.0}, // Cruising
                                {-77.0480, 38.8880, 78.0, -0.3, 93.0}, // Slight slowdown
                                {-77.0510, 38.8910, 82.0, 1.2, 91.0}, // Back to speed
                                {-77.0540, 38.8940, 45.0, -3.5, 88.0} // Exit ramp slowdown
                        },
                        baseTimestamp + 600000, // 10 minutes later
                        45)); // 45 second intervals

        // Track 3: City delivery route with frequent stops
        tracks.add(
                TrackDataModel.createTrack(
                        "track3",
                        new double[][]{
                                {-77.0300, 38.8950, 15.0, 1.0, 180.0}, // Starting slow
                                {-77.0302, 38.8948, 0.0, -2.0, 180.0}, // Stop
                                {-77.0302, 38.8948, 0.0, 0.0, 180.0}, // Stationary
                                {-77.0305, 38.8945, 20.0, 3.0, 175.0}, // Quick acceleration
                                {-77.0308, 38.8942, 25.0, 1.0, 170.0}, // Steady speed
                                {-77.0312, 38.8938, 0.0, -3.0, 165.0} // Another stop
                        },
                        baseTimestamp + 1200000, // 20 minutes later
                        60)); // 60 second intervals

        // Track 4: Overlapping area with different behavior
        tracks.add(
                TrackDataModel.createTrack(
                        "track4",
                        new double[][]{
                                {-77.0355, 38.8896, 40.0, 2.5, 30.0}, // Same area as track1 but different behavior
                                {-77.0357, 38.8898, 45.0, 1.5, 32.0}, // Higher speeds
                                {-77.0359, 38.8900, 50.0, 1.0, 35.0}, // Continuing acceleration
                                {-77.0365, 38.8975, 55.0, 1.2, 38.0}, // Peak speed
                                {-77.0367, 38.8977, 52.0, -0.8, 36.0}, // Slight deceleration
                                {-77.0370, 38.8980, 48.0, -1.0, 34.0} // Gradual slowdown
                        },
                        baseTimestamp + 1800000, // 30 minutes later
                        25)); // 25 second intervals

        return tracks;
    }

    /**
     * Ingest track data into the data store.
     */
    private void ingestTracks(
            final DataStore dataStore,
            final String typeName,
            final List<Track> tracks) {

        System.out.println("Ingesting " + tracks.size() + " tracks...");

        try (Writer<Track> writer = dataStore.createWriter(typeName)) {
            for (final Track track : tracks) {
                writer.write(track);
                System.out.println(
                        "Ingested track: "
                                + track.getTrackId()
                                + " with "
                                + track.getPointCount()
                                + " points, "
                                + "avg speed: "
                                + String.format("%.1f", track.getAverageSpeed())
                                + " km/h, "
                                + "duration: "
                                + (track.getDuration() / 1000)
                                + " seconds");
            }
        }

        System.out.println("Track ingestion completed.");
    }

    /**
     * Display the track statistics results from our custom track statistics.
     */
    private void displayTrackStatistics(
            final DataStore dataStore,
            final List<Object> statistics,
            final SpatialFieldValueBinningStrategy spatialBinning) {

        System.out.println("\n***** TRACK SPATIAL BINNING STATISTICS RESULTS *****");

        // Find our custom statistics
        CountStatistic trackCount = null;
        TrackSpeedStatistic speedStats = null;
        TrackAccelerationStatistic accelerationStats = null;
        TrackHeadingStatistic headingStats = null;
        TrackTwisterStatistic twisterStats = null;

        for (final Object stat : statistics) {
            if (stat instanceof CountStatistic) {
                trackCount = (CountStatistic) stat;
            } else if (stat instanceof TrackSpeedStatistic) {
                speedStats = (TrackSpeedStatistic) stat;
            } else if (stat instanceof TrackAccelerationStatistic) {
                accelerationStats = (TrackAccelerationStatistic) stat;
            } else if (stat instanceof TrackHeadingStatistic) {
                headingStats = (TrackHeadingStatistic) stat;
            } else if (stat instanceof TrackTwisterStatistic) {
                twisterStats = (TrackTwisterStatistic) stat;
            }
        }

        // Display track count by spatial bin, this shows as 0 because it is weighted count and each track is contributing to so many spatial bins that the weighted count rounds to 0
        if (trackCount != null) {
            System.out.println("\n** Track Count by Spatial Bin **");
            try (CloseableIterator<Pair<ByteArray, Long>> it =
                         dataStore.getBinnedStatisticValues(trackCount)) {
                while (it.hasNext()) {
                    final Pair<ByteArray, Long> pair = it.next();
                    System.out.println(
                            String.format(
                                    "Track Count: %d, Bin: %s",
                                    pair.getRight(),
                                    spatialBinning.binToString(pair.getLeft())));
                }
            }
        }

        // Display speed statistics by spatial bin
        if (speedStats != null) {
            System.out.println("\n** Track Speed Analysis by Spatial Bin **");
            try (CloseableIterator<Pair<ByteArray, AbstractTrackFieldStatistic.TrackFieldStatistics>> it =
                         dataStore.getBinnedStatisticValues(speedStats)) {
                while (it.hasNext()) {
                    final Pair<ByteArray, AbstractTrackFieldStatistic.TrackFieldStatistics> pair = it.next();
                    final AbstractTrackFieldStatistic.TrackFieldStatistics value = pair.getRight();
                    System.out.println(
                            String.format(
                                    "Bin: %s, %s",
                                    spatialBinning.binToString(pair.getLeft()),
                                    value.getSummary()));
                }
            }
        }

        // Display acceleration statistics by spatial bin
        if (accelerationStats != null) {
            System.out.println("\n** Track Acceleration Analysis by Spatial Bin **");
            try (CloseableIterator<Pair<ByteArray, AbstractTrackFieldStatistic.TrackFieldStatistics>> it =
                         dataStore.getBinnedStatisticValues(accelerationStats)) {
                while (it.hasNext()) {
                    final Pair<ByteArray, AbstractTrackFieldStatistic.TrackFieldStatistics> pair = it.next();
                    final AbstractTrackFieldStatistic.TrackFieldStatistics value = pair.getRight();
                    System.out.println(
                            String.format(
                                    "Bin: %s, %s",
                                    spatialBinning.binToString(pair.getLeft()),
                                    value.getSummary()));
                }
            }
        }

        // Display heading statistics by spatial bin
        if (headingStats != null) {
            System.out.println("\n** Track Heading Analysis by Spatial Bin **");
            try (CloseableIterator<Pair<ByteArray, AbstractTrackFieldStatistic.TrackFieldStatistics>> it =
                         dataStore.getBinnedStatisticValues(headingStats)) {
                while (it.hasNext()) {
                    final Pair<ByteArray, AbstractTrackFieldStatistic.TrackFieldStatistics> pair = it.next();
                    final AbstractTrackFieldStatistic.TrackFieldStatistics value = pair.getRight();
                    System.out.println(
                            String.format(
                                    "Bin: %s, %s",
                                    spatialBinning.binToString(pair.getLeft()),
                                    value.getSummary()));
                }
            }
        }

        // Display twister statistics by spatial bin
        if (twisterStats != null) {
            System.out.println("\n** Track Twister Analysis by Spatial Bin **");
            try (CloseableIterator<Pair<ByteArray, AbstractTrackFieldStatistic.TrackFieldStatistics>> it =
                         dataStore.getBinnedStatisticValues(twisterStats)) {
                while (it.hasNext()) {
                    final Pair<ByteArray, AbstractTrackFieldStatistic.TrackFieldStatistics> pair = it.next();
                    final AbstractTrackFieldStatistic.TrackFieldStatistics value = pair.getRight();
                    System.out.println(
                            String.format(
                                    "Bin: %s, %s",
                                    spatialBinning.binToString(pair.getLeft()),
                                    value.getSummary()));
                }
            }
        }

        System.out.println("\n** Summary **");
        System.out.println("Custom track statistics provide detailed analysis of:");
        System.out.println("- Speed patterns within spatial bins based on actual track segments");
        System.out.println("- Acceleration/deceleration behavior in different geographic areas");
        System.out.println("- Heading distributions showing movement directions");
        System.out.println("- Twister values indicating turning/maneuvering intensity");
        System.out.println(
                "Each statistic uses both TDigest (for quantiles) and StatsAccumulator (for basic stats)");
    }
}
