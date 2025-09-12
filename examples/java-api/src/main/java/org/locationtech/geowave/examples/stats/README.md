# Track Data Model for Spatial Binning Statistics

This package contains a custom data model for track data that enables spatial binning statistics for speed, heading, acceleration, and twister calculations within spatial bins. The model stores entire tracks as single entities with arrays of track points, providing better context for movement analysis compared to individual point-based approaches.

## Key Components

### 1. TrackDataModel.java
Contains the core data model classes:

- **TrackPoint**: Represents a single point within a track with GPS coordinates and movement attributes
- **Track**: Complete track data model with GeoWave annotations for proper indexing and spatial binning
- **Helper methods**: For creating tracks from raw coordinate data

### 2. TrackPointArrayStatistic.java
Custom statistic that computes TDigest statistics for speed, acceleration, heading, and twister from track point arrays within spatial bins.

### 3. TrackSpatialBinningWithCustomStatisticExample.java
Example demonstrating how to use the track data model with spatial binning statistics.

### 4. TrackDataModelTest.java
Unit tests verifying the data model functionality.

## Key Features

### Track-Level Context
- Stores entire tracks as single entities rather than individual points
- Enables computation of movement statistics within spatial bins based on full track context
- Provides track-level metadata (duration, total distance, average speeds, etc.)

### Spatial Binning Support
- Uses GeoWave annotations for proper spatial indexing
- Supports spatial binning at ~10 meter precision using S2 level 18
- Enables querying statistics within specific geographic areas

### Movement Statistics
- **Speed**: km/h measurements with TDigest statistics
- **Acceleration**: m/s² measurements with quantile analysis
- **Heading**: Directional data (0-360 degrees)
- **Twister**: Heading change calculations with wraparound handling

### Custom Statistics
- TrackPointArrayStatistic processes track point arrays to extract individual point statistics
- Maintains spatial binning context while analyzing detailed movement patterns
- Provides TDigest-based quantile analysis for all movement attributes

## Usage Example

```java
// Create track from coordinate data
Track track = TrackDataModel.createTrack(
    "track1",
    new double[][] {
        {-77.0352, 38.8895, 25.5, 2.1, 45.0}, // lon, lat, speed, accel, heading
        {-77.0354, 38.8897, 28.2, 1.8, 47.0},
        {-77.0356, 38.8899, 22.1, -0.5, 49.0}
    },
    System.currentTimeMillis(),
    30); // 30 second intervals

// Create adapter and ingest
BasicDataTypeAdapter<Track> adapter = 
    BasicDataTypeAdapter.newAdapter("Track", Track.class, "trackId");
dataStore.addType(adapter, spatialIndex);

try (Writer<Track> writer = dataStore.createWriter(adapter.getTypeName())) {
    writer.write(track);
}

// Set up custom statistics
TrackPointArrayStatistic trackPointStats = 
    new TrackPointArrayStatistic(typeName, "trackPoints");
trackPointStats.setBinningStrategy(spatialBinning);
dataStore.addEmptyStatistic(trackPointStats);
```

## Data Model Structure

### TrackPoint
```java
public static class TrackPoint {
    public final double latitude;
    public final double longitude;
    public final long timestamp;
    public final double speed;        // km/h
    public final double acceleration; // m/s²
    public final double heading;      // degrees (0-360)
    public final double twister;      // heading change from previous point
    public final double timeOfDay;    // hours since midnight
}
```

### Track (with GeoWave annotations)
```java
@GeoWaveDataType
public static class Track {
    @GeoWaveField(name = "trackId")
    private final String trackId;

    @GeoWaveSpatialField(spatialIndexHint = true, crs = "EPSG:4326")
    private final LineString geometry; // Overall track geometry

    @GeoWaveTemporalField(timeIndexHint = true)
    private final Date startTime;

    @GeoWaveField(name = "trackPoints")
    private final TrackPoint[] trackPoints; // Array of detailed points
    
    // Additional computed statistics...
}
```

## Advantages Over Point-Based Approach

1. **Context Preservation**: Maintains track-level context for better movement analysis
2. **Efficient Storage**: Single entity per track reduces storage overhead
3. **Rich Metadata**: Track-level statistics (duration, distance, average speeds)
4. **Spatial Binning**: Enables analysis of track behavior within geographic regions
5. **Movement Analysis**: Better representation of vehicle/object behavior patterns

## Statistics Output Example

```
** Track Point Array Statistics by Spatial Bin **
Bin: S2CellId(0x89c25892b4c00000)
  Speed: Count=12, Min=15.00, Max=55.00, Median=32.50, 95th=52.00 km/h
  Acceleration: Count=12, Min=-3.50, Max=3.20, Median=1.20 m/s²
  Heading: Count=12, Min=30.0°, Max=95.0°, Median=47.5°
  Twister: Count=12, Min=0.0°, Max=15.0°, Median=2.0°, 95th=8.5°
```

## Running the Example

```bash
# Compile and run the example
mvn compile exec:java -Dexec.mainClass="org.locationtech.geowave.examples.stats.TrackSpatialBinningWithCustomStatisticExample"

# Run tests
mvn test -Dtest=TrackDataModelTest
```

## Next Steps

1. **Custom Statistic Enhancement**: Extend TrackPointArrayStatistic to support additional movement metrics
2. **Temporal Binning**: Add time-based binning strategies for temporal analysis
3. **Query Optimization**: Implement specialized queries for track-based analysis
4. **Visualization**: Create tools for visualizing track statistics within spatial bins
5. **Performance Tuning**: Optimize for large-scale track datasets

This data model provides a foundation for sophisticated movement analysis within GeoWave's spatial binning framework, enabling more accurate and contextual statistics for track-based data.
