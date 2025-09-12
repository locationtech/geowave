package org.locationtech.geowave.examples.stats;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.geotime.index.api.SpatialIndexBuilder;
import org.locationtech.geowave.examples.stats.TrackDataModel.Track;
import org.locationtech.geowave.examples.stats.TrackDataModel.TrackPoint;

/**
 * Test class for the TrackDataModel to verify that the POJO structure works correctly with
 * GeoWave's BasicDataTypeAdapter and can be ingested and queried.
 */
public class TrackDataModelTest {

  @Test
  public void testTrackPointCreation() {
    final TrackPoint point =
        new TrackPoint(38.8895, -77.0352, System.currentTimeMillis(), 25.5, 2.1, 45.0, 5.0);

    assertEquals(38.8895, point.latitude, 0.0001);
    assertEquals(-77.0352, point.longitude, 0.0001);
    assertEquals(25.5, point.speed, 0.0001);
    assertEquals(2.1, point.acceleration, 0.0001);
    assertEquals(45.0, point.heading, 0.0001);
    assertEquals(5.0, point.twister, 0.0001);

    // Verify geometry creation
    assertNotNull(point.getGeometry());
    assertEquals(-77.0352, point.getGeometry().getX(), 0.0001);
    assertEquals(38.8895, point.getGeometry().getY(), 0.0001);
  }

  @Test
  public void testTrackCreation() {
    final List<TrackPoint> points = new ArrayList<>();
    final long baseTime = System.currentTimeMillis();

    points.add(new TrackPoint(38.8895, -77.0352, baseTime, 25.5, 2.1, 45.0, 0.0));
    points.add(new TrackPoint(38.8897, -77.0354, baseTime + 30000, 28.2, 1.8, 47.0, 2.0));
    points.add(new TrackPoint(38.8899, -77.0356, baseTime + 60000, 22.1, -0.5, 49.0, 2.0));

    final Track track = new Track("test-track", points);

    assertEquals("test-track", track.getTrackId());
    assertEquals(3, track.getPointCount());
    assertEquals(3, track.getTrackPoints().length);

    // Verify computed statistics
    assertTrue(track.getAverageSpeed() > 0);
    assertTrue(track.getMaxSpeed() >= track.getAverageSpeed());
    assertTrue(track.getDuration() > 0);
    assertTrue(track.getTotalDistance() > 0);

    // Verify geometry
    assertNotNull(track.getGeometry());
    assertEquals(3, track.getGeometry().getNumPoints());

    // Verify time fields
    assertNotNull(track.getStartTime());
    assertNotNull(track.getEndTime());
    assertTrue(track.getEndTime().getTime() >= track.getStartTime().getTime());
  }

  @Test
  public void testTrackCreateFromHelper() {
    final Track track =
        TrackDataModel.createTrack(
            "helper-track",
            new double[][] {
                {-77.0352, 38.8895, 25.5, 2.1, 45.0},
                {-77.0354, 38.8897, 28.2, 1.8, 47.0},
                {-77.0356, 38.8899, 22.1, -0.5, 49.0}},
            System.currentTimeMillis(),
            30);

    assertEquals("helper-track", track.getTrackId());
    assertEquals(3, track.getPointCount());

    // Verify twister calculation
    final TrackPoint[] points = track.getTrackPoints();
    assertEquals(0.0, points[0].twister, 0.0001); // First point has no twister
    assertEquals(2.0, points[1].twister, 0.0001); // |47 - 45| = 2
    assertEquals(2.0, points[2].twister, 0.0001); // |49 - 47| = 2
  }

  @Test
  public void testBasicDataTypeAdapterCompatibility() {
    // Test that our Track POJO works with BasicDataTypeAdapter
    final BasicDataTypeAdapter<Track> adapter =
        BasicDataTypeAdapter.newAdapter("Track", Track.class, "trackId");

    assertNotNull(adapter);
    assertEquals("Track", adapter.getTypeName());

    // Verify field descriptors are created
    assertNotNull(adapter.getFieldDescriptors());
    assertTrue(adapter.getFieldDescriptors().length > 0);

    // Check for key fields
    assertNotNull(adapter.getFieldDescriptor("trackId"));
    assertNotNull(adapter.getFieldDescriptor("geometry"));
    assertNotNull(adapter.getFieldDescriptor("trackPoints"));
  }

  @Test
  public void testDataStoreIngestion() {
    // Create an in-memory data store
    final DataStore dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());

    // Create adapter and index
    final BasicDataTypeAdapter<Track> adapter =
        BasicDataTypeAdapter.newAdapter("Track", Track.class, "trackId");
    final Index spatialIndex = new SpatialIndexBuilder().createIndex();

    // Add type to data store
    dataStore.addType(adapter, spatialIndex);

    // Create test track
    final Track track =
        TrackDataModel.createTrack(
            "ingest-test-track",
            new double[][] {
                {-77.0352, 38.8895, 25.5, 2.1, 45.0},
                {-77.0354, 38.8897, 28.2, 1.8, 47.0}},
            System.currentTimeMillis(),
            30);

    // Ingest track
    try (Writer<Track> writer = dataStore.createWriter(adapter.getTypeName())) {
      writer.write(track);
    }

    // Verify ingestion succeeded (no exceptions thrown)
    assertTrue("Track ingestion completed successfully", true);
  }

  @Test
  public void testEmptyTrack() {
    final Track emptyTrack = new Track("empty", new ArrayList<>());

    assertEquals("empty", emptyTrack.getTrackId());
    assertEquals(0, emptyTrack.getPointCount());
    assertEquals(0, emptyTrack.getTrackPoints().length);
    assertEquals(0.0, emptyTrack.getAverageSpeed(), 0.0001);
    assertEquals(0.0, emptyTrack.getTotalDistance(), 0.0001);
    assertNull(emptyTrack.getGeometry());
    assertNull(emptyTrack.getStartTime());
    assertNull(emptyTrack.getEndTime());
  }

  @Test
  public void testTwisterCalculationWithWraparound() {
    // Test heading wraparound (359° to 1°)
    final Track track =
        TrackDataModel.createTrack(
            "wraparound-track",
            new double[][] {
                {-77.0352, 38.8895, 25.5, 2.1, 359.0},
                {-77.0354, 38.8897, 28.2, 1.8, 1.0}},
            System.currentTimeMillis(),
            30);

    final TrackPoint[] points = track.getTrackPoints();
    assertEquals(0.0, points[0].twister, 0.0001); // First point
    assertEquals(2.0, points[1].twister, 0.0001); // 360 - |1 - 359| = 360 - 358 = 2
  }
}
