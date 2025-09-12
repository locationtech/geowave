package org.locationtech.geowave.examples.stats;

import static org.junit.Assert.*;

import org.junit.Test;
import org.locationtech.geowave.examples.stats.TrackDataModel.TrackPoint;
import org.locationtech.geowave.examples.stats.TrackPointArraySerializationProvider.TrackPointArrayReader;
import org.locationtech.geowave.examples.stats.TrackPointArraySerializationProvider.TrackPointArrayWriter;

/**
 * Test for TrackPointArraySerializationProvider to verify proper serialization and deserialization
 * of TrackPoint arrays.
 */
public class TrackPointArraySerializationTest {

  @Test
  public void testTrackPointArraySerialization() {
    // Create test data
    final TrackPoint[] originalArray =
        {
            new TrackPoint(38.9072, -77.0369, 1640995200000L, 25.5, 1.2, 90.0, 0.0), // Washington
                                                                                     // DC
            new TrackPoint(40.7128, -74.0060, 1640995260000L, 30.0, 2.1, 95.0, 5.0), // New York
            new TrackPoint(42.3601, -71.0589, 1640995320000L, 28.7, -0.8, 100.0, 5.0) // Boston
        };

    // Test serialization
    final TrackPointArrayWriter writer = new TrackPointArrayWriter();
    final byte[] serialized = writer.writeField(originalArray);

    assertNotNull("Serialized data should not be null", serialized);
    assertTrue("Serialized data should not be empty", serialized.length > 0);

    // Test deserialization
    final TrackPointArrayReader reader = new TrackPointArrayReader();
    final TrackPoint[] deserialized = reader.readField(serialized);

    assertNotNull("Deserialized array should not be null", deserialized);
    assertEquals("Array length should match", originalArray.length, deserialized.length);

    // Verify each TrackPoint
    for (int i = 0; i < originalArray.length; i++) {
      final TrackPoint original = originalArray[i];
      final TrackPoint restored = deserialized[i];

      assertNotNull("Restored TrackPoint should not be null", restored);
      assertEquals("Latitude should match", original.latitude, restored.latitude, 0.0001);
      assertEquals("Longitude should match", original.longitude, restored.longitude, 0.0001);
      assertEquals("Timestamp should match", original.timestamp, restored.timestamp);
      assertEquals("Speed should match", original.speed, restored.speed, 0.0001);
      assertEquals(
          "Acceleration should match",
          original.acceleration,
          restored.acceleration,
          0.0001);
      assertEquals("Heading should match", original.heading, restored.heading, 0.0001);
      assertEquals("Twister should match", original.twister, restored.twister, 0.0001);
      assertEquals("Time of day should match", original.timeOfDay, restored.timeOfDay, 0.0001);
    }
  }

  @Test
  public void testEmptyArray() {
    final TrackPointArrayWriter writer = new TrackPointArrayWriter();
    final TrackPointArrayReader reader = new TrackPointArrayReader();

    // Test empty array
    final TrackPoint[] emptyArray = new TrackPoint[0];
    final byte[] serialized = writer.writeField(emptyArray);
    final TrackPoint[] deserialized = reader.readField(serialized);

    assertNotNull("Deserialized empty array should not be null", deserialized);
    assertEquals("Empty array length should be 0", 0, deserialized.length);
  }

  @Test
  public void testNullArray() {
    final TrackPointArrayWriter writer = new TrackPointArrayWriter();
    final TrackPointArrayReader reader = new TrackPointArrayReader();

    // Test null array
    final byte[] serialized = writer.writeField(null);
    final TrackPoint[] deserialized = reader.readField(serialized);

    assertNull("Deserialized null array should be null", deserialized);
  }

  @Test
  public void testArrayWithNullElements() {
    final TrackPointArrayWriter writer = new TrackPointArrayWriter();
    final TrackPointArrayReader reader = new TrackPointArrayReader();

    // Test array with null elements
    final TrackPoint[] arrayWithNulls =
        {
            new TrackPoint(38.9072, -77.0369, 1640995200000L, 25.5, 1.2, 90.0, 0.0),
            null,
            new TrackPoint(42.3601, -71.0589, 1640995320000L, 28.7, -0.8, 100.0, 5.0)};

    final byte[] serialized = writer.writeField(arrayWithNulls);
    final TrackPoint[] deserialized = reader.readField(serialized);

    assertNotNull("Deserialized array should not be null", deserialized);
    assertEquals("Array length should match", arrayWithNulls.length, deserialized.length);

    // Check first element
    assertNotNull("First element should not be null", deserialized[0]);
    assertEquals(
        "First element latitude should match",
        arrayWithNulls[0].latitude,
        deserialized[0].latitude,
        0.0001);

    // Check null element
    assertNull("Second element should be null", deserialized[1]);

    // Check third element
    assertNotNull("Third element should not be null", deserialized[2]);
    assertEquals(
        "Third element latitude should match",
        arrayWithNulls[2].latitude,
        deserialized[2].latitude,
        0.0001);
  }

  @Test
  public void testSerializationProvider() {
    // Test that the provider returns the correct reader and writer
    final TrackPointArraySerializationProvider provider =
        new TrackPointArraySerializationProvider();

    assertNotNull("Provider should return a reader", provider.getFieldReader());
    assertNotNull("Provider should return a writer", provider.getFieldWriter());

    assertTrue(
        "Reader should be correct type",
        provider.getFieldReader() instanceof TrackPointArrayReader);
    assertTrue(
        "Writer should be correct type",
        provider.getFieldWriter() instanceof TrackPointArrayWriter);
  }
}
