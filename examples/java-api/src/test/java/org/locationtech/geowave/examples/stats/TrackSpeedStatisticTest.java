package org.locationtech.geowave.examples.stats;

import static org.junit.Assert.*;

import org.junit.Test;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.TDigestNumericHistogram;
import org.locationtech.geowave.examples.stats.TrackDataModel.TrackPoint;

/**
 * Test for TrackSpeedStatistic with proper spatial binning support.
 */
public class TrackSpeedStatisticTest {

  @Test
  public void testTrackSpeedStatisticConfiguration() {
    final TrackSpeedStatistic statistic =
        new TrackSpeedStatistic("TestType", "trackPoints", SpatialBinningType.S2, 18);

    // Test configuration
    assertEquals("Spatial type should be S2", SpatialBinningType.S2, statistic.getSpatialType());
    assertEquals("Spatial precision should be 18", 18, statistic.getSpatialPrecision());
    assertTrue(
        "Should be compatible with TrackPoint[]",
        statistic.isCompatibleWith(TrackPoint[].class));
    assertFalse(
        "Should not be compatible with String[]",
        statistic.isCompatibleWith(String[].class));

    // Test description
    assertTrue(
        "Description should mention spatial bins",
        statistic.getDescription().contains("spatial bins"));
    assertTrue(
        "Description should mention TDigest",
        statistic.getDescription().contains("TDigest"));
  }

  @Test
  public void testTrackSpeedValueEmpty() {
    final TrackSpeedStatistic statistic =
        new TrackSpeedStatistic("TestType", "trackPoints", SpatialBinningType.S2, 18);

    final TrackSpeedStatistic.TrackSpeedValue value = statistic.createEmpty();
    assertNotNull("Value should not be null", value);
    assertNotNull("Speed digest should not be null", value.getSpeedDigest());
    assertEquals("Total point count should be 0", 0, value.getTotalPointCount());
    assertEquals("Empty speed summary", "No speed data", value.getSpeedSummary());
  }

  @Test
  public void testTrackSpeedValueSerialization() {
    final TrackSpeedStatistic statistic =
        new TrackSpeedStatistic("TestType", "trackPoints", SpatialBinningType.S2, 18);

    final TrackSpeedStatistic.TrackSpeedValue value = statistic.createEmpty();

    // Add some test data
    value.getSpeedDigest().add(25.5);
    value.getSpeedDigest().add(30.0);
    value.getSpeedDigest().add(28.7);

    // Test serialization
    final byte[] serialized = value.toBinary();
    assertNotNull("Serialized data should not be null", serialized);
    assertTrue("Serialized data should not be empty", serialized.length > 0);

    // Test deserialization
    final TrackSpeedStatistic.TrackSpeedValue deserialized =
        new TrackSpeedStatistic.TrackSpeedValue(statistic);
    deserialized.fromBinary(serialized);

    // Verify deserialized data matches original
    assertEquals(
        "Speed count should match",
        value.getSpeedDigest().getTotalCount(),
        deserialized.getSpeedDigest().getTotalCount());
    assertEquals(
        "Total point count should match",
        value.getTotalPointCount(),
        deserialized.getTotalPointCount());

    // Verify quantiles are approximately correct
    assertEquals(
        "Speed median should be around 28.7",
        28.7,
        deserialized.getSpeedDigest().quantile(0.5),
        1.0);
  }

  @Test
  public void testTrackSpeedValueMerge() {
    final TrackSpeedStatistic statistic =
        new TrackSpeedStatistic("TestType", "trackPoints", SpatialBinningType.S2, 18);

    // Create first value
    final TrackSpeedStatistic.TrackSpeedValue value1 = statistic.createEmpty();
    value1.getSpeedDigest().add(20.0);
    value1.getSpeedDigest().add(25.0);

    // Create second value
    final TrackSpeedStatistic.TrackSpeedValue value2 = statistic.createEmpty();
    value2.getSpeedDigest().add(30.0);
    value2.getSpeedDigest().add(35.0);

    // Merge
    value1.merge(value2);

    // Verify merged result
    assertEquals("Merged speed count should be 4", 4, value1.getSpeedDigest().getTotalCount());
    assertEquals("Total point count should be 4", 4, value1.getTotalPointCount());

    // Verify median is reasonable
    final double median = value1.getSpeedDigest().quantile(0.5);
    assertTrue("Median should be between 25 and 30", median >= 25.0 && median <= 30.0);
  }

  @Test
  public void testTrackSpeedSummary() {
    final TrackSpeedStatistic statistic =
        new TrackSpeedStatistic("TestType", "trackPoints", SpatialBinningType.S2, 18);

    final TrackSpeedStatistic.TrackSpeedValue value = statistic.createEmpty();

    // Test empty case
    assertEquals("Empty speed summary", "No speed data", value.getSpeedSummary());

    // Add some data to both statistics
    value.getSpeedDigest().add(20.0);
    value.getSpeedDigest().add(25.0);
    value.getSpeedDigest().add(30.0);
    value.getSpeedDigest().add(35.0);
    value.getSpeedDigest().add(40.0);

    value.getSpeedStats().add(20.0);
    value.getSpeedStats().add(25.0);
    value.getSpeedStats().add(30.0);
    value.getSpeedStats().add(35.0);
    value.getSpeedStats().add(40.0);

    // Test non-empty case
    final String speedSummary = value.getSpeedSummary();
    System.out.println("Speed summary: " + speedSummary); // Debug output
    assertNotNull("Speed summary should not be null", speedSummary);
    assertTrue("Speed summary should contain count", speedSummary.contains("Count=5"));
    assertTrue("Speed summary should contain min", speedSummary.contains("Min=20.0"));
    assertTrue("Speed summary should contain max", speedSummary.contains("Max=40.0"));
    assertTrue("Speed summary should contain mean", speedSummary.contains("Mean=30.0"));
    assertTrue("Speed summary should contain speed units", speedSummary.contains("km/h"));
  }

  @Test
  public void testDefaultConstructors() {
    // Test default constructor
    final TrackSpeedStatistic defaultStatistic = new TrackSpeedStatistic();
    assertEquals(
        "Default spatial type should be S2",
        SpatialBinningType.S2,
        defaultStatistic.getSpatialType());
    assertEquals("Default precision should be 18", 18, defaultStatistic.getSpatialPrecision());

    // Test constructor with type and field
    final TrackSpeedStatistic namedStatistic = new TrackSpeedStatistic("TestType", "trackPoints");
    assertEquals(
        "Named spatial type should be S2",
        SpatialBinningType.S2,
        namedStatistic.getSpatialType());
    assertEquals("Named precision should be 18", 18, namedStatistic.getSpatialPrecision());
  }

  @Test
  public void testCustomSpatialConfiguration() {
    // Test constructor with custom spatial configuration
    final TrackSpeedStatistic customStatistic =
        new TrackSpeedStatistic("TestType", "trackPoints", SpatialBinningType.GEOHASH, 10);
    assertEquals(
        "Custom spatial type should be GEOHASH",
        SpatialBinningType.GEOHASH,
        customStatistic.getSpatialType());
    assertEquals("Custom precision should be 10", 10, customStatistic.getSpatialPrecision());

    // Test description
    assertTrue(
        "Description should mention GEOHASH",
        customStatistic.getDescription().contains("GEOHASH"));
    assertTrue(
        "Description should mention precision 10",
        customStatistic.getDescription().contains("10"));
  }

  @Test
  public void testBasicSpeedDigestFunctionality() {
    final TrackSpeedStatistic statistic =
        new TrackSpeedStatistic("TestType", "trackPoints", SpatialBinningType.S2, 18);

    final TrackSpeedStatistic.TrackSpeedValue value = statistic.createEmpty();

    // Test direct addition to both statistics (simulating what the spatial binning would do)
    value.getSpeedDigest().add(25.5);
    value.getSpeedDigest().add(30.0);
    value.getSpeedDigest().add(28.7);

    value.getSpeedStats().add(25.5);
    value.getSpeedStats().add(30.0);
    value.getSpeedStats().add(28.7);

    // Verify results
    assertEquals("Should have 3 speed measurements", 3, value.getTotalPointCount());
    assertEquals("Stats should have 3 measurements", 3, value.getSpeedStats().count());

    // Test that StatsAccumulator provides reliable min/max
    assertTrue("Min should be around 25.5", Math.abs(value.getSpeedStats().min() - 25.5) < 0.1);
    assertTrue("Max should be around 30.0", Math.abs(value.getSpeedStats().max() - 30.0) < 0.1);
    assertTrue("Mean should be around 28.1", Math.abs(value.getSpeedStats().mean() - 28.1) < 0.1);

    // Test summary
    final String summary = value.getSpeedSummary();
    assertTrue("Summary should contain count", summary.contains("Count=3"));
    assertTrue("Summary should contain speed units", summary.contains("km/h"));
  }

  @Test
  public void testDistanceCalculation() {
    final TrackSpeedStatistic statistic =
        new TrackSpeedStatistic("TestType", "trackPoints", SpatialBinningType.S2, 18);

    final TrackSpeedStatistic.TrackSpeedValue value = statistic.createEmpty();

    // Test distance calculation between two known points
    // Washington DC to New York City is approximately 328 km
    final double distance = value.calculateDistance(38.9072, -77.0369, 40.7128, -74.0060);

    // Should be approximately 328,000 meters
    assertTrue("Distance should be around 328km", Math.abs(distance - 328000) < 50000);
  }

  @Test
  public void testTDigestDirectly() {
    // Test TDigest directly to see if the issue is with our usage
    final TDigestNumericHistogram digest = new TDigestNumericHistogram();

    System.out.println(
        "Empty digest - Count: "
            + digest.getTotalCount()
            + ", Min: "
            + digest.getMinValue()
            + ", Max: "
            + digest.getMaxValue());

    digest.add(20.0);
    digest.add(25.0);
    digest.add(30.0);

    System.out.println(
        "After adding data - Count: "
            + digest.getTotalCount()
            + ", Min: "
            + digest.getMinValue()
            + ", Max: "
            + digest.getMaxValue());

    assertTrue("Should have 3 data points", digest.getTotalCount() == 3);

    // Note: TDigest min/max are broken in GeoWave, so we test quantiles instead
    final double median = digest.quantile(0.5);
    assertTrue("Median should be around 25.0", Math.abs(median - 25.0) < 5.0);

    // Test that quantiles work (even if min/max don't)
    final double p10 = digest.quantile(0.1);
    final double p90 = digest.quantile(0.9);
    assertTrue("P10 should be reasonable", p10 >= 15.0 && p10 <= 35.0);
    assertTrue("P90 should be reasonable", p90 >= 15.0 && p90 <= 35.0);
  }
}
