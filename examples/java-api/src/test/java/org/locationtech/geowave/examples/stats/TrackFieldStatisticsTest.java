/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.examples.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.locationtech.geowave.examples.stats.AbstractTrackFieldStatistic.TrackFieldStatistics;

/**
 * Test the TrackFieldStatistics wrapper class that combines TDigest and StatsAccumulator.
 */
public class TrackFieldStatisticsTest {

  @Test
  public void testTrackFieldStatisticsBasicFunctionality() {
    final TrackFieldStatistics stats = new TrackFieldStatistics("speed");

    // Add some test data
    stats.add(10.0);
    stats.add(20.0);
    stats.add(30.0);
    stats.add(40.0);
    stats.add(50.0);

    // Test basic statistics
    assertEquals(5, stats.getStats().count());
    assertEquals(30.0, stats.getStats().mean(), 0.01);
    assertEquals(10.0, stats.getStats().min(), 0.01);
    assertEquals(50.0, stats.getStats().max(), 0.01);

    // Test TDigest functionality
    assertEquals(5, stats.getDigest().getTotalCount());
    assertEquals(30.0, stats.getDigest().quantile(0.5), 1.0); // Median

    // Test summary
    final String summary = stats.getSummary();
    assertTrue("Summary should contain field name", summary.contains("speed"));
    assertTrue("Summary should contain count", summary.contains("Count=5"));
    assertTrue("Summary should contain min", summary.contains("Min=10.00"));
    assertTrue("Summary should contain max", summary.contains("Max=50.00"));
    assertTrue("Summary should contain mean", summary.contains("Mean=30.00"));

    System.out.println("TrackFieldStatistics summary: " + summary);
  }

  @Test
  public void testTrackFieldStatisticsMerge() {
    final TrackFieldStatistics stats1 = new TrackFieldStatistics("test");
    final TrackFieldStatistics stats2 = new TrackFieldStatistics("test");

    // Add data to first stats
    stats1.add(10.0);
    stats1.add(20.0);

    // Add data to second stats
    stats2.add(30.0);
    stats2.add(40.0);

    // Merge stats2 into stats1
    stats1.merge(stats2);

    // Verify merged results
    assertEquals(4, stats1.getStats().count());
    assertEquals(25.0, stats1.getStats().mean(), 0.01);
    assertEquals(10.0, stats1.getStats().min(), 0.01);
    assertEquals(40.0, stats1.getStats().max(), 0.01);

    System.out.println("Merged TrackFieldStatistics summary: " + stats1.getSummary());
  }

  @Test
  public void testTrackSpeedStatisticWithWrapper() {
    final TrackSpeedStatistic speedStat = new TrackSpeedStatistic("Track", "trackPoints");
    final TrackSpeedStatistic.TrackSpeedValue speedValue = speedStat.createEmpty();

    // Test that the wrapper is properly initialized
    final TrackFieldStatistics fieldStats = speedValue.getFieldStatistics();
    assertEquals(0, fieldStats.getStats().count());
    assertEquals(0, fieldStats.getDigest().getTotalCount());

    // Add some data directly to the wrapper
    fieldStats.add(25.5);
    fieldStats.add(35.2);
    fieldStats.add(42.1);

    // Test that both statistics are updated
    assertEquals(3, fieldStats.getStats().count());
    assertEquals(3, fieldStats.getDigest().getTotalCount());

    // Test the summary
    final String summary = fieldStats.getSummary();
    assertTrue("Summary should contain trackPoints", summary.contains("trackPoints"));
    assertTrue("Summary should contain count", summary.contains("Count=3"));

    System.out.println("TrackSpeedStatistic wrapper summary: " + summary);
  }
}
