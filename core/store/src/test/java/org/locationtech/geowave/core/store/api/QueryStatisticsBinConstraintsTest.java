/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import static org.junit.Assert.assertEquals;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.api.StatisticsOnlyWriterTest.TestData;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.core.store.statistics.binning.FieldValueBinningStrategy;
import org.locationtech.geowave.core.store.statistics.query.BinConstraintsImpl.ExplicitConstraints;

public class QueryStatisticsBinConstraintsTest {
  private static final String TYPE_NAME = "QueryStatisticsBinConstraintsData";

  private DataStore dataStore;
  private CountStatistic alpha;
  private CountStatistic beta;

  @Before
  public void createStore() {
    dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());
    dataStore.addType(
        BasicDataTypeAdapter.newAdapter(TYPE_NAME, TestData.class, "id"),
        new NullIndex("index"));
    alpha = countStatistic("alpha");
    beta = countStatistic("beta");
    dataStore.addEmptyStatistic(alpha, beta);
    for (int i = 1; i <= 5; i++) {
      final ByteArray bin = new ByteArray("b" + i);
      dataStore.incorporateStatisticValue(alpha, count(i), bin);
      dataStore.incorporateStatisticValue(beta, count(10 * i), bin);
    }
  }

  // The in-memory store is cached statically, so anything left behind leaks into other tests.
  @After
  public void tearDown() {
    dataStore.deleteAll();
  }

  private static CountStatistic countStatistic(final String tag) {
    final CountStatistic statistic = new CountStatistic(TYPE_NAME);
    statistic.setTag(tag);
    statistic.setBinningStrategy(new FieldValueBinningStrategy("group"));
    return statistic;
  }

  private static CountValue count(final long count) {
    final CountValue value = new CountValue();
    value.fromBinary(VarintUtils.writeSignedLong(count));
    return value;
  }

  private Map<String, Long> query(final String tag, final BinConstraints constraints) {
    final StatisticQueryBuilder<CountValue, Long, ?> query =
        StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).typeName(
            TYPE_NAME).binConstraints(constraints);
    if (tag != null) {
      query.tag(tag);
    }
    final Map<String, Long> values = new HashMap<>();
    try (CloseableIterator<CountValue> it = dataStore.queryStatistics(query.build())) {
      while (it.hasNext()) {
        final CountValue value = it.next();
        final String key = value.getStatistic().getTag() + ":" + value.getBin().getString();
        assertEquals("each value is returned once", null, values.put(key, value.getValue()));
      }
    }
    return values;
  }

  @Test
  public void rangeOnlyConstraintsAreQueried() {
    final BinConstraints range =
        BinConstraints.ofRange(
            new ByteArrayRange(new ByteArray("b2").getBytes(), new ByteArray("b4").getBytes()));
    final Map<String, Long> expected = new HashMap<>();
    expected.put("alpha:b2", 2L);
    expected.put("alpha:b3", 3L);
    expected.put("alpha:b4", 4L);
    assertEquals(expected, query("alpha", range));

    final CountValue aggregated =
        dataStore.aggregateStatistics(
            StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).typeName(TYPE_NAME).tag(
                "alpha").binConstraints(range).build());
    assertEquals(9L, aggregated.getValue().longValue());
  }

  // The range x..y covers xa and xy (its end is an inclusive prefix) but not abc or zz.
  private CountStatistic zones() {
    final CountStatistic zones = countStatistic("zones");
    dataStore.addEmptyStatistic(zones);
    final String[] bins = {"ab", "abc", "xa", "xy", "zz"};
    for (int i = 0; i < bins.length; i++) {
      dataStore.incorporateStatisticValue(zones, count(i + 1), new ByteArray(bins[i]));
    }
    return zones;
  }

  private static ByteArrayRange range(final String start, final String end) {
    return new ByteArrayRange(new ByteArray(start).getBytes(), new ByteArray(end).getBytes());
  }

  private static BinConstraints binsAndRanges(
      final boolean prefix,
      final String[] bins,
      final ByteArrayRange... ranges) {
    return BinConstraints.ofObject(
        new ExplicitConstraints(
            Arrays.stream(bins).map(ByteArray::new).toArray(ByteArray[]::new),
            ranges,
            prefix,
            false));
  }

  private static BinConstraints binsAndRange(final boolean prefix, final String... bins) {
    return binsAndRanges(prefix, bins, range("x", "y"));
  }

  private long aggregate(final String tag, final BinConstraints constraints) {
    return dataStore.aggregateStatistics(
        StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).typeName(TYPE_NAME).tag(
            tag).binConstraints(constraints).build()).getValue();
  }

  @Test
  public void exactBinsAndRangesAreQueriedTogether() {
    zones();
    final Map<String, Long> expected = new HashMap<>();
    expected.put("zones:ab", 1L);
    expected.put("zones:xa", 3L);
    expected.put("zones:xy", 4L);
    assertEquals(expected, query("zones", binsAndRange(false, "ab")));
    assertEquals(8L, aggregate("zones", binsAndRange(false, "ab")));
    // xa is also in the range, and is still returned once
    assertEquals(expected, query("zones", binsAndRange(false, "ab", "xa")));
    assertEquals(8L, aggregate("zones", binsAndRange(false, "ab", "xa")));
  }

  @Test
  public void prefixBinsAndRangesAreQueriedTogether() {
    zones();
    final Map<String, Long> expected = new HashMap<>();
    expected.put("zones:ab", 1L);
    expected.put("zones:abc", 2L);
    expected.put("zones:xa", 3L);
    expected.put("zones:xy", 4L);
    assertEquals(expected, query("zones", binsAndRange(true, "ab")));
    assertEquals(10L, aggregate("zones", binsAndRange(true, "ab")));
    // the prefix xa overlaps the range, and its value is still returned once
    assertEquals(expected, query("zones", binsAndRange(true, "ab", "xa")));
    assertEquals(10L, aggregate("zones", binsAndRange(true, "ab", "xa")));
  }

  @Test
  public void invertedRangesAddNothingToBins() {
    zones();
    final Map<String, Long> expected = new HashMap<>();
    expected.put("zones:ab", 1L);
    assertEquals(
        expected,
        query("zones", binsAndRanges(false, new String[] {"ab"}, range("y", "x"))));
    expected.put("zones:abc", 2L);
    assertEquals(
        expected,
        query("zones", binsAndRanges(true, new String[] {"ab"}, range("y", "x"))));
  }

  @Test
  public void eachStatisticIsQueriedWithItsOwnConstraints() {
    final Map<String, Long> expected = new HashMap<>();
    expected.put("alpha:b1", 1L);
    expected.put("beta:b1", 10L);
    assertEquals(expected, query(null, BinConstraints.of(new ByteArray("b1"))));
  }
}
