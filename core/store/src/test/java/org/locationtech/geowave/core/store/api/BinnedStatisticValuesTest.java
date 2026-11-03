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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.core.store.statistics.binning.FieldValueBinningStrategy;
import org.locationtech.geowave.core.store.statistics.query.BinConstraintsImpl.ExplicitConstraints;

public class BinnedStatisticValuesTest {
  private static final String TYPE_NAME = "TestData";
  private static final String[] BINS = {"b01", "b02", "b03", "b04", "b05", "b06"};

  private DataStore dataStore;
  private CountStatistic alpha;
  private CountStatistic beta;
  private CountStatistic unbinned;

  public static class TestData {
    private String id;
    private String group;

    public TestData() {}

    public String getId() {
      return id;
    }

    public void setId(final String id) {
      this.id = id;
    }

    public String getGroup() {
      return group;
    }

    public void setGroup(final String group) {
      this.group = group;
    }
  }

  @Before
  public void createStore() {
    dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());
    dataStore.addType(
        BasicDataTypeAdapter.newAdapter(TYPE_NAME, TestData.class, "id"),
        new NullIndex("index"));
    alpha = binnedCount("alpha");
    beta = binnedCount("beta");
    unbinned = new CountStatistic(TYPE_NAME);
    unbinned.setTag("unbinned");
    dataStore.addEmptyStatistic(alpha, beta, unbinned);
    dataStore.incorporateStatisticValues(alpha, valuesFor(alpha, 1));
    dataStore.incorporateStatisticValues(beta, valuesFor(beta, 100));
  }

  // The in-memory store is cached statically per namespace, so anything left behind is visible to
  // every other test in the JVM.
  @After
  public void tearDown() {
    dataStore.deleteAll();
  }

  private CountStatistic binnedCount(final String tag) {
    final CountStatistic stat = new CountStatistic(TYPE_NAME);
    stat.setTag(tag);
    stat.setBinningStrategy(new FieldValueBinningStrategy("group"));
    return stat;
  }

  private static CountValue value(final CountStatistic stat, final ByteArray bin, final long n) {
    final CountValue value = stat.createEmpty();
    value.setBin(bin);
    value.fromBinary(VarintUtils.writeSignedLong(n));
    return value;
  }

  // Bin b0N gets the count base + N.
  private static Map<ByteArray, CountValue> valuesFor(final CountStatistic stat, final long base) {
    final Map<ByteArray, CountValue> values = new LinkedHashMap<>();
    for (int i = 0; i < BINS.length; i++) {
      final ByteArray bin = new ByteArray(BINS[i]);
      values.put(bin, value(stat, bin, base + i + 1));
    }
    return values;
  }

  private static ByteArray bin(final String bin) {
    return new ByteArray(bin);
  }

  private static ByteArrayRange range(final String start, final String end) {
    return new ByteArrayRange(
        start == null ? null : start.getBytes(StandardCharsets.UTF_8),
        end == null ? null : end.getBytes(StandardCharsets.UTF_8));
  }

  private Map<String, Long> query(final CountStatistic stat, final BinConstraints constraints) {
    final Map<String, Long> result = new TreeMap<>();
    try (CloseableIterator<CountValue> it =
        dataStore.queryStatistics(
            StatisticQueryBuilder.count().typeName(TYPE_NAME).tag(stat.getTag()).binConstraints(
                constraints).build())) {
      while (it.hasNext()) {
        final CountValue value = it.next();
        // a query matching nothing yields a single empty value
        if (value.getValue() > 0) {
          result.put(
              new String(value.getBin().getBytes(), StandardCharsets.UTF_8),
              value.getValue());
        }
      }
    }
    return result;
  }

  private Map<String, Long> stored(final CountStatistic stat) {
    return query(stat, BinConstraints.allBins());
  }

  private static Map<String, Long> expected(final long base, final String... bins) {
    final Map<String, Long> result = new TreeMap<>();
    for (final String bin : bins) {
      result.put(bin, base + Integer.parseInt(bin.substring(1)));
    }
    return result;
  }

  @Test
  public void incorporatesAllBinnedValues() {
    assertEquals(expected(1, BINS), stored(alpha));
    assertEquals(expected(100, BINS), stored(beta));
  }

  @Test
  public void batchedIncorporateAggregatesWithExistingValues() {
    dataStore.incorporateStatisticValues(
        alpha,
        Collections.singletonMap(bin("b03"), value(alpha, bin("b03"), 1000)));
    assertEquals(Long.valueOf(1004), stored(alpha).get("b03"));
    assertEquals(Long.valueOf(2), stored(alpha).get("b01"));
  }

  @Test
  public void batchedIncorporateRequiresBinnedExistingStatistic() {
    final Map<ByteArray, CountValue> values =
        Collections.singletonMap(bin("b01"), value(unbinned, bin("b01"), 1));
    assertThrows(
        IllegalArgumentException.class,
        () -> dataStore.incorporateStatisticValues(unbinned, values));
    final CountStatistic missing = binnedCount("missing");
    assertThrows(
        IllegalArgumentException.class,
        () -> dataStore.incorporateStatisticValues(missing, valuesFor(missing, 0)));
  }

  @Test
  public void queryOnlyReturnsValuesOfTheQueriedStatistic() {
    assertEquals(expected(1, "b02"), query(alpha, BinConstraints.of(bin("b02"))));
    assertEquals(
        expected(100, "b02", "b05"),
        query(beta, BinConstraints.of(bin("b02"), bin("b05"))));
    assertEquals(expected(1, BINS), query(alpha, BinConstraints.ofPrefix(bin("b0"))));
    assertEquals(expected(100, "b03"), query(beta, BinConstraints.ofPrefix(bin("b03"))));
  }

  @Test
  public void queryByRangeReturnsInRangeBins() {
    assertEquals(
        expected(1, "b02", "b03", "b04"),
        query(alpha, BinConstraints.ofRange(range("b02", "b04"))));
    assertEquals(
        expected(100, "b01", "b05", "b06"),
        query(beta, BinConstraints.ofRange(range("b01", "b01"), range("b05", "b06"))));
  }

  @Test
  public void queryByOpenEndedRange() {
    assertEquals(
        expected(1, "b01", "b02"),
        query(alpha, BinConstraints.ofRange(range(null, "b02"))));
    assertEquals(
        expected(1, "b05", "b06"),
        query(alpha, BinConstraints.ofRange(range("b05", null))));
    assertEquals(expected(100, BINS), query(beta, BinConstraints.ofRange(range(null, null))));
  }

  @Test
  public void removesOnlyInRangeBinsOfTheGivenStatistic() {
    assertTrue(dataStore.removeStatisticValues(alpha, BinConstraints.ofRange(range("b02", "b04"))));
    assertEquals(expected(1, "b01", "b05", "b06"), stored(alpha));
    assertEquals(expected(100, BINS), stored(beta));
  }

  @Test
  public void removesOpenEndedRangesWithoutEscapingTheStatistic() {
    dataStore.removeStatisticValues(alpha, BinConstraints.ofRange(range(null, "b02")));
    assertEquals(expected(1, "b03", "b04", "b05", "b06"), stored(alpha));
    dataStore.removeStatisticValues(alpha, BinConstraints.ofRange(range("b05", null)));
    assertEquals(expected(1, "b03", "b04"), stored(alpha));
    assertEquals(expected(100, BINS), stored(beta));
    dataStore.removeStatisticValues(beta, BinConstraints.ofRange(range(null, null)));
    assertTrue(stored(beta).isEmpty());
    assertEquals(expected(1, "b03", "b04"), stored(alpha));
  }

  @Test
  public void invertedRangesMatchNothing() {
    assertTrue(query(alpha, BinConstraints.ofRange(range("b04", "b02"))).isEmpty());
    assertFalse(
        dataStore.removeStatisticValues(alpha, BinConstraints.ofRange(range("b04", "b02"))));
    assertEquals(expected(1, BINS), stored(alpha));
    assertEquals(expected(100, BINS), stored(beta));
  }

  @Test
  public void removalsThatMatchNothingReportFalse() {
    assertFalse(dataStore.removeStatisticValues(alpha, BinConstraints.ofRange(range("x1", "x9"))));
    assertFalse(dataStore.removeStatisticValues(alpha, BinConstraints.ofPrefix(bin("x"))));
    assertFalse(dataStore.removeStatisticValues(alpha, BinConstraints.of(bin("x5"))));
    assertEquals(expected(1, BINS), stored(alpha));
    assertEquals(expected(100, BINS), stored(beta));
  }

  // The range x..y covers xa and xy (its end is an inclusive prefix) but not abc or zz.
  private CountStatistic zones(final String tag) {
    final CountStatistic zones = binnedCount(tag);
    dataStore.addEmptyStatistic(zones);
    final Map<ByteArray, CountValue> values = new LinkedHashMap<>();
    long count = 1;
    for (final String zone : new String[] {"ab", "abc", "xa", "xy", "zz"}) {
      values.put(bin(zone), value(zones, bin(zone), count++));
    }
    dataStore.incorporateStatisticValues(zones, values);
    return zones;
  }

  private static BinConstraints binsAndRange(final boolean prefix, final String... bins) {
    return BinConstraints.ofObject(
        new ExplicitConstraints(
            Arrays.stream(bins).map(ByteArray::new).toArray(ByteArray[]::new),
            new ByteArrayRange[] {range("x", "y")},
            prefix,
            false));
  }

  @Test
  public void removesWhatBinsAndRangesTogetherRead() {
    for (final boolean prefix : new boolean[] {false, true}) {
      final CountStatistic zones = zones(prefix ? "prefixZones" : "exactZones");
      final BinConstraints constraints = binsAndRange(prefix, "ab", "xa");
      final Map<String, Long> read = query(zones, constraints);
      assertEquals(
          prefix ? Set.of("ab", "abc", "xa", "xy") : Set.of("ab", "xa", "xy"),
          read.keySet());
      final Map<String, Long> remaining = new TreeMap<>(stored(zones));
      remaining.keySet().removeAll(read.keySet());

      assertTrue(dataStore.removeStatisticValues(zones, constraints));
      assertEquals(remaining, stored(zones));
      assertEquals(expected(1, BINS), stored(alpha));
      assertEquals(expected(100, BINS), stored(beta));
    }
  }

  @Test
  public void removesExactAndPrefixBins() {
    dataStore.removeStatisticValues(alpha, BinConstraints.of(bin("b02"), bin("b05")));
    assertEquals(expected(1, "b01", "b03", "b04", "b06"), stored(alpha));
    dataStore.removeStatisticValues(alpha, BinConstraints.ofPrefix(bin("b03"), bin("b06")));
    assertEquals(expected(1, "b01", "b04"), stored(alpha));
    assertEquals(expected(100, BINS), stored(beta));
  }

  @Test
  public void removesAllBinsOfAStatistic() {
    assertTrue(dataStore.removeStatisticValues(alpha, BinConstraints.allBins()));
    assertTrue(stored(alpha).isEmpty());
    assertEquals(expected(100, BINS), stored(beta));
  }

  // recalculation first removes all stored bins, which used to stop after the first one
  @Test
  public void recalculatingAStatisticClearsEveryBin() {
    dataStore.recalcStatistic(alpha);
    assertTrue(stored(alpha).isEmpty());
    assertEquals(expected(100, BINS), stored(beta));
  }

  @Test
  public void removeByBinConstraintsRequiresBinnedStatistic() {
    assertThrows(
        IllegalArgumentException.class,
        () -> dataStore.removeStatisticValues(unbinned, BinConstraints.allBins()));
  }
}
