/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.StatisticQueryBuilder;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.core.store.statistics.binning.FieldValueBinningStrategy;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Two binned statistics share a group (the same type), so every delete is also a check that it
 * stays within one statistic. The expected result of each constraint is computed from the bins, so
 * the same checks run over fixed and variable length bins.
 */
@RunWith(GeoWaveITRunner.class)
public class GeoWaveStatisticValueRangesIT extends AbstractGeoWaveIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveStatisticValueRangesIT.class);

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB,
          GeoWaveStoreType.FILESYSTEM})
  protected DataStorePluginOptions dataStore;

  private static final String[] FIXED_BINS = {"b01", "b02", "b03", "b04", "b05"};
  // a bin followed by the rest of a key sorts inside or past the raw key range of a neighbor
  private static final String[] DIGIT_BINS = {"1", "10", "20", "5"};
  private static final String[] LETTER_BINS = {"a", "b", "b1", "c"};

  private static long startMillis;

  private DataStore ds;
  private CountStatistic alpha;
  private CountStatistic beta;

  @BeforeClass
  public static void reportTestStart() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("* RUNNING GeoWaveStatisticValueRangesIT *");
    LOGGER.warn("-----------------------------------------");
  }

  @AfterClass
  public static void reportTestFinish() {
    LOGGER.warn("------------------------------------------");
    LOGGER.warn("* FINISHED GeoWaveStatisticValueRangesIT *");
    LOGGER.warn("*         {}s elapsed.", (System.currentTimeMillis() - startMillis) / 1000);
    LOGGER.warn("------------------------------------------");
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStore;
  }

  @Before
  public void initialize() {
    ds = dataStore.createDataStore();
    ds.addType(
        SimpleIngest.createDataAdapter(SimpleIngest.createPointFeatureType()),
        SimpleIngest.createSpatialIndex());
    alpha = binnedCount("alpha");
    beta = binnedCount("beta");
    ds.addEmptyStatistic(alpha, beta);
  }

  @After
  public void cleanupWorkspace() {
    TestUtils.deleteAll(dataStore);
  }

  private static CountStatistic binnedCount(final String tag) {
    final CountStatistic stat = new CountStatistic(SimpleIngest.FEATURE_NAME);
    stat.setTag(tag);
    stat.setBinningStrategy(new FieldValueBinningStrategy("Comment"));
    return stat;
  }

  // The count of a bin is its position, plus an offset that tells the statistics apart.
  private static Map<String, Long> countsFor(final String[] bins, final long offset) {
    final Map<String, Long> counts = new LinkedHashMap<>();
    for (int i = 0; i < bins.length; i++) {
      counts.put(bins[i], offset + i + 1);
    }
    return counts;
  }

  private void write(final CountStatistic stat, final Map<String, Long> counts) {
    final Map<ByteArray, CountValue> values = new LinkedHashMap<>();
    counts.forEach((bin, count) -> {
      final ByteArray key = new ByteArray(bin);
      final CountValue value = stat.createEmpty();
      value.setBin(key);
      value.fromBinary(VarintUtils.writeSignedLong(count));
      values.put(key, value);
    });
    ds.incorporateStatisticValues(stat, values);
  }

  private Map<String, Long> query(final CountStatistic stat, final BinConstraints constraints) {
    final Map<String, Long> result = new TreeMap<>();
    try (CloseableIterator<CountValue> it =
        ds.queryStatistics(
            StatisticQueryBuilder.count().typeName(SimpleIngest.FEATURE_NAME).tag(
                stat.getTag()).binConstraints(constraints).build())) {
      while (it.hasNext()) {
        final CountValue value = it.next();
        // a query that matches nothing yields a single empty value
        if (value.getValue() > 0) {
          result.put(
              new String(value.getBin().getBytes(), StandardCharsets.UTF_8),
              value.getValue());
        }
      }
    }
    return result;
  }

  private Map<String, Long> binned(final CountStatistic stat, final BinConstraints constraints) {
    final Map<String, Long> result = new TreeMap<>();
    try (CloseableIterator<Pair<ByteArray, Long>> it =
        ds.getBinnedStatisticValues(stat, constraints)) {
      while (it.hasNext()) {
        final Pair<ByteArray, Long> pair = it.next();
        if (pair.getValue() > 0) {
          result.put(new String(pair.getKey().getBytes(), StandardCharsets.UTF_8), pair.getValue());
        }
      }
    }
    return result;
  }

  private static Map<String, Long> select(
      final Map<String, Long> counts,
      final Predicate<byte[]> matches) {
    final Map<String, Long> selected = new TreeMap<>();
    counts.forEach((bin, count) -> {
      if (matches.test(bin.getBytes(StandardCharsets.UTF_8))) {
        selected.put(bin, count);
      }
    });
    return selected;
  }

  private static Map<String, Long> without(
      final Map<String, Long> counts,
      final Map<String, Long> removed) {
    final Map<String, Long> remaining = new TreeMap<>(counts);
    remaining.keySet().removeAll(removed.keySet());
    return remaining;
  }

  // The end is an inclusive prefix, so a bin is in the range up to the next prefix of the end.
  private static Predicate<byte[]> inRange(final ByteArrayRange range) {
    return bin -> ((range.getStart() == null)
        || (ByteArrayUtils.compare(bin, range.getStart()) >= 0))
        && ((range.getEnd() == null)
            || (ByteArrayUtils.compare(bin, range.getEndAsNextPrefix()) < 0));
  }

  private static ByteArrayRange range(final String start, final String end) {
    return new ByteArrayRange(
        start == null ? null : start.getBytes(StandardCharsets.UTF_8),
        end == null ? null : end.getBytes(StandardCharsets.UTF_8));
  }

  // every combination of the bins as start and end, which includes inverted ranges, and open ends
  private static List<ByteArrayRange> rangesOver(final String[] bins) {
    final List<ByteArrayRange> ranges = new ArrayList<>();
    for (final String start : bins) {
      ranges.add(range(start, null));
      ranges.add(range(null, start));
      for (final String end : bins) {
        ranges.add(range(start, end));
      }
    }
    ranges.add(range(null, null));
    // bounds that are not bins themselves
    ranges.add(range("b0", "b2"));
    ranges.add(range("0", "2"));
    ranges.add(range("a", "b0"));
    ranges.add(range("10", "20"));
    return ranges;
  }

  private void checkRanges(final String[] bins) {
    final Map<String, Long> alphaCounts = countsFor(bins, 0);
    final Map<String, Long> betaCounts = countsFor(bins, 100);
    for (final ByteArrayRange range : rangesOver(bins)) {
      final BinConstraints constraints = BinConstraints.ofRange(range);
      final String description = Arrays.toString(bins) + " " + describe(range);
      write(alpha, alphaCounts);
      write(beta, betaCounts);

      final Map<String, Long> inRange = select(alphaCounts, inRange(range));
      assertEquals("query " + description, inRange, query(alpha, constraints));
      assertEquals("binned " + description, inRange, binned(alpha, constraints));
      assertEquals(
          "query of the other statistic " + description,
          select(betaCounts, inRange(range)),
          query(beta, constraints));

      ds.removeStatisticValues(alpha, constraints);
      assertEquals(
          "remaining " + description,
          without(alphaCounts, inRange),
          query(alpha, BinConstraints.allBins()));
      assertEquals("untouched " + description, betaCounts, query(beta, BinConstraints.allBins()));

      ds.removeStatisticValues(alpha, BinConstraints.allBins());
      ds.removeStatisticValues(beta, BinConstraints.allBins());
    }
  }

  private static String describe(final ByteArrayRange range) {
    return "["
        + (range.getStart() == null ? "null" : new String(range.getStart(), StandardCharsets.UTF_8))
        + ", "
        + (range.getEnd() == null ? "null" : new String(range.getEnd(), StandardCharsets.UTF_8))
        + "]";
  }

  @Test
  public void testRangesOverFixedLengthBins() {
    checkRanges(FIXED_BINS);
  }

  @Test
  public void testRangesOverVariableLengthDigitBins() {
    checkRanges(DIGIT_BINS);
  }

  @Test
  public void testRangesOverVariableLengthLetterBins() {
    checkRanges(LETTER_BINS);
  }

  @Test
  public void testMultipleRangesAndExactAndPrefixBins() {
    for (final String[] bins : new String[][] {FIXED_BINS, DIGIT_BINS, LETTER_BINS}) {
      final Map<String, Long> alphaCounts = countsFor(bins, 0);
      final Map<String, Long> betaCounts = countsFor(bins, 100);
      write(alpha, alphaCounts);
      write(beta, betaCounts);

      final BinConstraints exact =
          BinConstraints.of(new ByteArray(bins[0]), new ByteArray(bins[2]));
      final Map<String, Long> exactBins =
          select(
              alphaCounts,
              bin -> Arrays.equals(bin, bins[0].getBytes(StandardCharsets.UTF_8))
                  || Arrays.equals(bin, bins[2].getBytes(StandardCharsets.UTF_8)));
      assertEquals(exactBins, query(alpha, exact));
      ds.removeStatisticValues(alpha, exact);
      assertEquals(without(alphaCounts, exactBins), query(alpha, BinConstraints.allBins()));

      final byte[] prefix = bins[bins.length - 1].substring(0, 1).getBytes(StandardCharsets.UTF_8);
      final BinConstraints prefixed = BinConstraints.ofPrefix(new ByteArray(prefix));
      final Map<String, Long> prefixedBins =
          select(without(alphaCounts, exactBins), bin -> ByteArrayUtils.startsWith(bin, prefix));
      assertEquals(prefixedBins, query(alpha, prefixed));
      ds.removeStatisticValues(alpha, prefixed);
      assertEquals(
          without(without(alphaCounts, exactBins), prefixedBins),
          query(alpha, BinConstraints.allBins()));

      final ByteArrayRange[] twoRanges =
          new ByteArrayRange[] {range(bins[1], bins[1]), range(bins[bins.length - 1], null)};
      ds.removeStatisticValues(alpha, BinConstraints.allBins());
      write(alpha, alphaCounts);
      final Map<String, Long> inEither =
          select(
              alphaCounts,
              bin -> inRange(twoRanges[0]).test(bin) || inRange(twoRanges[1]).test(bin));
      assertEquals(inEither, query(alpha, BinConstraints.ofRange(twoRanges)));
      ds.removeStatisticValues(alpha, BinConstraints.ofRange(twoRanges));
      assertEquals(without(alphaCounts, inEither), query(alpha, BinConstraints.allBins()));

      assertEquals(betaCounts, query(beta, BinConstraints.allBins()));
      assertTrue(ds.removeStatisticValues(beta, BinConstraints.allBins()));
      ds.removeStatisticValues(alpha, BinConstraints.allBins());
    }
  }

  // Accumulo reports true whenever a delete ran. HBase, Cassandra and DynamoDB delete an exact bin
  // without reading it first, so they report true for it too.
  @Test
  public void testRemovalsThatMatchNothingReportFalse() {
    final Map<String, Long> alphaCounts = countsFor(FIXED_BINS, 0);
    final Map<String, Long> betaCounts = countsFor(FIXED_BINS, 100);
    write(alpha, alphaCounts);
    write(beta, betaCounts);

    final boolean emptyRange =
        ds.removeStatisticValues(alpha, BinConstraints.ofRange(range("x1", "x9")));
    final boolean emptyPrefix =
        ds.removeStatisticValues(alpha, BinConstraints.ofPrefix(new ByteArray("x")));
    final boolean missingBin =
        ds.removeStatisticValues(alpha, BinConstraints.of(new ByteArray("x5")));
    final String type = dataStore.getType();
    if (!"accumulo".equals(type)) {
      assertFalse("empty range on " + type, emptyRange);
      assertFalse("empty prefix on " + type, emptyPrefix);
    }
    if (Set.of("rocksdb", "filesystem", "redis").contains(type)) {
      assertFalse("missing bin on " + type, missingBin);
    }
    assertEquals(alphaCounts, query(alpha, BinConstraints.allBins()));
    assertEquals(betaCounts, query(beta, BinConstraints.allBins()));
  }

  @Test
  public void testBatchedIncorporateAggregatesWithExistingValues() {
    write(alpha, countsFor(FIXED_BINS, 0));
    write(alpha, countsFor(FIXED_BINS, 1000));
    final Map<String, Long> expected = new TreeMap<>();
    countsFor(FIXED_BINS, 0).forEach((bin, count) -> expected.put(bin, (2 * count) + 1000));
    assertEquals(expected, query(alpha, BinConstraints.allBins()));
  }
}
