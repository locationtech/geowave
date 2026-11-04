/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.StatisticQueryBuilder;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.core.store.statistics.binning.FieldValueBinningStrategy;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.datastore.rocksdb.config.RocksDBOptions;
import org.locationtech.geowave.datastore.rocksdb.operations.RocksDBOperations;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBClientCache;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBUtils;

public class RocksDBStatisticValuesTest {
  private static final String TYPE_NAME = "TestData";
  private static final String[] BINS = {"b01", "b02", "b03", "b04", "b05", "b06"};

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private RocksDBOptions options;
  private DataStore dataStore;
  private CountStatistic alpha;
  private CountStatistic beta;

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
    options = new RocksDBOptions();
    options.setDirectory(folder.getRoot().getAbsolutePath());
    dataStore = new RocksDBStoreFactoryFamily().getDataStoreFactory().createStore(options);
    dataStore.addType(
        BasicDataTypeAdapter.newAdapter(TYPE_NAME, TestData.class, "id"),
        new NullIndex("index"));
    alpha = binnedCount("alpha");
    beta = binnedCount("beta");
    dataStore.addEmptyStatistic(alpha, beta);
    dataStore.incorporateStatisticValues(alpha, valuesFor(alpha, 1));
    dataStore.incorporateStatisticValues(beta, valuesFor(beta, 100));
  }

  @After
  public void closeClients() {
    RocksDBClientCache.getInstance().closeAll();
  }

  private static CountStatistic binnedCount(final String tag) {
    final CountStatistic stat = new CountStatistic(TYPE_NAME);
    stat.setTag(tag);
    stat.setBinningStrategy(new FieldValueBinningStrategy("group"));
    return stat;
  }

  // Bin b0N gets the count base + N.
  private static Map<ByteArray, CountValue> valuesFor(final CountStatistic stat, final long base) {
    final Map<ByteArray, CountValue> values = new LinkedHashMap<>();
    for (int i = 0; i < BINS.length; i++) {
      final ByteArray bin = new ByteArray(BINS[i]);
      final CountValue value = stat.createEmpty();
      value.setBin(bin);
      value.fromBinary(VarintUtils.writeSignedLong(base + i + 1));
      values.put(bin, value);
    }
    return values;
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
  public void queriesRangesOfOneStatistic() {
    assertEquals(expected(1, BINS), stored(alpha));
    assertEquals(
        expected(100, "b02", "b03", "b04"),
        query(beta, BinConstraints.ofRange(range("b02", "b04"))));
  }

  @Test
  public void removesRangesOfOneStatistic() {
    assertTrue(dataStore.removeStatisticValues(alpha, BinConstraints.ofRange(range("b02", "b04"))));
    assertEquals(expected(1, "b01", "b05", "b06"), stored(alpha));
    assertEquals(expected(100, BINS), stored(beta));

    dataStore.removeStatisticValues(alpha, BinConstraints.ofRange(range(null, "b01")));
    dataStore.removeStatisticValues(alpha, BinConstraints.ofRange(range("b06", null)));
    assertEquals(expected(1, "b05"), stored(alpha));
    assertEquals(expected(100, BINS), stored(beta));
  }

  @Test
  public void removesExactPrefixAndAllBins() {
    dataStore.removeStatisticValues(beta, BinConstraints.of(new ByteArray("b02")));
    dataStore.removeStatisticValues(beta, BinConstraints.ofPrefix(new ByteArray("b05")));
    assertEquals(expected(100, "b01", "b03", "b04", "b06"), stored(beta));
    assertEquals(expected(1, BINS), stored(alpha));

    dataStore.removeStatisticValues(beta, BinConstraints.allBins());
    assertTrue(stored(beta).isEmpty());
    assertEquals(expected(1, BINS), stored(alpha));
  }

  private CountStatistic incorporate(final String tag, final String... bins) {
    final CountStatistic stat = binnedCount(tag);
    dataStore.addEmptyStatistic(stat);
    final Map<ByteArray, CountValue> values = new LinkedHashMap<>();
    long count = 1;
    for (final String bin : bins) {
      final ByteArray key = new ByteArray(bin);
      final CountValue value = stat.createEmpty();
      value.setBin(key);
      value.fromBinary(VarintUtils.writeSignedLong(count++));
      values.put(key, value);
    }
    dataStore.incorporateStatisticValues(stat, values);
    return stat;
  }

  private static java.util.Set<String> keys(final Map<String, Long> values) {
    return values.keySet();
  }

  // Keys are primary id then secondary id, so bin 1 followed by the group id sorts inside the raw
  // key range of [10, 20] and bin b followed by the group id sorts after the raw end of [a, b0].
  @Test
  public void rangesOverVariableLengthBinsOnlyMatchBinsInTheRange() {
    final CountStatistic digits = incorporate("digits", "1", "10", "20", "5");
    final CountStatistic letters = incorporate("letters", "a", "b", "b1", "c");

    assertEquals(
        java.util.Set.of("10", "20"),
        keys(query(digits, BinConstraints.ofRange(range("10", "20")))));
    assertEquals(
        java.util.Set.of("a", "b"),
        keys(query(letters, BinConstraints.ofRange(range("a", "b0")))));

    dataStore.removeStatisticValues(digits, BinConstraints.ofRange(range("10", "20")));
    assertEquals(java.util.Set.of("1", "5"), keys(stored(digits)));
    dataStore.removeStatisticValues(letters, BinConstraints.ofRange(range("a", "b0")));
    assertEquals(java.util.Set.of("b1", "c"), keys(stored(letters)));
    // neither delete touches the fixed-length statistics in the same group
    assertEquals(expected(1, BINS), stored(alpha));
    assertEquals(expected(100, BINS), stored(beta));
  }

  @Test
  public void openAndInvertedRangesStayWithinTheStatistic() {
    final CountStatistic letters = incorporate("letters", "a", "b", "b1", "c");
    dataStore.removeStatisticValues(alpha, BinConstraints.ofRange(range("b05", null)));
    dataStore.removeStatisticValues(alpha, BinConstraints.ofRange(range("b04", "b02")));
    assertEquals(expected(1, "b01", "b02", "b03", "b04"), stored(alpha));
    assertEquals(expected(100, BINS), stored(beta));
    assertEquals(java.util.Set.of("a", "b", "b1", "c"), keys(stored(letters)));
  }

  // The start and end of this day of 8-byte hour bins first differ in the fifth byte, so the scan
  // stops at the end of the 2^24 ms block (under five hours) that holds the end. Scanning to the
  // end of their common prefix would read every bin from the start on.
  @Test
  public void rangesOverFixedLengthBinsScanLittlePastTheEnd() {
    final long hour = 3_600_000L;
    final long day = Instant.parse("2026-11-03T00:00:00Z").toEpochMilli();
    final CountStatistic hourly = binnedCount("hourly");
    dataStore.addEmptyStatistic(hourly);
    final Map<ByteArray, CountValue> values = new LinkedHashMap<>();
    for (long time = day - (48 * hour); time < (day + (96 * hour)); time += hour) {
      final ByteArray bin = new ByteArray(Lexicoders.LONG.toByteArray(time));
      final CountValue value = hourly.createEmpty();
      value.setBin(bin);
      value.fromBinary(VarintUtils.writeSignedLong(1));
      values.put(bin, value);
    }
    dataStore.incorporateStatisticValues(hourly, values);

    final byte[] start = Lexicoders.LONG.toByteArray(day);
    final byte[] end = Lexicoders.LONG.toByteArray(day + (23 * hour));
    assertEquals(
        24L,
        dataStore.aggregateStatistics(
            StatisticQueryBuilder.count().typeName(TYPE_NAME).tag(hourly.getTag()).binConstraints(
                BinConstraints.ofRange(
                    new ByteArrayRange(start, end))).build()).getValue().longValue());

    final ByteArrayRange valueIds =
        new ByteArrayRange(
            StatisticValue.getValueId(hourly.getId(), start),
            StatisticValue.getValueId(hourly.getId(), end));
    int scanned = 0;
    try (CloseableIterator<GeoWaveMetadata> it =
        RocksDBUtils.getMetadataTable(
            new RocksDBOperations(options).getClient(),
            MetadataType.STATISTIC_VALUES).iterator(DataStoreUtils.toKeyScanRange(valueIds))) {
      while (it.hasNext()) {
        it.next();
        scanned++;
      }
    }
    assertTrue("scanned " + scanned + " keys", scanned <= (24 + 5));
  }
}
