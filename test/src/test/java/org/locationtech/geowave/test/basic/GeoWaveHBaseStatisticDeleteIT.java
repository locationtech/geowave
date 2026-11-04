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
import java.io.IOException;
import java.util.function.BiConsumer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.metadata.AbstractGeoWavePersistence;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.core.store.statistics.binning.FieldValueBinningStrategy;
import org.locationtech.geowave.datastore.hbase.operations.HBaseOperations;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

/**
 * Statistic values deleted and then rewritten must stay deleted after the memstore is flushed and
 * the store files compacted, which is when HBase server-side merging runs.
 */
@RunWith(GeoWaveITRunner.class)
public class GeoWaveHBaseStatisticDeleteIT {
  @GeoWaveTestStore(value = {GeoWaveStoreType.HBASE})
  protected DataStorePluginOptions dataStore;

  private static final ByteArray BIN = new ByteArray("h0001");

  private DataStore ds;
  private HBaseOperations operations;

  @Before
  public void initialize() {
    ds = dataStore.createDataStore();
    operations = (HBaseOperations) dataStore.createDataStoreOperations();
    ds.addType(
        SimpleIngest.createDataAdapter(SimpleIngest.createPointFeatureType()),
        SimpleIngest.createSpatialIndex());
  }

  @After
  public void cleanupWorkspace() {
    TestUtils.deleteAll(dataStore);
  }

  @Test
  public void testExactBinDeleteSurvivesFlushAndCompaction() throws Exception {
    assertDeleteSurvives("exact", (statistics, stat) -> statistics.removeStatisticValue(stat, BIN));
  }

  @Test
  public void testAllBinsDeleteSurvivesFlushAndCompaction() throws Exception {
    assertDeleteSurvives("all", DataStatisticsStore::removeStatisticValues);
  }

  @Test
  public void testDeleteBeforeAnyFlush() throws Exception {
    final CountStatistic stat = addStatistic("unflushed");
    final DataStatisticsStore statistics = dataStore.createDataStatisticsStore();
    ds.incorporateStatisticValue(stat, count(5), BIN);
    statistics.removeStatisticValue(stat, BIN);
    ds.incorporateStatisticValue(stat, count(3), BIN);
    flush();
    assertEquals(3, read(stat));
    majorCompact();
    assertEquals(3, read(stat));
  }

  @Test
  public void testDeleteSurvivesFlushWithScannerOpen() throws Exception {
    final CountStatistic stat = addStatistic("openFlush");
    final DataStatisticsStore statistics = dataStore.createDataStatisticsStore();
    ds.incorporateStatisticValue(stat, count(5), BIN);
    try (Table table = operations.getConnection().getTable(statisticValuesTable());
        ResultScanner scanner = openScanner(table)) {
      statistics.removeStatisticValue(stat, BIN);
      assertEquals(0, read(stat));
      ds.incorporateStatisticValue(stat, count(3), BIN);
      flush();
      assertEquals(3, read(stat));
    }
    majorCompact();
    assertEquals(3, read(stat));
  }

  @Test
  public void testDeleteSurvivesMajorCompactionWithScannerOpen() throws Exception {
    final CountStatistic stat = addStatistic("openCompaction");
    final DataStatisticsStore statistics = dataStore.createDataStatisticsStore();
    ds.incorporateStatisticValue(stat, count(5), BIN);
    flush();
    try (Table table = operations.getConnection().getTable(statisticValuesTable());
        ResultScanner scanner = openScanner(table)) {
      statistics.removeStatisticValue(stat, BIN);
      assertEquals(0, read(stat));
      ds.incorporateStatisticValue(stat, count(3), BIN);
      flush();
      majorCompact();
      assertEquals(3, read(stat));
    }
    majorCompact();
    assertEquals(3, read(stat));
  }

  @Test
  public void testRowDeleteSurvivesFlushOfLongRowWithScannerOpen() throws Exception {
    final CountStatistic stat = addStatistic("openLongRow");
    ds.incorporateStatisticValue(stat, count(5), BIN);
    try (Table table = operations.getConnection().getTable(statisticValuesTable());
        ResultScanner scanner = openScanner(table)) {
      // a row delete, as GeoWave deletes data rows, writes a family marker that sorts first in the
      // row; 11 rewrites push the masked Put(5) past the 10 cells a flush hands over at a time
      table.delete(new Delete(StatisticValue.getValueId(stat.getId(), BIN)));
      assertEquals(0, read(stat));
      for (int i = 0; i < 11; i++) {
        // distinct timestamps, or HBase keeps only one of the Puts
        Thread.sleep(2);
        ds.incorporateStatisticValue(stat, count(1), BIN);
      }
      flush();
      assertEquals(11, read(stat));
    }
    majorCompact();
    assertEquals(11, read(stat));
  }

  /**
   * Starts a scan of the statistic values table and leaves it unfinished, as a large scan read
   * partway would. While it is open, a flush or compaction keeps a newer delete marker without
   * applying it and hands the Puts it masks to server-side merging.
   */
  private static ResultScanner openScanner(final Table table) throws IOException {
    final Scan scan = new Scan();
    // one row per RPC, so the region server keeps the scan open
    scan.setCaching(1);
    final ResultScanner scanner = table.getScanner(scan);
    scanner.next();
    return scanner;
  }

  private void assertDeleteSurvives(
      final String tag,
      final BiConsumer<DataStatisticsStore, CountStatistic> delete) throws Exception {
    final CountStatistic stat = addStatistic(tag);
    final DataStatisticsStore statistics = dataStore.createDataStatisticsStore();
    ds.incorporateStatisticValue(stat, count(5), BIN);
    flush();
    assertEquals(5, read(stat));

    delete.accept(statistics, stat);
    assertEquals(0, read(stat));
    // the delete marker is now in the memstore and the deleted value in a store file
    ds.incorporateStatisticValue(stat, count(3), BIN);
    assertEquals(3, read(stat));

    flush();
    assertEquals(3, read(stat));
    majorCompact();
    assertEquals(3, read(stat));
  }

  private CountStatistic addStatistic(final String tag) {
    final CountStatistic stat = new CountStatistic(SimpleIngest.FEATURE_NAME);
    stat.setTag(tag);
    stat.setBinningStrategy(new FieldValueBinningStrategy("Comment"));
    ds.addStatistic(stat);
    return stat;
  }

  private static CountValue count(final long count) {
    final CountValue value = new CountValue();
    value.fromBinary(VarintUtils.writeSignedLong(count));
    return value;
  }

  private long read(final CountStatistic stat) {
    long sum = 0;
    try (CloseableIterator<Pair<ByteArray, Long>> it =
        ds.getBinnedStatisticValues(stat, BinConstraints.of(BIN))) {
      while (it.hasNext()) {
        sum += it.next().getValue();
      }
    }
    return sum;
  }

  private TableName statisticValuesTable() {
    return operations.getTableName(AbstractGeoWavePersistence.METADATA_TABLE);
  }

  private void flush() throws IOException {
    try (Admin admin = operations.getConnection().getAdmin()) {
      admin.flush(statisticValuesTable());
    }
  }

  private void majorCompact() throws IOException, InterruptedException {
    try (Admin admin = operations.getConnection().getAdmin()) {
      admin.majorCompact(statisticValuesTable());
      // the request is asynchronous; wait for it to start and then to finish
      final long deadline = System.currentTimeMillis() + 60_000;
      Thread.sleep(500);
      while ((admin.getCompactionState(statisticValuesTable()) != CompactionState.NONE)
          && (System.currentTimeMillis() < deadline)) {
        Thread.sleep(250);
      }
    }
  }
}
