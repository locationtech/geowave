/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.List;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic.BoundingBoxValue;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.StatisticQueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
import org.locationtech.geowave.service.client.StatServiceClient;
import org.locationtech.geowave.service.client.StoreServiceClient;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
@Environments({Environment.SERVICES})
public class StatServicesIT extends BaseServiceIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(StatServicesIT.class);
  private static StoreServiceClient storeServiceClient;
  private static StatServiceClient statServiceClient;

  private final String store_name = "test_store";

  private static final String testName = "StatServicesIT";

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB,
          GeoWaveStoreType.FILESYSTEM},
      namespace = TestUtils.TEST_NAMESPACE)
  protected DataStorePluginOptions dataStoreOptions;

  private static long startMillis;

  @BeforeClass
  public static void startTimer() {
    startMillis = System.currentTimeMillis();
    TestUtils.printStartOfTest(LOGGER, testName);
  }

  @AfterClass
  public static void reportTest() {
    TestUtils.printEndOfTest(LOGGER, testName, startMillis);
  }

  @Before
  public void initialize() throws IOException {
    statServiceClient = new StatServiceClient(ServicesTestEnvironment.GEOWAVE_BASE_URL);
    storeServiceClient = new StoreServiceClient(ServicesTestEnvironment.GEOWAVE_BASE_URL);

    final DataStore ds = dataStoreOptions.createDataStore();
    final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();
    final Index idx = SimpleIngest.createSpatialIndex();
    final GeotoolsFeatureDataAdapter fda = SimpleIngest.createDataAdapter(sft);
    final List<SimpleFeature> features =
        SimpleIngest.getGriddedFeatures(new SimpleFeatureBuilder(sft), 8675309);
    LOGGER.info(
        String.format("Beginning to ingest a uniform grid of %d features", features.size()));
    int ingestedFeatures = 0;
    final int featuresPer5Percent = features.size() / 20;
    ds.addType(fda, idx);

    try (Writer writer = ds.createWriter(fda.getTypeName())) {
      for (final SimpleFeature feat : features) {
        ingestedFeatures++;
        if ((ingestedFeatures % featuresPer5Percent) == 0) {
          // just write 5 percent of the grid
          writer.write(feat);
        }
      }
    }
    storeServiceClient.addStoreReRoute(
        store_name,
        dataStoreOptions.getType(),
        dataStoreOptions.getGeoWaveNamespace(),
        dataStoreOptions.getOptionsAsMap());
  }

  @After
  public void cleanupWorkspace() {
    storeServiceClient.removeStore(store_name);
    TestUtils.deleteAll(dataStoreOptions);
  }

  @Test
  public void testListStats() {
    TestUtils.assertStatusCode(
        "Should successfully liststats for existent store",
        200,
        statServiceClient.listStats(store_name));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to liststats for nonexistent store",
        400,
        statServiceClient.listStats("nonexistent-store"));
    unmuteLogging();
  }

  @Test
  public void testRecalcStats() {
    final DataStore ds = dataStoreOptions.createDataStore();
    final CountValue expectedCount;
    final BoundingBoxValue expectedBoundingBox;
    try (CloseableIterator<CountValue> iter =
        ds.queryStatistics(
            StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).typeName(
                SimpleIngest.FEATURE_NAME).build())) {
      assertTrue(iter.hasNext());
      expectedCount = iter.next();
      assertFalse(iter.hasNext());
    }

    try (CloseableIterator<BoundingBoxValue> iter =
        ds.queryStatistics(
            StatisticQueryBuilder.newBuilder(BoundingBoxStatistic.STATS_TYPE).typeName(
                SimpleIngest.FEATURE_NAME).build())) {
      assertTrue(iter.hasNext());
      expectedBoundingBox = iter.next();
      assertFalse(iter.hasNext());
    }

    TestUtils.assertStatusCode(
        "Should successfully recalc stats for existent store",
        200,
        statServiceClient.recalcStats(store_name));

    // Verify that the statistic values are still correct
    try (CloseableIterator<CountValue> iter =
        ds.queryStatistics(
            StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).typeName(
                SimpleIngest.FEATURE_NAME).build())) {
      assertTrue(iter.hasNext());
      final CountValue newCount = iter.next();
      assertFalse(iter.hasNext());
      assertEquals(expectedCount.getValue(), newCount.getValue());
    }

    try (CloseableIterator<BoundingBoxValue> iter =
        ds.queryStatistics(
            StatisticQueryBuilder.newBuilder(BoundingBoxStatistic.STATS_TYPE).typeName(
                SimpleIngest.FEATURE_NAME).build())) {
      assertTrue(iter.hasNext());
      final BoundingBoxValue newBoundingBox = iter.next();
      assertFalse(iter.hasNext());
      assertEquals(expectedBoundingBox.getValue(), newBoundingBox.getValue());
    }

    TestUtils.assertStatusCode(
        "Should successfully recalc stats for existent store and existent adapter",
        200,
        statServiceClient.recalcStats(
            store_name,
            null,
            null,
            SimpleIngest.FEATURE_NAME,
            null,
            null,
            true,
            null));

    // Verify that the statistic values are still correct
    try (CloseableIterator<CountValue> iter =
        ds.queryStatistics(
            StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).typeName(
                SimpleIngest.FEATURE_NAME).build())) {
      assertTrue(iter.hasNext());
      final CountValue newCount = iter.next();
      assertFalse(iter.hasNext());
      assertEquals(expectedCount.getValue(), newCount.getValue());
    }

    try (CloseableIterator<BoundingBoxValue> iter =
        ds.queryStatistics(
            StatisticQueryBuilder.newBuilder(BoundingBoxStatistic.STATS_TYPE).typeName(
                SimpleIngest.FEATURE_NAME).build())) {
      assertTrue(iter.hasNext());
      final BoundingBoxValue newBoundingBox = iter.next();
      assertFalse(iter.hasNext());
      assertEquals(expectedBoundingBox.getValue(), newBoundingBox.getValue());
    }

    // The following case should probably return a 404 based on the
    // situation described in the test description
    TestUtils.assertStatusCode(
        "Returns a 400 status for recalc stats for existent store and nonexistent adapter",
        400,
        statServiceClient.recalcStats(
            store_name,
            null,
            null,
            "nonexistent-adapter",
            null,
            null,
            true,
            null));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to recalc stats for nonexistent store",
        400,
        statServiceClient.recalcStats("nonexistent-store"));
    unmuteLogging();
  }

  @Test
  public void testRemoveStat() {
    TestUtils.assertStatusCode(
        "Should successfully remove stat for existent store, adapterID, and statID",
        200,
        statServiceClient.removeStat(store_name, "COUNT", "GridPoint", true));

    // The following case should probably return a 404 based on the
    // situation described in the test description
    TestUtils.assertStatusCode(
        "Should fail to remove a nonexistent stat.",
        400,
        statServiceClient.removeStat(store_name, "nonexistent-stat", "GridPoint", true));

    // The following case should probably return a 404 based on the
    // situation described in the test description
    TestUtils.assertStatusCode(
        "Should fail to remove a stat from a nonexistent type.",
        400,
        statServiceClient.removeStat(store_name, "COUNT", "nonexistent-type", true));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to remove for existent data type name and stat type, but nonexistent store",
        400,
        statServiceClient.removeStat("nonexistent-store", "COUNT", "GridPoint", true));
    unmuteLogging();
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStoreOptions;
  }
}
