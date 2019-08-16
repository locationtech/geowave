/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.services;

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
import org.locationtech.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
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

  private static final String testName = "RemoteIT";

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB},
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
  public void initialize() throws MismatchedIndexToAdapterMapping, IOException {
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
  public void testCalcStat() {
    TestUtils.assertStatusCode(
        "Should successfully calculate stat for existent store, adapterID, and statID",
        200,
        statServiceClient.calcStat(store_name, "GridPoint", "COUNT_DATA"));

    // The following case should probably return a 404 based on the
    // situation described in the test description
    TestUtils.assertStatusCode(
        "Returns a successful 200 status for calculate stat for existent store and adapterID, but a nonexistent statID.  A warning is output",
        200,
        statServiceClient.calcStat(store_name, "GridPoint", "nonexistent-stat"));

    // The following case should probably return a 404 based on the
    // situation described in the test description
    TestUtils.assertStatusCode(
        "Returns a successful 200 status for calculate stat for existent store and statID, but nonexistent adapterID.  A warning is output",
        200,
        statServiceClient.calcStat(store_name, "nonexistent-adapter", "COUNT_DATA"));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to calculate stat for existent adapterID and statID, but nonexistent store",
        400,
        statServiceClient.calcStat("nonexistent-store", "GridPoint", "COUNT_DATA"));
    unmuteLogging();
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
    TestUtils.assertStatusCode(
        "Should successfully recalc stats for existent store",
        200,
        statServiceClient.recalcStats(store_name));

    TestUtils.assertStatusCode(
        "Should successfully recalc stats for existent store and existent adapter",
        200,
        statServiceClient.recalcStats(store_name, "GridPoint", null, null));

    // The following case should probably return a 404 based on the
    // situation described in the test description
    TestUtils.assertStatusCode(
        "Returns a successful 200 status for recalc stats for existent store and nonexistent adapter",
        200,
        statServiceClient.recalcStats(store_name, "nonexistent-adapter", null, null));

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
        statServiceClient.removeStat(store_name, "GridPoint", "COUNT_DATA"));

    // The following case should probably return a 404 based on the
    // situation described in the test description
    TestUtils.assertStatusCode(
        "Returns a successful 200 status for removing stat for existent store and type name, but a nonexistent stat type.  A warning is output.",
        200,
        statServiceClient.removeStat(store_name, "GridPoint", "nonexistent-stat"));

    // The following case should probably return a 404 based on the
    // situation described in the test description
    TestUtils.assertStatusCode(
        "Returns a successful 200 status for removing stat for existent store and stat type, but nonexistent type name.  A warning is output",
        200,
        statServiceClient.removeStat(store_name, "nonexistent-type", "COUNT_DATA"));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to remove for existent data type name and stat type, but nonexistent store",
        400,
        statServiceClient.removeStat("nonexistent-store", "GridPoint", "COUNT_DATA"));
    unmuteLogging();
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStoreOptions;
  }
}
