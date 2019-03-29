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
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
import org.locationtech.geowave.service.client.ConfigServiceClient;
import org.locationtech.geowave.service.client.RemoteServiceClient;
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
public class RemoteIT extends BaseServiceIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteIT.class);
  private static ConfigServiceClient configServiceClient;
  private RemoteServiceClient remoteServiceClient;

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
    remoteServiceClient = new RemoteServiceClient(ServicesTestEnvironment.GEOWAVE_BASE_URL);
    configServiceClient = new ConfigServiceClient(ServicesTestEnvironment.GEOWAVE_BASE_URL);

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
    configServiceClient.addStoreReRoute(
        store_name,
        dataStoreOptions.getType(),
        dataStoreOptions.getGeoWaveNamespace(),
        dataStoreOptions.getOptionsAsMap());
  }

  @After
  public void cleanupWorkspace() {
    configServiceClient.removeStore(store_name);
    TestUtils.deleteAll(dataStoreOptions);
  }

  @Test
  public void testCalcStat() {
    TestUtils.assertStatusCode(
        "Should successfully calculate stat for existent store, adapterID, and statID",
        200,
        remoteServiceClient.calcStat(store_name, "GridPoint", "COUNT_DATA"));

    // The following case should probably return a 404 based on the
    // situation described in the test description
    TestUtils.assertStatusCode(
        "Returns a successful 200 status for calculate stat for existent store and adapterID, but a nonexistent statID.  A warning is output",
        200,
        remoteServiceClient.calcStat(store_name, "GridPoint", "nonexistent-stat"));

    // The following case should probably return a 404 based on the
    // situation described in the test description
    TestUtils.assertStatusCode(
        "Returns a successful 200 status for calculate stat for existent store and statID, but nonexistent adapterID.  A warning is output",
        200,
        remoteServiceClient.calcStat(store_name, "nonexistent-adapter", "COUNT_DATA"));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to calculate stat for existent adapterID and statID, but nonexistent store",
        400,
        remoteServiceClient.calcStat("nonexistent-store", "GridPoint", "COUNT_DATA"));
    unmuteLogging();
  }

  @Test
  public void testClear() {
    TestUtils.assertStatusCode(
        "Should successfully clear for existent store",
        200,
        remoteServiceClient.clear(store_name));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to clear for nonexistent store",
        400,
        remoteServiceClient.clear("nonexistent-store"));
    unmuteLogging();
  }

  @Test
  public void testListTypes() {
    TestUtils.assertStatusCode(
        "Should successfully list types for existent store",
        200,
        remoteServiceClient.listTypes(store_name));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to list types for nonexistent store",
        400,
        remoteServiceClient.listTypes("nonexistent-store"));
    unmuteLogging();
  }

  @Test
  public void testListIndex() {
    TestUtils.assertStatusCode(
        "Should successfully list indices for existent store",
        200,
        remoteServiceClient.listIndices(store_name));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to list indices for nonexistent store",
        400,
        remoteServiceClient.listIndices("nonexistent-store"));
    unmuteLogging();
  }

  @Test
  public void testListStats() {
    TestUtils.assertStatusCode(
        "Should successfully liststats for existent store",
        200,
        remoteServiceClient.listStats(store_name));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to liststats for nonexistent store",
        400,
        remoteServiceClient.listStats("nonexistent-store"));
    unmuteLogging();
  }

  @Test
  public void testRecalcStats() {
    TestUtils.assertStatusCode(
        "Should successfully recalc stats for existent store",
        200,
        remoteServiceClient.recalcStats(store_name));

    TestUtils.assertStatusCode(
        "Should successfully recalc stats for existent store and existent adapter",
        200,
        remoteServiceClient.recalcStats(store_name, "GridPoint", null, null));

    // The following case should probably return a 404 based on the
    // situation described in the test description
    TestUtils.assertStatusCode(
        "Returns a successful 200 status for recalc stats for existent store and nonexistent adapter",
        200,
        remoteServiceClient.recalcStats(store_name, "nonexistent-adapter", null, null));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to recalc stats for nonexistent store",
        400,
        remoteServiceClient.recalcStats("nonexistent-store"));
    unmuteLogging();
  }

  @Test
  public void testRemoveType() {
    TestUtils.assertStatusCode(
        "Should successfully remove adapter for existent store and existent type",
        200,
        remoteServiceClient.removeType(store_name, "GridPoint"));

    // The following case should probably return a 404 based on the
    // situation described in the test description
    TestUtils.assertStatusCode(
        "Returns a successful 200 status for removing type for existent store and previously removed type.  A warning is output",
        200,
        remoteServiceClient.removeType(store_name, "GridPoint"));

    // The following case should probably return a 404 based on the
    // situation described in the test description
    TestUtils.assertStatusCode(
        "Returns a successful 200 status for removing type for existent store and nonexistent type.  A warning is output",
        200,
        remoteServiceClient.removeType(store_name, "nonexistent-adapter"));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to remove type for nonexistent store",
        400,
        remoteServiceClient.removeType("nonexistent-store", "GridPoint"));
    unmuteLogging();
  }

  @Test
  public void testRemoveStat() {
    TestUtils.assertStatusCode(
        "Should successfully remove stat for existent store, adapterID, and statID",
        200,
        remoteServiceClient.removeStat(store_name, "GridPoint", "COUNT_DATA"));

    // The following case should probably return a 404 based on the
    // situation described in the test description
    TestUtils.assertStatusCode(
        "Returns a successful 200 status for removing stat for existent store and type name, but a nonexistent stat type.  A warning is output.",
        200,
        remoteServiceClient.removeStat(store_name, "GridPoint", "nonexistent-stat"));

    // The following case should probably return a 404 based on the
    // situation described in the test description
    TestUtils.assertStatusCode(
        "Returns a successful 200 status for removing stat for existent store and stat type, but nonexistent type name.  A warning is output",
        200,
        remoteServiceClient.removeStat(store_name, "nonexistent-type", "COUNT_DATA"));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to remove for existent data type name and stat type, but nonexistent store",
        400,
        remoteServiceClient.removeStat("nonexistent-store", "GridPoint", "COUNT_DATA"));
    unmuteLogging();
  }

  @Test
  public void testVersion() {
    TestUtils.assertStatusCode(
        "Should successfully return version for existent store",
        200,
        remoteServiceClient.version(store_name));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to return version for nonexistent store",
        400,
        remoteServiceClient.version("nonexistent-store"));
    unmuteLogging();
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStoreOptions;
  }
}
