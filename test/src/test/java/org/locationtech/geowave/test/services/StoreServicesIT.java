/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.services;

import java.util.List;
import javax.ws.rs.core.Response;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
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
public class StoreServicesIT extends BaseServiceIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(StoreServicesIT.class);
  private static StoreServiceClient storeServiceClient;

  @GeoWaveTestStore({
      GeoWaveStoreType.ACCUMULO,
      GeoWaveStoreType.BIGTABLE,
      GeoWaveStoreType.HBASE,
      GeoWaveStoreType.CASSANDRA,
      GeoWaveStoreType.DYNAMODB,
      GeoWaveStoreType.KUDU,
      GeoWaveStoreType.REDIS,
      GeoWaveStoreType.ROCKSDB})
  protected DataStorePluginOptions dataStorePluginOptions;

  private static long startMillis;
  private static final String testName = "StoreServicesIT";

  private final String storeName = "test-store-name";

  @BeforeClass
  public static void setup() {
    storeServiceClient = new StoreServiceClient(ServicesTestEnvironment.GEOWAVE_BASE_URL);
    startMillis = System.currentTimeMillis();
    TestUtils.printStartOfTest(LOGGER, testName);
  }

  @AfterClass
  public static void reportTest() {
    TestUtils.printEndOfTest(LOGGER, testName, startMillis);
  }

  @Before
  public void before() {
    muteLogging();
    // remove any Geowave objects that may interfere with tests.
    storeServiceClient.removeStore(storeName);
    unmuteLogging();
  }

  @Test
  public void testAddStoreReRoute() {
    TestUtils.assertStatusCode(
        "Should Create Store",
        201,
        storeServiceClient.addStoreReRoute(
            storeName,
            dataStorePluginOptions.getType(),
            null,
            dataStorePluginOptions.getOptionsAsMap()));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to create duplicate store",
        400,
        storeServiceClient.addStoreReRoute(
            storeName,
            dataStorePluginOptions.getType(),
            null,
            dataStorePluginOptions.getOptionsAsMap()));
    unmuteLogging();
  }

  @Test
  public void testRemoveStore() {
    storeServiceClient.addStoreReRoute(
        "test_remove_store",
        dataStorePluginOptions.getType(),
        null,
        dataStorePluginOptions.getOptionsAsMap());

    final Response firstRemove = storeServiceClient.removeStore("test_remove_store");
    TestUtils.assertStatusCode("Should Remove Store", 200, firstRemove);

    muteLogging();
    final Response secondRemove = storeServiceClient.removeStore("test_remove_store");
    unmuteLogging();

    TestUtils.assertStatusCode(
        "This should return 404, that store does not exist",
        404,
        secondRemove);
  }

  @Test
  public void testClear() {
    storeServiceClient.addStoreReRoute(
        storeName,
        dataStorePluginOptions.getType(),
        null,
        dataStorePluginOptions.getOptionsAsMap());

    TestUtils.assertStatusCode(
        "Should successfully clear for existent store",
        200,
        storeServiceClient.clear(storeName));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to clear for nonexistent store",
        400,
        storeServiceClient.clear("nonexistent-store"));
    unmuteLogging();
  }

  @Test
  public void testListTypes() {
    storeServiceClient.addStoreReRoute(
        storeName,
        dataStorePluginOptions.getType(),
        null,
        dataStorePluginOptions.getOptionsAsMap());

    TestUtils.assertStatusCode(
        "Should successfully list types for existent store",
        200,
        storeServiceClient.listTypes(storeName));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to list types for nonexistent store",
        400,
        storeServiceClient.listTypes("nonexistent-store"));
    unmuteLogging();
  }

  @Test
  public void testRemoveType() {
    // Add data
    final DataStore ds = dataStorePluginOptions.createDataStore();
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
        storeName,
        dataStorePluginOptions.getType(),
        dataStorePluginOptions.getGeoWaveNamespace(),
        dataStorePluginOptions.getOptionsAsMap());

    TestUtils.assertStatusCode(
        "Should successfully remove adapter for existent store and existent type",
        200,
        storeServiceClient.removeType(storeName, "GridPoint"));

    // The following case should probably return a 404 based on the
    // situation described in the test description
    TestUtils.assertStatusCode(
        "Returns a successful 200 status for removing type for existent store and previously removed type.  A warning is output",
        200,
        storeServiceClient.removeType(storeName, "GridPoint"));

    // The following case should probably return a 404 based on the
    // situation described in the test description
    TestUtils.assertStatusCode(
        "Returns a successful 200 status for removing type for existent store and nonexistent type.  A warning is output",
        200,
        storeServiceClient.removeType(storeName, "nonexistent-adapter"));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to remove type for nonexistent store",
        400,
        storeServiceClient.removeType("nonexistent-store", "GridPoint"));
    unmuteLogging();
  }

  @Test
  public void testVersion() {
    storeServiceClient.addStoreReRoute(
        storeName,
        dataStorePluginOptions.getType(),
        null,
        dataStorePluginOptions.getOptionsAsMap());

    TestUtils.assertStatusCode(
        "Should successfully return version for existent store",
        200,
        storeServiceClient.version(storeName));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to return version for nonexistent store",
        400,
        storeServiceClient.version("nonexistent-store"));
    unmuteLogging();
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStorePluginOptions;
  }
}
