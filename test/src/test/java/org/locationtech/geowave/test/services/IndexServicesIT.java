/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.services;

import javax.ws.rs.core.Response;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.service.client.IndexServiceClient;
import org.locationtech.geowave.service.client.StoreServiceClient;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
@Environments({Environment.SERVICES})
public class IndexServicesIT extends BaseServiceIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexServicesIT.class);
  private static IndexServiceClient indexServiceClient;
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
  private static final String testName = "IndexServicesIT";

  private final String storeName = "test-store-name";
  private final String spatialIndexName = "testSpatialIndexName";
  private final String spatialTemporalIndexName = "testSpatialTemporalIndexName";

  @BeforeClass
  public static void setup() {
    indexServiceClient = new IndexServiceClient(ServicesTestEnvironment.GEOWAVE_BASE_URL);
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
    indexServiceClient.removeIndex(storeName, spatialIndexName);
    indexServiceClient.removeIndex(storeName, spatialTemporalIndexName);
    storeServiceClient.removeStore(storeName);
    storeServiceClient.addStoreReRoute(
        storeName,
        dataStorePluginOptions.getType(),
        null,
        dataStorePluginOptions.getOptionsAsMap());
    unmuteLogging();
  }

  @Test
  public void listplugins() {
    // should always return 200
    TestUtils.assertStatusCode(
        "Should successfully list plugins",
        200,
        indexServiceClient.listPlugins());
  }

  @Test
  public void testAddSpatialIndex() {

    final Response firstAdd = indexServiceClient.addSpatialIndex(storeName, spatialIndexName);

    TestUtils.assertStatusCode("Should Create Spatial Index", 201, firstAdd);

    muteLogging();
    final Response secondAdd = indexServiceClient.addSpatialIndex(storeName, spatialIndexName);
    unmuteLogging();

    TestUtils.assertStatusCode("Should fail to create duplicate index", 400, secondAdd);
  }

  @Test
  public void testAddSpatialTemporalIndex() {

    final Response firstAdd =
        indexServiceClient.addSpatialTemporalIndex(storeName, spatialTemporalIndexName);

    TestUtils.assertStatusCode("Should Create Spatial Temporal Index", 201, firstAdd);

    muteLogging();
    final Response secondAdd =
        indexServiceClient.addSpatialTemporalIndex(storeName, spatialTemporalIndexName);
    unmuteLogging();

    TestUtils.assertStatusCode("Should fail to create duplicate index", 400, secondAdd);
  }

  @Test
  public void testRemoveIndex() {

    indexServiceClient.addSpatialIndex(storeName, "test_remove_index");

    final Response firstRemove = indexServiceClient.removeIndex(storeName, "test_remove_index");
    TestUtils.assertStatusCode("Should Remove Index", 200, firstRemove);

    muteLogging();
    final Response secondRemove = indexServiceClient.removeIndex(storeName, "test_remove_index");
    unmuteLogging();

    TestUtils.assertStatusCode(
        "This should return 404, that index does not exist",
        404,
        secondRemove);
  }

  @Test
  public void testListIndex() {
    TestUtils.assertStatusCode(
        "Should successfully list indices for existent store",
        200,
        indexServiceClient.listIndices(storeName));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to list indices for nonexistent store",
        400,
        indexServiceClient.listIndices("nonexistent-store"));
    unmuteLogging();
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStorePluginOptions;
  }
}
