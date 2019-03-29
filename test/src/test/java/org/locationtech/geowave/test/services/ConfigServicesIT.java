/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.services;

import static org.junit.Assert.assertEquals;
import javax.ws.rs.core.Response;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.service.client.ConfigServiceClient;
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
public class ConfigServicesIT extends BaseServiceIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigServicesIT.class);
  private static ConfigServiceClient configServiceClient;

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
  private static final String testName = "ConfigServicesIT";

  private final String storeName = "test-store-name";
  private final String spatialIndexName = "test-spatial-index-name";
  private final String spatialTemporalIndexName = "test-spatial-temporal-index-name";
  private final String indexGroupName = "test-indexGroup-name";

  @BeforeClass
  public static void setup() {
    configServiceClient = new ConfigServiceClient(ServicesTestEnvironment.GEOWAVE_BASE_URL);
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
    configServiceClient.removeStore(storeName);
    configServiceClient.removeIndex(spatialIndexName);
    configServiceClient.removeIndex(spatialTemporalIndexName);
    configServiceClient.removeIndexGroup(indexGroupName);
    configServiceClient.removeIndexGroup(indexGroupName + "-bad");
    unmuteLogging();
  }

  @Test
  public void testAddStoreReRoute() {
    TestUtils.assertStatusCode(
        "Should Create Store",
        201,
        configServiceClient.addStoreReRoute(
            storeName,
            dataStorePluginOptions.getType(),
            null,
            dataStorePluginOptions.getOptionsAsMap()));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should fail to create duplicate store",
        400,
        configServiceClient.addStoreReRoute(
            storeName,
            dataStorePluginOptions.getType(),
            null,
            dataStorePluginOptions.getOptionsAsMap()));
    unmuteLogging();
  }

  @Test
  public void testAddSpatialIndex() {

    final Response firstAdd = configServiceClient.addSpatialIndex(spatialIndexName);

    TestUtils.assertStatusCode("Should Create Spatial Index", 201, firstAdd);

    muteLogging();
    final Response secondAdd = configServiceClient.addSpatialIndex(spatialIndexName);
    unmuteLogging();

    TestUtils.assertStatusCode("Should fail to create duplicate index", 400, secondAdd);
  }

  @Test
  public void testAddSpatialTemporalIndex() {

    final Response firstAdd = configServiceClient.addSpatialTemporalIndex(spatialTemporalIndexName);

    TestUtils.assertStatusCode("Should Create Spatial Temporal Index", 201, firstAdd);

    muteLogging();
    final Response secondAdd =
        configServiceClient.addSpatialTemporalIndex(spatialTemporalIndexName);
    unmuteLogging();

    TestUtils.assertStatusCode("Should fail to create duplicate index", 400, secondAdd);
  }

  @Test
  public void testAddIndex() {
    final Response spatial = configServiceClient.addIndex(spatialIndexName, "spatial");

    TestUtils.assertStatusCode("Should Create Spatial Index", 201, spatial);

    final Response spatial_temporal =
        configServiceClient.addIndex(spatialTemporalIndexName, "spatial_temporal");

    TestUtils.assertStatusCode("Should Create Spatial Index", 201, spatial_temporal);
  }

  @Test
  public void testAddIndexGroup() {

    muteLogging();
    configServiceClient.addSpatialIndex("index1");
    configServiceClient.addSpatialIndex("index2");
    unmuteLogging();

    final String[] indexes = {"index1", "index2"};
    final Response firstAdd = configServiceClient.addIndexGroup(indexGroupName, indexes);

    TestUtils.assertStatusCode("Should Create Index Group", 201, firstAdd);

    muteLogging();
    final Response secondAdd = configServiceClient.addIndexGroup(indexGroupName, indexes);
    unmuteLogging();

    TestUtils.assertStatusCode("Should fail to create duplicate index group", 400, secondAdd);

    final String[] badIndexes = {"index1", "badIndex"};

    muteLogging();
    final Response thirdAdd =
        configServiceClient.addIndexGroup(indexGroupName + "-bad", badIndexes);
    unmuteLogging();

    TestUtils.assertStatusCode(
        "This should return 404, one of the indexes listed does not exist",
        404,
        thirdAdd);
  }

  @Test
  public void testRemoveStore() {
    configServiceClient.addStoreReRoute(
        "test_remove_store",
        dataStorePluginOptions.getType(),
        null,
        dataStorePluginOptions.getOptionsAsMap());

    final Response firstRemove = configServiceClient.removeStore("test_remove_store");
    TestUtils.assertStatusCode("Should Remove Store", 200, firstRemove);

    muteLogging();
    final Response secondRemove = configServiceClient.removeStore("test_remove_store");
    unmuteLogging();

    TestUtils.assertStatusCode(
        "This should return 404, that store does not exist",
        404,
        secondRemove);
  }

  @Test
  public void testRemoveIndex() {

    configServiceClient.addSpatialIndex("test_remove_index");

    final Response firstRemove = configServiceClient.removeIndex("test_remove_index");
    TestUtils.assertStatusCode("Should Remove Index", 200, firstRemove);

    muteLogging();
    final Response secondRemove = configServiceClient.removeIndex("test_remove_index");
    unmuteLogging();

    TestUtils.assertStatusCode(
        "This should return 404, that index does not exist",
        404,
        secondRemove);
  }

  @Test
  public void testRemoveIndexGroup() {

    muteLogging();
    configServiceClient.addSpatialIndex("index1");
    configServiceClient.addSpatialIndex("index2");
    unmuteLogging();

    final String[] indexes = {"index1", "index2"};
    configServiceClient.addIndexGroup("test_remove_index_group", indexes);

    final Response firstRemove = configServiceClient.removeIndexGroup("test_remove_index_group");
    TestUtils.assertStatusCode("Should Remove Index Group", 200, firstRemove);

    muteLogging();
    final Response secondRemove = configServiceClient.removeIndexGroup("test_remove_index_group");
    unmuteLogging();

    TestUtils.assertStatusCode(
        "This should return 404, that index group does not exist",
        404,
        secondRemove);
  }

  @Test
  public void testHdfsConfig() {
    // Should always return 200
    final Response config = configServiceClient.configHDFS("localhost:8020");
    TestUtils.assertStatusCode("Should Configure HDFS", 200, config);
  }

  @Test
  public void testSet() throws ParseException {
    // Should always return 200
    final Response set = configServiceClient.set("Property", "Value");
    TestUtils.assertStatusCode("Should Set Property", 200, set);
    final String list = configServiceClient.list().readEntity(String.class);
    final JSONParser parser = new JSONParser();
    final JSONObject json = (JSONObject) parser.parse(list);
    final JSONObject values = (JSONObject) json.get("data");

    // check to make sure that property was actually set
    assertEquals("The property was not set correctly", "Value", values.get("Property"));
  }

  @Test
  public void testList() {
    // Should always return 200
    final Response list = configServiceClient.list();
    TestUtils.assertStatusCode("Should Return List", 200, list);
  }

  @Test
  public void testConfigGeoServer() throws ParseException {
    // Should always return 200
    final Response configGeoserver = configServiceClient.configGeoServer("test-geoserver");
    TestUtils.assertStatusCode("Should Configure Geoserver", 200, configGeoserver);
    final String list = configServiceClient.list().readEntity(String.class);
    final JSONParser parser = new JSONParser();
    final JSONObject json = (JSONObject) parser.parse(list);
    final JSONObject values = (JSONObject) json.get("data");

    // check to make sure that geoserver was actually set
    assertEquals("GeoServer was not set correctly", "test-geoserver", values.get("geoserver.url"));
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStorePluginOptions;
  }
}
