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
import javax.ws.rs.core.Response;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
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
      GeoWaveStoreType.ROCKSDB,
      GeoWaveStoreType.FILESYSTEM})
  protected DataStorePluginOptions dataStorePluginOptions;

  private static long startMillis;
  private static final String testName = "ConfigServicesIT";

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
