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
import java.io.IOException;
import jakarta.ws.rs.core.Response;
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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@RunWith(GeoWaveITRunner.class)
@Environments({Environment.SERVICES})
public class ConfigServicesIT extends BaseServiceIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigServicesIT.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static ConfigServiceClient configServiceClient;

  @GeoWaveTestStore({
      GeoWaveStoreType.ACCUMULO,
      GeoWaveStoreType.HBASE,
      GeoWaveStoreType.CASSANDRA,
      GeoWaveStoreType.DYNAMODB,
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
  public void testSet() throws IOException {
    // Should always return 200
    final Response set = configServiceClient.set("Property", "Value");
    TestUtils.assertStatusCode("Should Set Property", 200, set);
    final String list = configServiceClient.list().readEntity(String.class);
    final JsonNode values = MAPPER.readTree(list).get("data");

    // check to make sure that property was actually set
    assertEquals("The property was not set correctly", "Value", values.get("Property").asText());
  }

  @Test
  public void testList() {
    // Should always return 200
    final Response list = configServiceClient.list();
    TestUtils.assertStatusCode("Should Return List", 200, list);
  }

  @Test
  public void testConfigGeoServer() throws IOException {
    // Should always return 200
    final Response configGeoserver = configServiceClient.configGeoServer("test-geoserver");
    TestUtils.assertStatusCode("Should Configure Geoserver", 200, configGeoserver);
    final String list = configServiceClient.list().readEntity(String.class);
    final JsonNode values = MAPPER.readTree(list).get("data");

    // check to make sure that geoserver was actually set
    assertEquals(
        "GeoServer was not set correctly",
        "test-geoserver",
        values.get("geoserver.url").asText());
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStorePluginOptions;
  }
}
