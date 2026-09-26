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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.service.client.BaseServiceClient;
import org.locationtech.geowave.service.rest.RestRoute;
import org.locationtech.geowave.service.rest.RestRoutes;
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

/** Checks the REST services' Swagger description and asynchronous operation status. */
@RunWith(GeoWaveITRunner.class)
@Environments({Environment.SERVICES})
public class RestApiIT extends BaseServiceIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(RestApiIT.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String testName = "RestApiIT";

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
  private static Client client;

  @BeforeClass
  public static void setup() {
    client = ClientBuilder.newClient();
    startMillis = System.currentTimeMillis();
    TestUtils.printStartOfTest(LOGGER, testName);
  }

  @AfterClass
  public static void reportTest() {
    client.close();
    TestUtils.printEndOfTest(LOGGER, testName, startMillis);
  }

  @Test
  public void swaggerDescribesEveryRoute() throws IOException {
    final Response response =
        client.target(ServicesTestEnvironment.GEOWAVE_BASE_URL).path("api").request(
            MediaType.APPLICATION_JSON).get();
    TestUtils.assertStatusCode("Should return the Swagger description", 200, response);
    final JsonNode swagger = MAPPER.readTree(response.readEntity(String.class));

    assertEquals("2.0", swagger.get("swagger").asText());
    assertEquals("localhost:" + ServicesTestEnvironment.REST_PORT, swagger.get("host").asText());
    assertEquals(ServicesTestEnvironment.GEOWAVE_CONTEXT_PATH, swagger.get("basePath").asText());

    final JsonNode paths = swagger.get("paths");
    final Set<String> routePaths = new HashSet<>();
    for (final RestRoute route : RestRoutes.find().list()) {
      final String path = "/" + route.getPath();
      routePaths.add(path);
      assertNotNull("Missing " + path, paths.get(path));
      assertNotNull(
          "Missing the method of " + path,
          paths.get(path).get(route.getOperation().getMethod().toString().toLowerCase()));
    }
    assertTrue(routePaths.contains("/v0/store/add/filesystem"));
    assertTrue(routePaths.contains("/v0/gs/ws/add"));
    // every route, plus file upload
    assertEquals(routePaths.size() + 1, paths.size());
    assertNotNull(paths.get("/v0/fileupload").get("post"));
  }

  @Test
  public void unknownOperationStatus() throws IOException {
    final Response response =
        new BaseServiceClient(ServicesTestEnvironment.GEOWAVE_BASE_URL).operation_status(
            "no-such-id");
    TestUtils.assertStatusCode("Should report an unknown operation", 200, response);
    final JsonNode status = MAPPER.readTree(response.readEntity(String.class));
    assertEquals("ERROR", status.get("status").asText());
    assertEquals("no operation found for ID: no-such-id", status.get("message").asText());
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStorePluginOptions;
  }
}
