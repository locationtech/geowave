/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.services;

import java.io.File;
import java.net.URISyntaxException;
import javax.ws.rs.core.Response;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.raster.util.ZipUtils;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.service.client.BaseServiceClient;
import org.locationtech.geowave.service.client.ConfigServiceClient;
import org.locationtech.geowave.service.client.IngestServiceClient;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.ZookeeperTestEnvironment;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.mapreduce.MapReduceTestEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
@Environments({Environment.SERVICES})
public class IngestIT extends BaseServiceIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(IngestIT.class);

  private static final String TEST_MAPREDUCE_DATA_ZIP_RESOURCE_PATH =
      TestUtils.TEST_RESOURCE_PACKAGE + "mapreduce-testdata.zip";
  protected static final String OSM_GPX_INPUT_DIR = TestUtils.TEST_CASE_BASE + "osm_gpx_test_case/";

  private static IngestServiceClient ingestServiceClient;
  private static ConfigServiceClient configServiceClient;
  private static BaseServiceClient baseServiceClient;

  private final String storeName = "existent-store";
  private final String spatialIndex = "spatial-index";
  private static JSONParser parser;

  private static final String testName = "IngestIT";

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB})
  protected DataStorePluginOptions dataStoreOptions;

  private static long startMillis;

  @BeforeClass
  public static void startTimer() {
    startMillis = System.currentTimeMillis();
    TestUtils.printStartOfTest(LOGGER, testName);
    configServiceClient = new ConfigServiceClient(ServicesTestEnvironment.GEOWAVE_BASE_URL);
    ingestServiceClient = new IngestServiceClient(ServicesTestEnvironment.GEOWAVE_BASE_URL);
    baseServiceClient = new BaseServiceClient(ServicesTestEnvironment.GEOWAVE_BASE_URL);
    parser = new JSONParser();

    try {
      extractTestFiles();
    } catch (final URISyntaxException e) {
      LOGGER.error("Error encountered extracting test files.", e.getMessage());
    }
  }

  public static void extractTestFiles() throws URISyntaxException {
    ZipUtils.unZipFile(
        new File(
            MapReduceTestEnvironment.class.getClassLoader().getResource(
                TEST_MAPREDUCE_DATA_ZIP_RESOURCE_PATH).toURI()),
        TestUtils.TEST_CASE_BASE);
  }

  @AfterClass
  public static void reportTest() {
    TestUtils.printEndOfTest(LOGGER, testName, startMillis);
  }

  @Before
  public void initialize() {
    configServiceClient.addStoreReRoute(
        storeName,
        dataStoreOptions.getType(),
        null,
        dataStoreOptions.getOptionsAsMap());
    configServiceClient.addSpatialIndex(spatialIndex);
    configServiceClient.configHDFS(MapReduceTestEnvironment.getInstance().getHdfs());
  }

  @After
  public void cleanupWorkspace() {
    configServiceClient.removeStore(storeName);
    configServiceClient.removeIndex(spatialIndex);
  }

  public static void assertFinalIngestStatus(
      final String msg,
      final String expectedStatus,
      Response r,
      final int sleepTime /* in milliseconds */) {

    JSONObject json = null;
    String operationID = null;
    String status = null;

    try {
      json = (JSONObject) parser.parse(r.readEntity(String.class));
      status = (String) (json.get("status"));
      if (!status.equals("STARTED")) {
        Assert.assertTrue(msg, status.equals(expectedStatus));
        return;
      }
      operationID = (String) (json.get("data"));
    } catch (final ParseException e) {
      Assert.fail("Error occurred while parsing JSON response: '" + e.getMessage() + "'");
    }

    if (operationID != null) {
      try {
        while (true) {
          r = baseServiceClient.operation_status(operationID);
          if (r.getStatus() != 200) {
            Assert.fail("Entered an error handling a request.");
          }
          try {
            json = (JSONObject) parser.parse(r.readEntity(String.class));
            status = (String) (json.get("status"));
          } catch (final ParseException e) {
            Assert.fail("Entered an error while parsing JSON response: '" + e.getMessage() + "'");
          }

          if (!status.equals("RUNNING")) {
            Assert.assertTrue(msg, status.equals(expectedStatus));
            return;
          }

          Thread.sleep(sleepTime);
        }
      } catch (final InterruptedException e) {
        LOGGER.warn("Ingest interrupted.");
      }
    }
  }

  // Combined testing of localToKafka and kafkaToGW into one test as the
  // latter requires the former to test
  @Test
  public void localToKafkaToGW() {
    Response r = ingestServiceClient.localToKafka(OSM_GPX_INPUT_DIR);
    assertFinalIngestStatus("Should successfully complete ingest", "COMPLETE", r, 500);

    r =
        ingestServiceClient.kafkaToGW(
            storeName,
            spatialIndex,
            null,
            null,
            "testGroup",
            ZookeeperTestEnvironment.getInstance().getZookeeper(),
            null,
            null,
            null,
            null,
            null,
            null,
            "gpx");
    assertFinalIngestStatus("Should successfully ingest from kafka to geowave", "COMPLETE", r, 50);

    muteLogging();
    r =
        ingestServiceClient.kafkaToGW(
            "nonexistent-store",
            spatialIndex,
            null,
            null,
            "testGroup",
            ZookeeperTestEnvironment.getInstance().getZookeeper(),
            null,
            null,
            null,
            null,
            null,
            null,
            "gpx");
    assertFinalIngestStatus("Should fail to ingest for nonexistent store", "ERROR", r, 500);
    unmuteLogging();
  }

  @Test
  public void listplugins() {
    // should always return 200
    TestUtils.assertStatusCode(
        "Should successfully list plugins",
        200,
        ingestServiceClient.listPlugins());
  }

  /**
   * I think that all ingest commands (except for listplugins()) should return a 202 status instead
   * of a 201, especially since all errors are discovered by the baseServiceClient and not the
   * ingestServiceClient. Nothing is created directly from the ingestClient call as it simply kicks
   * off another process.
   */
  @Test
  public void localToGW() {
    Response r = ingestServiceClient.localToGW(OSM_GPX_INPUT_DIR, storeName, spatialIndex);
    assertFinalIngestStatus("Should successfully complete ingest", "COMPLETE", r, 500);

    muteLogging();
    r = ingestServiceClient.localToGW(OSM_GPX_INPUT_DIR, "nonexistent-store", spatialIndex);
    assertFinalIngestStatus(
        "Should fail to complete ingest for nonexistent store",
        "ERROR",
        r,
        500);
    unmuteLogging();
  }

  @Test
  public void localToHdfs() {
    final String hdfsBaseDirectory = MapReduceTestEnvironment.getInstance().getHdfsBaseDirectory();

    final Response r =
        ingestServiceClient.localToHdfs(OSM_GPX_INPUT_DIR, hdfsBaseDirectory, null, "gpx");
    assertFinalIngestStatus("Should successfully complete ingest", "COMPLETE", r, 500);
  }

  // combined testing of commands localToMrGW and mrToGW into one test as
  // mrToGW requires data already ingested into MapReduce.
  @Test
  public void localToMrToGW() {
    final String hdfsBaseDirectory = MapReduceTestEnvironment.getInstance().getHdfsBaseDirectory();
    final String hdfsJobTracker = MapReduceTestEnvironment.getInstance().getJobtracker();

    Response r =
        ingestServiceClient.localToMrGW(
            OSM_GPX_INPUT_DIR,
            hdfsBaseDirectory,
            storeName,
            spatialIndex,
            null,
            hdfsJobTracker,
            null,
            null,
            "gpx");
    assertFinalIngestStatus("Should successfully complete ingest", "COMPLETE", r, 500);

    r =
        ingestServiceClient.mrToGW(
            hdfsBaseDirectory,
            storeName,
            spatialIndex,
            null,
            hdfsJobTracker,
            null,
            null,
            "gpx");
    assertFinalIngestStatus(
        "Should successfully ingest from MapReduce to geowave",
        "COMPLETE",
        r,
        500);

    muteLogging();
    r =
        ingestServiceClient.localToMrGW(
            OSM_GPX_INPUT_DIR,
            hdfsBaseDirectory,
            storeName,
            "nonexistent-index",
            null,
            hdfsJobTracker,
            null,
            null,
            "gpx");
    assertFinalIngestStatus("Should fail to ingest for nonexistent index", "ERROR", r, 500);

    r =
        ingestServiceClient.mrToGW(
            hdfsBaseDirectory,
            "nonexistent-store",
            spatialIndex,
            null,
            hdfsJobTracker,
            null,
            null,
            "gpx");
    assertFinalIngestStatus("Should fail to ingest for nonexistent store", "ERROR", r, 500);
    unmuteLogging();
  }

  @Test
  @Ignore
  public void sparkToGW() {
    final String hdfsBaseDirectory = MapReduceTestEnvironment.getInstance().getHdfsBaseDirectory();

    Response r = ingestServiceClient.localToHdfs(OSM_GPX_INPUT_DIR, hdfsBaseDirectory, null, "gpx");
    assertFinalIngestStatus("Should successfully complete ingest", "COMPLETE", r, 500);

    r = ingestServiceClient.sparkToGW(hdfsBaseDirectory, storeName, spatialIndex);
    assertFinalIngestStatus("Should successfully ingest from spark to geowave", "COMPLETE", r, 500);
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStoreOptions;
  }
}
