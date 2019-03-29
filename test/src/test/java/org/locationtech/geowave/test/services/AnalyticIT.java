/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.services;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.service.client.AnalyticServiceClient;
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
public class AnalyticIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticIT.class);
  private static final String WFS_URL_PREFIX =
      ServicesTestEnvironment.JETTY_BASE_URL + "/geoserver/wfs";

  private static final String GEOSTUFF_LAYER_FILE =
      "src/test/resources/wfs-requests/geostuff_layer.xml";
  private static final String INSERT_FILE = "src/test/resources/wfs-requests/insert.xml";
  private static final String LOCK_FILE = "src/test/resources/wfs-requests/lock.xml";
  private static final String QUERY_FILE = "src/test/resources/wfs-requests/query.xml";
  private static final String UPDATE_FILE = "src/test/resources/wfs-requests/update.xml";

  private AnalyticServiceClient analyticServiceClient;

  private String input_storename;
  private String output_storename;

  private static final String testName = "AnalyticIT";

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
  }

  @AfterClass
  public static void reportTest() {
    TestUtils.printEndOfTest(LOGGER, testName, startMillis);
  }

  @Before
  public void initialize() {
    // Perform ingest operations here, so there is data on which to run the
    // analytics.
  }

  @After
  public void cleanupWorkspace() {
    // Remove everything created in the @Before method, so each test starts
    // with a clean slate.

    // If confident the initialization data does not change during the test,
    // you may move the setup/tear down actions to the @BeforeClass and
    // @AfterClass methods.
  }

  @Test
  @Ignore
  public void example() {
    // Tests should contain calls to the REST services methods, checking
    // them for proper response and status codes.

    // Use this method to check:

    TestUtils.assertStatusCode(
        "Should Successfully <Insert Objective Here>",
        200,
        analyticServiceClient.kmeansSpark(input_storename, output_storename));
  }

  @Test
  @Ignore
  public void dbScan() {
    // TODO: Implement this test
  }

  @Test
  @Ignore
  public void kde() {
    // TODO: Implement this test
  }

  @Test
  @Ignore
  public void kmeansspark() {
    // TODO: Implement this test
  }

  @Test
  @Ignore
  public void nn() {
    // TODO: Implement this test
  }

  @Test
  @Ignore
  public void spatialjoin() {
    // TODO: Implement this test
  }

  @Test
  @Ignore
  public void sql() {
    // TODO: Implement this test
  }
}
