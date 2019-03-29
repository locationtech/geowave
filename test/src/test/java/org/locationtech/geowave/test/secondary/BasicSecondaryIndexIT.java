/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.secondary;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
public class BasicSecondaryIndexIT extends AbstractSecondaryIndexIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(BasicSecondaryIndexIT.class);

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB},
      options = {"enableSecondaryIndexing=true"})
  protected DataStorePluginOptions dataStoreOptions;
  private static long startMillis;
  private static final String testName = "BasicSecondaryIndexIT";

  @BeforeClass
  public static void startTimer() {
    startMillis = System.currentTimeMillis();
    TestUtils.printStartOfTest(LOGGER, testName);
  }

  @AfterClass
  public static void reportTest() {
    TestUtils.printEndOfTest(LOGGER, testName, startMillis);
  }

  @Test
  public void testLocalIngestAndQuerySpatial() throws Exception {
    testIngestAndQuery(DimensionalityType.SPATIAL);
  }

  @Test
  public void testLocalIngestAndQuerySpatialTemporal() throws Exception {
    testIngestAndQuery(DimensionalityType.SPATIAL_TEMPORAL);
  }

  @Test
  public void testLocalIngestAndQuerySpatialAndSpatialTemporal() throws Exception {
    testIngestAndQuery(DimensionalityType.ALL);
  }

  protected void testIngestAndQuery(final DimensionalityType dimensionality) throws Exception {
    testIngestAndQuery(dimensionality, (d, f) -> {
      try {
        TestUtils.testLocalIngest(getDataStorePluginOptions(), dimensionality, f, 1);
      } catch (final Exception e) {
        LOGGER.warn("Unable to ingest locally", e);
        Assert.fail(e.getMessage());
      }
    }, (input, expected, description) -> {
      try {
        testQuery(input, expected, description);
      } catch (final Exception e) {
        LOGGER.warn("Unable to query locally", e);
        Assert.fail(e.getMessage());
      }
    }, (dimensionalityType, urls) -> testStats(urls, false, dimensionality.getDefaultIndices()));
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStoreOptions;
  }
}
