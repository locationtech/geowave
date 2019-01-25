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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
@Environments({Environment.MAP_REDUCE, Environment.SPARK})
public class MapReduceSecondaryIndexIT extends AbstractSecondaryIndexIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(MapReduceSecondaryIndexIT.class);
  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB},
      options = {"enableSecondaryIndex=true"})
  protected DataStorePluginOptions dataStoreOptions;
  private static long startMillis;
  private static final String testName = "MapReduceSecondaryIndexIT";

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
  public void testDistributedIngestAndQuerySpatial() throws Exception {
    testIngestAndQuery(DimensionalityType.SPATIAL, true);
  }

  @Test
  public void testDistributedIngestAndQuerySpatialTemporal() throws Exception {
    testIngestAndQuery(DimensionalityType.SPATIAL_TEMPORAL, true);
  }

  @Test
  public void testDistributedIngestAndQuerySpatialAndSpatialTemporal() throws Exception {
    testIngestAndQuery(DimensionalityType.ALL, true);
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStoreOptions;
  }
}
