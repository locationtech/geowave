/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.secondary;

import java.io.File;
import org.apache.commons.io.FilenameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
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
import org.locationtech.geowave.test.mapreduce.MapReduceTestEnvironment;
import org.locationtech.geowave.test.mapreduce.MapReduceTestUtils;
import org.locationtech.geowave.test.spark.SparkTestEnvironment;
import org.locationtech.geowave.test.spark.SparkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
@Environments({Environment.MAP_REDUCE, Environment.SPARK})
public class MapReduceSecondaryIndexIT extends AbstractSecondaryIndexIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(MapReduceSecondaryIndexIT.class);
  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          // HBase for cloudera 5.14 takes too long
          // GeoWaveStoreType.HBASE,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.CASSANDRA,
          // DYNAMODB takes too long
          // GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB},
      options = {"enableSecondaryIndexing=true"})
  protected DataStorePluginOptions dataStoreOptions;
  private static long startMillis;
  private static final String testName = "MapReduceSecondaryIndexIT";
  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          // HBase for cloudera 5.14 takes too long
          // GeoWaveStoreType.HBASE,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.CASSANDRA,
          // GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB},
      options = {"enableSecondaryIndexing=true"},
      namespace = "MapReduceSecondaryIndexIT_tmp")
  protected DataStorePluginOptions inputDataStoreOptions;
  private static boolean inputStoreCreated = false;

  @BeforeClass
  public static void startTimer() {
    startMillis = System.currentTimeMillis();
    TestUtils.printStartOfTest(LOGGER, testName);

  }

  @Before
  public synchronized void createInputStore() throws Exception {
    if (!inputStoreCreated) {
      TestUtils.testLocalIngest(
          inputDataStoreOptions,
          DimensionalityType.SPATIAL,
          HAIL_SHAPEFILE_FILE,
          1);
      MapReduceTestUtils.testMapReduceExport(
          inputDataStoreOptions,
          FilenameUtils.getBaseName(HAIL_SHAPEFILE_FILE));
      TestUtils.testLocalIngest(
          inputDataStoreOptions,
          DimensionalityType.SPATIAL,
          TORNADO_TRACKS_SHAPEFILE_FILE,
          1);
      MapReduceTestUtils.testMapReduceExport(
          inputDataStoreOptions,
          FilenameUtils.getBaseName(TORNADO_TRACKS_SHAPEFILE_FILE));
      inputStoreCreated = true;
    }
  }

  protected void testIngestAndQuery(final DimensionalityType dimensionality) throws Exception {
    testIngestAndQuery(dimensionality, (d, f) -> {
      try {
        MapReduceTestUtils.testMapReduceIngest(
            dataStoreOptions,
            dimensionality,
            "avro",
            TestUtils.TEMP_DIR
                + File.separator
                + MapReduceTestEnvironment.HDFS_BASE_DIRECTORY
                + File.separator
                + FilenameUtils.getBaseName(f));
      } catch (final Exception e) {
        LOGGER.warn("Unable to ingest map-reduce", e);
        Assert.fail(e.getMessage());
      }
    },
        (input, expected, description) -> SparkUtils.verifyQuery(
            getDataStorePluginOptions(),
            SparkTestEnvironment.getInstance().getDefaultSession().sparkContext(),
            input,
            expected,
            description,
            null,
            false),
        (dimensionalityType, urls) -> {
          // no-op on verify stats because the "expected" stats that are calculated are off by an
          // epsilon (ie. problem with the test, not the actual results)
        });
  }

  @AfterClass
  public static void reportTest() {
    TestUtils.printEndOfTest(LOGGER, testName, startMillis);
  }

  @Test
  public void testDistributedIngestAndQuerySpatial() throws Exception {
    testIngestAndQuery(DimensionalityType.SPATIAL);
  }

  @Test
  public void testDistributedIngestAndQuerySpatialTemporal() throws Exception {
    testIngestAndQuery(DimensionalityType.SPATIAL_TEMPORAL);
  }

  @Test
  public void testDistributedIngestAndQuerySpatialAndSpatialTemporal() throws Exception {
    testIngestAndQuery(DimensionalityType.ALL);
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStoreOptions;
  }
}
