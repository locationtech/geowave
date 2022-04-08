/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.basic;

import java.io.File;
import java.net.URL;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveBasicTemporalVectorIT extends AbstractGeoWaveBasicVectorIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveBasicTemporalVectorIT.class);

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB,
          GeoWaveStoreType.FILESYSTEM})
  protected DataStorePluginOptions dataStore;

  private static long startMillis;
  private static final boolean POINTS_ONLY = false;
  private static final String HAIL_GEOM_FIELD = "the_geom";
  private static final String HAIL_TIME_FIELD = "DATE";
  public static final Pair<String, String> OPTIMAL_CQL_GEOMETRY_AND_TIME_FIELDS =
      Pair.of(HAIL_GEOM_FIELD, HAIL_TIME_FIELD);
  private static final int NUM_THREADS = 4;

  @BeforeClass
  public static void reportTestStart() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("-----------------------------------------------");
    LOGGER.warn("*                                             *");
    LOGGER.warn("* RUNNING GeoWaveBasicTemporalVectorIT *");
    LOGGER.warn("*                                             *");
    LOGGER.warn("-----------------------------------------------");
  }

  @AfterClass
  public static void reportTestFinish() {
    LOGGER.warn("------------------------------------------------");
    LOGGER.warn("*                                              *");
    LOGGER.warn("* FINISHED GeoWaveBasicTemporalVectorIT *");
    LOGGER.warn(
        "*                "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                  *");
    LOGGER.warn("*                                              *");
    LOGGER.warn("------------------------------------------------");
  }

  @Test
  public void testIngestAndQueryTemporalPointsAndLines() throws Exception {
    // ingest both lines and points
    TestUtils.testLocalIngest(
        dataStore,
        DimensionalityType.TEMPORAL,
        null,
        HAIL_SHAPEFILE_FILE,
        "geotools-vector",
        NUM_THREADS,
        false);

    if (!POINTS_ONLY) {
      TestUtils.testLocalIngest(
          dataStore,
          DimensionalityType.TEMPORAL,
          null,
          TORNADO_TRACKS_SHAPEFILE_FILE,
          "geotools-vector",
          NUM_THREADS,
          false);
    }

    try {
      URL[] expectedResultsUrls;
      if (POINTS_ONLY) {
        expectedResultsUrls =
            new URL[] {new File(HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()};
      } else {
        expectedResultsUrls =
            new URL[] {
                new File(HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL(),
                new File(TORNADO_TRACKS_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()};
      }

      testQuery(
          new File(TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
          expectedResultsUrls,
          null,
          null,
          "bounding box and time range",
          null,
          false);
    } catch (final Exception e) {
      e.printStackTrace();
      TestUtils.deleteAll(dataStore);
      Assert.fail(
          "Error occurred while testing a bounding box and time range query of spatial temporal index: '"
              + e.getLocalizedMessage()
              + "'");
    }

    try {
      URL[] expectedResultsUrls;
      if (POINTS_ONLY) {
        expectedResultsUrls =
            new URL[] {
                new File(HAIL_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()};
      } else {
        expectedResultsUrls =
            new URL[] {
                new File(HAIL_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL(),
                new File(
                    TORNADO_TRACKS_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()};
      }

      testQuery(
          new File(TEST_POLYGON_TEMPORAL_FILTER_FILE).toURI().toURL(),
          expectedResultsUrls,
          OPTIMAL_CQL_GEOMETRY_AND_TIME_FIELDS,
          null,
          "polygon constraint and time range",
          null,
          false);
    } catch (final Exception e) {
      e.printStackTrace();
      TestUtils.deleteAll(dataStore);
      Assert.fail(
          "Error occurred while testing a polygon and time range query of spatial temporal index: '"
              + e.getLocalizedMessage()
              + "'");
    }

    try {
      URL[] statsInputs;
      if (POINTS_ONLY) {
        statsInputs = new URL[] {new File(HAIL_SHAPEFILE_FILE).toURI().toURL()};
      } else {
        statsInputs =
            new URL[] {
                new File(HAIL_SHAPEFILE_FILE).toURI().toURL(),
                new File(TORNADO_TRACKS_SHAPEFILE_FILE).toURI().toURL()};
      }

      testStats(statsInputs, (NUM_THREADS > 1), TestUtils.DEFAULT_TEMPORAL_INDEX);
    } catch (final Exception e) {
      e.printStackTrace();
      TestUtils.deleteAll(dataStore);
      Assert.fail(
          "Error occurred while testing a bounding box stats on spatial temporal index: '"
              + e.getLocalizedMessage()
              + "'");
    }

    try {
      testSpatialTemporalLocalExportAndReingestWithCQL(
          new File(TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
          NUM_THREADS,
          POINTS_ONLY,
          DimensionalityType.TEMPORAL);
    } catch (final Exception e) {
      e.printStackTrace();
      TestUtils.deleteAll(dataStore);
      Assert.fail(
          "Error occurred while testing deletion of an entry using spatial index: '"
              + e.getLocalizedMessage()
              + "'");
    }

    try {
      testDeleteDataId(
          new File(TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
          TestUtils.DEFAULT_TEMPORAL_INDEX);
    } catch (final Exception e) {
      e.printStackTrace();
      TestUtils.deleteAll(dataStore);
      Assert.fail(
          "Error occurred while testing deletion of an entry using spatial temporal index: '"
              + e.getLocalizedMessage()
              + "'");
    }
    TestUtils.deleteAll(dataStore);
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStore;
  }
}
