/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.basic;

import java.io.File;
import java.net.URL;
import org.geotools.referencing.CRS;
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
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveCustomCRSSpatialVectorIT extends AbstractGeoWaveBasicVectorIT {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeoWaveCustomCRSSpatialVectorIT.class);

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB})
  protected DataStorePluginOptions dataStore;

  private static long startMillis;

  @BeforeClass
  public static void reportTestStart() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("---------------------------------------------");
    LOGGER.warn("*                                           *");
    LOGGER.warn("*  RUNNING GeoWaveCustomCRSSpatialVectorIT  *");
    LOGGER.warn("*                                           *");
    LOGGER.warn("---------------------------------------------");
  }

  @AfterClass
  public static void reportTestFinish() {
    LOGGER.warn("---------------------------------------------");
    LOGGER.warn("*                                           *");
    LOGGER.warn("* FINISHED GeoWaveCustomCRSSpatialVectorIT  *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                            *");
    LOGGER.warn("*                                           *");
    LOGGER.warn("---------------------------------------------");
  }

  @Test
  public void testSingleThreadedIngestAndQuerySpatialPointsAndLines() throws Exception {
    testIngestAndQuerySpatialPointsAndLines(1);
  }

  public void testIngestAndQuerySpatialPointsAndLines(final int nthreads) throws Exception {
    long mark = System.currentTimeMillis();

    LOGGER.debug("Testing DataStore Type: " + dataStore.getType());

    // ingest both lines and points
    TestUtils.testLocalIngest(
        dataStore,
        DimensionalityType.SPATIAL,
        TestUtils.CUSTOM_CRSCODE,
        HAIL_SHAPEFILE_FILE,
        "geotools-vector",
        nthreads);

    long dur = (System.currentTimeMillis() - mark);
    LOGGER.debug("Ingest (points) duration = " + dur + " ms with " + nthreads + " thread(s).");

    mark = System.currentTimeMillis();

    TestUtils.testLocalIngest(
        dataStore,
        DimensionalityType.SPATIAL,
        TestUtils.CUSTOM_CRSCODE,
        TORNADO_TRACKS_SHAPEFILE_FILE,
        "geotools-vector",
        nthreads);

    dur = (System.currentTimeMillis() - mark);
    LOGGER.debug("Ingest (lines) duration = " + dur + " ms with " + nthreads + " thread(s).");
    try {
      final CoordinateReferenceSystem crs = CRS.decode(TestUtils.CUSTOM_CRSCODE);
      mark = System.currentTimeMillis();

      testQuery(
          new File(TEST_BOX_FILTER_FILE).toURI().toURL(),
          new URL[] {
              new File(HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE).toURI().toURL(),
              new File(TORNADO_TRACKS_EXPECTED_BOX_FILTER_RESULTS_FILE).toURI().toURL()},
          TestUtils.createWebMercatorSpatialIndex(),
          "bounding box constraint only",
          crs,
          true);

      dur = (System.currentTimeMillis() - mark);
      LOGGER.debug("BBOX query duration = " + dur + " ms.");
      mark = System.currentTimeMillis();

      testQuery(
          new File(TEST_POLYGON_FILTER_FILE).toURI().toURL(),
          new URL[] {
              new File(HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE).toURI().toURL(),
              new File(TORNADO_TRACKS_EXPECTED_POLYGON_FILTER_RESULTS_FILE).toURI().toURL()},
          TestUtils.createWebMercatorSpatialIndex(),
          "polygon constraint only",
          crs,
          true);

      dur = (System.currentTimeMillis() - mark);
      LOGGER.debug("POLY query duration = " + dur + " ms.");
    } catch (final Exception e) {
      e.printStackTrace();
      TestUtils.deleteAll(dataStore);
      Assert.fail(
          "Error occurred while testing a polygon query of spatial index: '"
              + e.getLocalizedMessage()
              + "'");
    }

    try {
      testStats(
          new URL[] {
              new File(HAIL_SHAPEFILE_FILE).toURI().toURL(),
              new File(TORNADO_TRACKS_SHAPEFILE_FILE).toURI().toURL()},
          false,
          CRS.decode(TestUtils.CUSTOM_CRSCODE),
          TestUtils.createWebMercatorSpatialIndex());
    } catch (final Exception e) {
      e.printStackTrace();
      TestUtils.deleteAll(dataStore);
      Assert.fail(
          "Error occurred while testing a bounding box stats on spatial index: '"
              + e.getLocalizedMessage()
              + "'");
    }

    try {
      testDeleteCQL(CQL_DELETE_STR, TestUtils.createWebMercatorSpatialIndex());
    } catch (final Exception e) {
      e.printStackTrace();
      TestUtils.deleteAll(dataStore);
      Assert.fail(
          "Error occurred while testing deletion of an entry using spatial index: '"
              + e.getLocalizedMessage()
              + "'");
    }

    try {
      testDeleteByBasicQuery(
          new File(TEST_POLYGON_FILTER_FILE).toURI().toURL(),
          TestUtils.createWebMercatorSpatialIndex());
    } catch (final Exception e) {
      e.printStackTrace();
      TestUtils.deleteAll(dataStore);
      Assert.fail(
          "Error occurred while testing deletion of an entry using spatial index: '"
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
