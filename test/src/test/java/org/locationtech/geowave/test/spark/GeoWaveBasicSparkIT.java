/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.spark;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.analytic.spark.GeoWaveRDD;
import org.locationtech.geowave.analytic.spark.GeoWaveRDDLoader;
import org.locationtech.geowave.analytic.spark.RDDOptions;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveBasicVectorIT;
import org.locationtech.jts.util.Stopwatch;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
@Environments({Environment.SPARK})
public class GeoWaveBasicSparkIT extends AbstractGeoWaveBasicVectorIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveBasicSparkIT.class);
  public static final int HAIL_COUNT = 13742;
  public static final int TORNADO_COUNT = 1196;
  private static final String HAIL_GEOM_FIELD = "the_geom";
  private static final String HAIL_TIME_FIELD = "DATE";
  public static final Pair<String, String> OPTIMAL_CQL_GEOMETRY_AND_TIME_FIELDS =
      Pair.of(HAIL_GEOM_FIELD, HAIL_TIME_FIELD);

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.BIGTABLE,
          // TODO: Dynamo test takes too long to finish on Travis (>5 minutes)
          // GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB})
  protected DataStorePluginOptions dataStore;

  private static Stopwatch stopwatch = new Stopwatch();

  @BeforeClass
  public static void reportTestStart() {
    stopwatch.reset();
    stopwatch.start();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  RUNNING GeoWaveBasicSparkIT           *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @AfterClass
  public static void reportTestFinish() {
    stopwatch.stop();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("* FINISHED GeoWaveBasicSparkIT           *");
    LOGGER.warn("*         " + stopwatch.getTimeString() + " elapsed.             *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @Test
  public void testLoadRDD() throws Exception {
    // Set up Spark
    final SparkContext context =
        SparkTestEnvironment.getInstance().getDefaultSession().sparkContext();

    TestUtils.deleteAll(dataStore);
    // test spatial temporal queries with spatial index for tornado tracks
    TestUtils.testLocalIngest(
        dataStore,
        DimensionalityType.SPATIAL,
        TORNADO_TRACKS_SHAPEFILE_FILE,
        1);
    verifyQuery(
        context,
        TEST_BOX_TEMPORAL_FILTER_FILE,
        TORNADO_TRACKS_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE,
        "bounding box tornado tracks spatial-temporal query with spatial only index",
        true);
    verifyQuery(
        context,
        TEST_POLYGON_TEMPORAL_FILTER_FILE,
        TORNADO_TRACKS_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE,
        "polygon tornado tracks spatial-temporal query with spatial only index",
        false);
    TestUtils.deleteAll(dataStore);

    // test spatial queries with spatial temporal index for tornado tracks
    TestUtils.testLocalIngest(
        dataStore,
        DimensionalityType.SPATIAL_TEMPORAL,
        TORNADO_TRACKS_SHAPEFILE_FILE,
        1);
    verifyQuery(
        context,
        TEST_BOX_FILTER_FILE,
        TORNADO_TRACKS_EXPECTED_BOX_FILTER_RESULTS_FILE,
        "bounding box tornado tracks spatial query with spatial temporal index only",
        true);
    verifyQuery(
        context,
        TEST_POLYGON_FILTER_FILE,
        TORNADO_TRACKS_EXPECTED_POLYGON_FILTER_RESULTS_FILE,
        "polygon tornado tracks spatial query with spatial temporal index only",
        true);
    TestUtils.deleteAll(dataStore);
    // ingest test points
    TestUtils.testLocalIngest(dataStore, DimensionalityType.ALL, HAIL_SHAPEFILE_FILE, 1);
    verifyQuery(
        context,
        TEST_BOX_FILTER_FILE,
        HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE,
        "bounding box hail spatial query",
        true);
    verifyQuery(
        context,
        TEST_POLYGON_FILTER_FILE,
        HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE,
        "polygon hail spatial query",
        true);
    verifyQuery(
        context,
        TEST_BOX_TEMPORAL_FILTER_FILE,
        HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE,
        "bounding box hail spatial-temporal query",
        false);
    verifyQuery(
        context,
        TEST_POLYGON_TEMPORAL_FILTER_FILE,
        HAIL_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE,
        "polygon hail spatial-temporal query",
        true);
    // test configurable CRS for hail points
    verifyQuery(
        context,
        TEST_BOX_FILTER_FILE,
        HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE,
        "bounding box hail spatial query with other CRS",
        TestUtils.CUSTOM_CRS,
        true);
    verifyQuery(
        context,
        TEST_POLYGON_FILTER_FILE,
        HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE,
        "polygon hail spatial query with other CRS",
        TestUtils.CUSTOM_CRS,
        true);
    verifyQuery(
        context,
        TEST_BOX_TEMPORAL_FILTER_FILE,
        HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE,
        "bounding box hail spatial-temporal query with other CRS",
        TestUtils.CUSTOM_CRS,
        true);
    verifyQuery(
        context,
        TEST_POLYGON_TEMPORAL_FILTER_FILE,
        HAIL_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE,
        "polygon hail spatial-temporal query with other CRS",
        TestUtils.CUSTOM_CRS,
        false);

    TestUtils.deleteAll(dataStore);

    // test lines only
    TestUtils.testLocalIngest(dataStore, DimensionalityType.ALL, TORNADO_TRACKS_SHAPEFILE_FILE, 1);

    verifyQuery(
        context,
        TEST_BOX_FILTER_FILE,
        TORNADO_TRACKS_EXPECTED_BOX_FILTER_RESULTS_FILE,
        "bounding box tornado tracks spatial query",
        true);
    verifyQuery(
        context,
        TEST_POLYGON_FILTER_FILE,
        TORNADO_TRACKS_EXPECTED_POLYGON_FILTER_RESULTS_FILE,
        "polygon tornado tracks spatial query",
        true);
    verifyQuery(
        context,
        TEST_BOX_TEMPORAL_FILTER_FILE,
        TORNADO_TRACKS_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE,
        "bounding box tornado tracks spatial-temporal query",
        true);
    verifyQuery(
        context,
        TEST_POLYGON_TEMPORAL_FILTER_FILE,
        TORNADO_TRACKS_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE,
        "polygon tornado tracks spatial-temporal query",
        true);

    // test configurable CRS for tornado tracks
    verifyQuery(
        context,
        TEST_BOX_FILTER_FILE,
        TORNADO_TRACKS_EXPECTED_BOX_FILTER_RESULTS_FILE,
        "bounding box tornado tracks spatial query with other CRS",
        TestUtils.CUSTOM_CRS,
        true);
    verifyQuery(
        context,
        TEST_POLYGON_FILTER_FILE,
        TORNADO_TRACKS_EXPECTED_POLYGON_FILTER_RESULTS_FILE,
        "polygon tornado tracks spatial query with other CRS",
        TestUtils.CUSTOM_CRS,
        true);
    verifyQuery(
        context,
        TEST_BOX_TEMPORAL_FILTER_FILE,
        TORNADO_TRACKS_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE,
        "bounding box tornado tracks spatial-temporal query with other CRS",
        TestUtils.CUSTOM_CRS,
        false);
    verifyQuery(
        context,
        TEST_POLYGON_TEMPORAL_FILTER_FILE,
        TORNADO_TRACKS_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE,
        "polygon tornado tracks spatial-temporal query with other CRS",
        TestUtils.CUSTOM_CRS,
        true);

    // now test with both ingested
    TestUtils.testLocalIngest(dataStore, DimensionalityType.ALL, HAIL_SHAPEFILE_FILE, 1);

    // Retrieve the adapters
    final CloseableIterator<InternalDataAdapter<?>> adapterIt =
        dataStore.createAdapterStore().getAdapters();
    DataTypeAdapter hailAdapter = null;
    DataTypeAdapter tornadoAdapter = null;

    while (adapterIt.hasNext()) {
      final DataTypeAdapter adapter = adapterIt.next().getAdapter();
      final String adapterName = adapter.getTypeName();

      if (adapterName.equals("hail")) {
        hailAdapter = adapter;
      } else {
        tornadoAdapter = adapter;
      }

      LOGGER.warn("DataStore has feature adapter: " + adapterName);
    }

    // Load RDD using hail adapter
    try {

      final RDDOptions queryOpts = new RDDOptions();
      queryOpts.setQuery(QueryBuilder.newBuilder().addTypeName(hailAdapter.getTypeName()).build());
      final GeoWaveRDD newRDD = GeoWaveRDDLoader.loadRDD(context, dataStore, queryOpts);
      final JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = newRDD.getRawRDD();

      final long count = SparkUtils.getCount(javaRdd, dataStore.getType());

      Assert.assertEquals(HAIL_COUNT, count);

      LOGGER.warn(
          "DataStore loaded into RDD with "
              + count
              + " features for adapter "
              + hailAdapter.getTypeName());
    } catch (final Exception e) {
      e.printStackTrace();
      TestUtils.deleteAll(dataStore);
      Assert.fail(
          "Error occurred while loading RDD with adapter: '" + e.getLocalizedMessage() + "'");
    }

    // Load RDD using tornado adapter
    try {
      final RDDOptions queryOpts = new RDDOptions();
      queryOpts.setQuery(
          QueryBuilder.newBuilder().addTypeName(tornadoAdapter.getTypeName()).build());
      final GeoWaveRDD newRDD = GeoWaveRDDLoader.loadRDD(context, dataStore, queryOpts);
      final JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = newRDD.getRawRDD();

      final long count = SparkUtils.getCount(javaRdd, dataStore.getType());
      LOGGER.warn(
          "DataStore loaded into RDD with "
              + count
              + " features for adapter "
              + tornadoAdapter.getTypeName());

      Assert.assertEquals(TORNADO_COUNT, count);
    } catch (final Exception e) {
      e.printStackTrace();
      TestUtils.deleteAll(dataStore);
      Assert.fail(
          "Error occurred while loading RDD with adapter: '" + e.getLocalizedMessage() + "'");
    }

    // Clean up
    TestUtils.deleteAll(dataStore);
  }

  protected void verifyQuery(
      final SparkContext context,
      final String filterFile,
      final String expectedResultsFile,
      final String name,
      final boolean useDuring) throws MalformedURLException {
    verifyQuery(context, filterFile, expectedResultsFile, name, null, useDuring);
  }

  protected void verifyQuery(
      final SparkContext context,
      final String filterFile,
      final String expectedResultsFile,
      final String name,
      final CoordinateReferenceSystem crsTransform,
      final boolean useDuring) throws MalformedURLException {
    SparkUtils.verifyQuery(
        dataStore,
        context,
        new File(filterFile).toURI().toURL(),
        new URL[] {new File(expectedResultsFile).toURI().toURL()},
        name,
        crsTransform,
        OPTIMAL_CQL_GEOMETRY_AND_TIME_FIELDS,
        useDuring);
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStore;
  }
}
