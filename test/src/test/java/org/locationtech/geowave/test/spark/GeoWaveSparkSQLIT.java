/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.analytic.spark.GeoWaveRDD;
import org.locationtech.geowave.analytic.spark.GeoWaveRDDLoader;
import org.locationtech.geowave.analytic.spark.RDDOptions;
import org.locationtech.geowave.analytic.spark.sparksql.SqlQueryRunner;
import org.locationtech.geowave.analytic.spark.sparksql.SqlResultsWriter;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
@Environments({Environment.SPARK})
public class GeoWaveSparkSQLIT extends AbstractGeoWaveBasicVectorIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveSparkSQLIT.class);

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.HBASE,
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
    LOGGER.warn("*  RUNNING GeoWaveSparkSQLIT        *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @AfterClass
  public static void reportTestFinish() {
    stopwatch.stop();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("* FINISHED GeoWaveSparkSQLIT        *");
    LOGGER.warn("*         " + stopwatch.getTimeString() + " elapsed.             *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @Test
  public void testCreateDataFrame() throws Exception {
    // Set up Spark
    final SparkSession session = SparkTestEnvironment.getInstance().getDefaultSession();
    final SparkContext context = session.sparkContext();

    // ingest test points
    TestUtils.testLocalIngest(dataStore, DimensionalityType.SPATIAL, HAIL_SHAPEFILE_FILE, 1);

    final SqlQueryRunner queryRunner = new SqlQueryRunner();
    queryRunner.setSparkSession(session);

    try {
      // Load RDD from datastore, no filters
      final GeoWaveRDD newRDD = GeoWaveRDDLoader.loadRDD(context, dataStore, new RDDOptions());
      final JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = newRDD.getRawRDD();

      final long count = javaRdd.count();
      LOGGER.warn("DataStore loaded into RDD with " + count + " features.");

      queryRunner.addInputStore(dataStore, null, "features");

      final String bbox = "POLYGON ((-94 34, -93 34, -93 35, -94 35, -94 34))";

      queryRunner.setSql(
          "SELECT * FROM features WHERE GeomContains(GeomFromWKT('" + bbox + "'), geom)");

      Dataset<Row> results = queryRunner.run();
      final long containsCount = results.count();
      LOGGER.warn("Got " + containsCount + " for GeomContains test");

      queryRunner.setSql(
          "SELECT * FROM features WHERE GeomWithin(geom, GeomFromWKT('" + bbox + "'))");
      results = queryRunner.run();
      final long withinCount = results.count();
      LOGGER.warn("Got " + withinCount + " for GeomWithin test");

      Assert.assertTrue("Within and Contains counts should be equal", containsCount == withinCount);

      // Test the output writer
      final SqlResultsWriter sqlResultsWriter = new SqlResultsWriter(results, dataStore);

      sqlResultsWriter.writeResults("sqltest");

      queryRunner.removeAllStores();

      // Test other spatial UDFs
      final String line1 = "LINESTRING(0 0, 10 10)";
      final String line2 = "LINESTRING(0 10, 10 0)";
      queryRunner.setSql(
          "SELECT GeomIntersects(GeomFromWKT('" + line1 + "'), GeomFromWKT('" + line2 + "'))");
      Row result = queryRunner.run().head();

      final boolean intersect = result.getBoolean(0);
      LOGGER.warn("GeomIntersects returned " + intersect);

      Assert.assertTrue("Lines should intersect", intersect);

      queryRunner.setSql(
          "SELECT GeomDisjoint(GeomFromWKT('" + line1 + "'), GeomFromWKT('" + line2 + "'))");
      result = queryRunner.run().head();

      final boolean disjoint = result.getBoolean(0);
      LOGGER.warn("GeomDisjoint returned " + disjoint);

      Assert.assertFalse("Lines should not be disjoint", disjoint);

    } catch (final Exception e) {
      e.printStackTrace();
      TestUtils.deleteAll(dataStore);
      Assert.fail(
          "Error occurred while testing a bounding box query of spatial index: '"
              + e.getLocalizedMessage()
              + "'");
    }

    // Clean up
    TestUtils.deleteAll(dataStore);
  }

  @Test
  public void testSpatialJoin() throws Exception {

    // Set up Spark
    final SparkSession session = SparkTestEnvironment.getInstance().getDefaultSession();

    final SqlQueryRunner queryRunner = new SqlQueryRunner();
    queryRunner.setSparkSession(session);

    // ingest test points
    TestUtils.testLocalIngest(dataStore, DimensionalityType.SPATIAL, HAIL_SHAPEFILE_FILE, 1);

    TestUtils.testLocalIngest(
        dataStore,
        DimensionalityType.SPATIAL,
        TORNADO_TRACKS_SHAPEFILE_FILE,
        1);

    try {
      // Run a valid sql query that should do a optimized join
      queryRunner.addInputStore(dataStore, "hail", "hail");
      queryRunner.addInputStore(dataStore, "tornado_tracks", "tornado");
      queryRunner.setSql(
          "select hail.* from hail, tornado where GeomIntersects(hail.geom, tornado.geom)");
      final Dataset<Row> results = queryRunner.run();
      LOGGER.warn("Indexed intersect from sql returns: " + results.count() + " results.");
    } catch (final Exception e) {
      e.printStackTrace();
      TestUtils.deleteAll(dataStore);
      Assert.fail(
          "Error occurred while attempting optimized join from sql query runner: '"
              + e.getLocalizedMessage()
              + "'");
    }

    // Clean up
    TestUtils.deleteAll(dataStore);
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStore;
  }
}
