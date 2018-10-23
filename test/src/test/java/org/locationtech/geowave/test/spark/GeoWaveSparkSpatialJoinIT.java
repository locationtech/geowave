/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.test.spark;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkContext;
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
import org.locationtech.geowave.analytic.spark.sparksql.SimpleFeatureDataFrame;
import org.locationtech.geowave.analytic.spark.sparksql.udf.GeomFunctionRegistry;
import org.locationtech.geowave.analytic.spark.sparksql.udf.GeomWithinDistance;
import org.locationtech.geowave.analytic.spark.spatial.SpatialJoinRunner;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.query.QueryOptions;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.annotation.NamespaceOverride;
import org.locationtech.geowave.test.basic.AbstractGeoWaveBasicVectorIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.SPARK
})
@GeoWaveTestStore(value = {
	GeoWaveStoreType.ACCUMULO,
	GeoWaveStoreType.BIGTABLE,
	GeoWaveStoreType.DYNAMODB,
	GeoWaveStoreType.CASSANDRA
})
public class GeoWaveSparkSpatialJoinIT extends
		AbstractGeoWaveBasicVectorIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveSparkSpatialJoinIT.class);

	protected DataStorePluginOptions hailStore;

	@NamespaceOverride("geowave_tornado")
	protected DataStorePluginOptions tornadoStore;

	private static long startMillis;
	private static SparkSession session = null;
	private static SparkContext context = null;
	private GeoWaveRDD hailRDD = null;
	private GeoWaveRDD tornadoRDD = null;
	private Dataset<Row> hailBruteResults = null;
	private long hailBruteCount = 0;
	private Dataset<Row> tornadoBruteResults = null;
	private long tornadoBruteCount = 0;

	@BeforeClass
	public static void reportTestStart() {

		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*  RUNNING GeoWaveSparkSpatialJoinIT  *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

	}

	@AfterClass
	public static void reportTestFinish() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED GeoWaveSparkSpatialJoinIT  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

	}

	@Test
	public void testHailTornadoDistanceJoin()
			throws Exception {

		session = SparkTestEnvironment.getInstance().getDefaultSession();
		context = session.sparkContext();
		GeomFunctionRegistry.registerGeometryFunctions(session);
		LOGGER.debug("Testing DataStore Type: " + hailStore.getType());
		long mark = System.currentTimeMillis();
		ingestHailandTornado();
		long dur = (System.currentTimeMillis() - mark);

		ByteArrayId hail_adapter = new ByteArrayId(
				"hail");
		ByteArrayId tornado_adapter = new ByteArrayId(
				"tornado_tracks");
		GeomWithinDistance distancePredicate = new GeomWithinDistance(
				0.01);
		String sqlHail = "select hail.* from hail, tornado where GeomDistance(hail.geom,tornado.geom) <= 0.01";
		String sqlTornado = "select tornado.* from hail, tornado where GeomDistance(hail.geom,tornado.geom) <= 0.01";

		SpatialJoinRunner runner = new SpatialJoinRunner(
				session);
		runner.setLeftStore(hailStore);
		runner.setLeftAdapterId(hail_adapter);

		runner.setRightStore(tornadoStore);
		runner.setRightAdapterId(tornado_adapter);

		runner.setPredicate(distancePredicate);
		loadRDDs(
				hail_adapter,
				tornado_adapter);

		long tornadoIndexedCount = 0;
		long hailIndexedCount = 0;
		LOGGER.warn("------------ Running indexed spatial join. ----------");
		mark = System.currentTimeMillis();
		try {
			runner.run();
		}
		catch (InterruptedException | ExecutionException e) {
			LOGGER.error("Async error in join");
			e.printStackTrace();
		}
		catch (IOException e) {
			LOGGER.error("IO error in join");
			e.printStackTrace();
		}
		hailIndexedCount = runner.getLeftResults().getRawRDD().count();
		tornadoIndexedCount = runner.getRightResults().getRawRDD().count();
		long indexJoinDur = (System.currentTimeMillis() - mark);
		LOGGER.warn("Indexed Result Count: " + (hailIndexedCount + tornadoIndexedCount));
		SimpleFeatureDataFrame indexHailFrame = new SimpleFeatureDataFrame(
				session);
		SimpleFeatureDataFrame indexTornadoFrame = new SimpleFeatureDataFrame(
				session);

		indexTornadoFrame.init(
				tornadoStore,
				tornado_adapter);
		Dataset<Row> indexedTornado = indexTornadoFrame.getDataFrame(runner.getRightResults());

		indexHailFrame.init(
				hailStore,
				hail_adapter);
		Dataset<Row> indexedHail = indexHailFrame.getDataFrame(runner.getLeftResults());

		LOGGER.warn("------------ Running Brute force spatial join. ----------");
		dur = runBruteForceJoin(
				hail_adapter,
				tornado_adapter,
				sqlHail,
				sqlTornado);

		LOGGER.warn("Indexed join duration = " + indexJoinDur + " ms.");
		LOGGER.warn("Brute join duration = " + dur + " ms.");

		// Verify each row matches
		Assert.assertTrue((hailIndexedCount == hailBruteCount));
		Assert.assertTrue((tornadoIndexedCount == tornadoBruteCount));
		Dataset<Row> subtractedFrame = indexedHail.except(hailBruteResults);
		subtractedFrame = subtractedFrame.cache();
		Assert.assertTrue(
				"Subtraction between brute force join and indexed Hail should result in count of 0",
				(subtractedFrame.count() == 0));
		subtractedFrame.unpersist();
		subtractedFrame = indexedTornado.except(tornadoBruteResults);
		subtractedFrame = subtractedFrame.cache();
		Assert.assertTrue(
				"Subtraction between brute force join and indexed Tornado should result in count of 0",
				(subtractedFrame.count() == 0));

		TestUtils.deleteAll(hailStore);
		TestUtils.deleteAll(tornadoStore);
	}

	private void ingestHailandTornado()
			throws Exception {
		long mark = System.currentTimeMillis();

		// ingest both lines and points
		TestUtils.testLocalIngest(
				hailStore,
				DimensionalityType.SPATIAL,
				HAIL_SHAPEFILE_FILE,
				1);

		long dur = (System.currentTimeMillis() - mark);
		LOGGER.debug("Ingest (points) duration = " + dur + " ms with " + 1 + " thread(s).");

		mark = System.currentTimeMillis();

		TestUtils.testLocalIngest(
				tornadoStore,
				DimensionalityType.SPATIAL,
				TORNADO_TRACKS_SHAPEFILE_FILE,
				1);

		dur = (System.currentTimeMillis() - mark);
		LOGGER.debug("Ingest (lines) duration = " + dur + " ms with " + 1 + " thread(s).");

	}

	private void loadRDDs(
			ByteArrayId hail_adapter,
			ByteArrayId tornado_adapter ) {

		short hailInternalAdapterId = hailStore.createInternalAdapterStore().getInternalAdapterId(
				hail_adapter);
		// Write out the hull features
		InternalDataAdapter<?> hailAdapter = hailStore.createAdapterStore().getAdapter(
				hailInternalAdapterId);
		short tornadoInternalAdapterId = tornadoStore.createInternalAdapterStore().getInternalAdapterId(
				tornado_adapter);
		InternalDataAdapter<?> tornadoAdapter = tornadoStore.createAdapterStore().getAdapter(
				tornadoInternalAdapterId);
		try {
			RDDOptions hailOpts = new RDDOptions();
			hailOpts.setQueryOptions(new QueryOptions(
					hailAdapter.getAdapter()));
			hailRDD = GeoWaveRDDLoader.loadRDD(
					context,
					hailStore,
					hailOpts);

			RDDOptions tornadoOpts = new RDDOptions();
			tornadoOpts.setQueryOptions(new QueryOptions(
					tornadoAdapter.getAdapter()));
			tornadoRDD = GeoWaveRDDLoader.loadRDD(
					context,
					tornadoStore,
					tornadoOpts);
		}
		catch (final Exception e) {
			LOGGER.error("Could not load rdds for test");
			e.printStackTrace();
			TestUtils.deleteAll(hailStore);
			TestUtils.deleteAll(tornadoStore);
			Assert.fail();
		}
	}

	private long runBruteForceJoin(
			ByteArrayId hail_adapter,
			ByteArrayId tornado_adapter,
			String sqlHail,
			String sqlTornado ) {
		long mark = System.currentTimeMillis();
		SimpleFeatureDataFrame hailFrame = new SimpleFeatureDataFrame(
				session);
		SimpleFeatureDataFrame tornadoFrame = new SimpleFeatureDataFrame(
				session);

		tornadoFrame.init(
				tornadoStore,
				tornado_adapter);
		tornadoFrame.getDataFrame(
				tornadoRDD).createOrReplaceTempView(
				"tornado");

		hailFrame.init(
				hailStore,
				hail_adapter);
		hailFrame.getDataFrame(
				hailRDD).createOrReplaceTempView(
				"hail");

		hailBruteResults = session.sql(sqlHail);
		hailBruteResults = hailBruteResults.dropDuplicates();
		hailBruteResults.cache();
		hailBruteCount = hailBruteResults.count();

		tornadoBruteResults = session.sql(sqlTornado);
		tornadoBruteResults = tornadoBruteResults.dropDuplicates();
		tornadoBruteResults.cache();
		tornadoBruteCount = tornadoBruteResults.count();
		long dur = (System.currentTimeMillis() - mark);
		LOGGER.warn("Brute Result Count: " + (tornadoBruteCount + hailBruteCount));
		return dur;
	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return hailStore;
	}
}
