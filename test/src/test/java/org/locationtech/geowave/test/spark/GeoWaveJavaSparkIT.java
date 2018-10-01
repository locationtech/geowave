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

import java.io.File;
import java.net.URL;

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
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.DataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.query.DistributableQuery;
import org.locationtech.geowave.core.store.query.QueryOptions;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.TestUtils.ExpectedResults;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveBasicVectorIT;
import org.locationtech.geowave.test.spark.SparkTestEnvironment;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.util.Stopwatch;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.SPARK
})
public class GeoWaveJavaSparkIT extends
		AbstractGeoWaveBasicVectorIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveJavaSparkIT.class);

	private static final String TEST_BOX_FILTER_FILE = TEST_FILTER_PACKAGE + "Box-Filter.shp";
	private static final String TEST_POLYGON_FILTER_FILE = TEST_FILTER_PACKAGE + "Polygon-Filter.shp";
	private static final String HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE + "hail-box-filter.shp";
	private static final String HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE
			+ "hail-polygon-filter.shp";
	private static final int HAIL_COUNT = 13742;
	private static final int TORNADO_COUNT = 1196;

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		// TODO: Dynamo test takes too long to finish on Travis (>5 minutes)
		// GeoWaveStoreType.DYNAMODB,
		GeoWaveStoreType.CASSANDRA
	})
	protected DataStorePluginOptions dataStore;

	private static Stopwatch stopwatch = new Stopwatch();

	@BeforeClass
	public static void reportTestStart() {
		stopwatch.reset();
		stopwatch.start();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*  RUNNING GeoWaveJavaSparkIT           *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTestFinish() {
		stopwatch.stop();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED GeoWaveJavaSparkIT           *");
		LOGGER.warn("*         " + stopwatch.getTimeString() + " elapsed.             *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Test
	public void testLoadRDD()
			throws Exception {
		// Set up Spark
		SparkContext context = SparkTestEnvironment.getInstance().getDefaultSession().sparkContext();

		// ingest test points
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				HAIL_SHAPEFILE_FILE,
				1);

		try {
			// get expected results (box filter)
			final ExpectedResults expectedResults = TestUtils.getExpectedResults(new URL[] {
				new File(
						HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE).toURI().toURL()
			});

			final DistributableQuery query = TestUtils.resourceToQuery(new File(
					TEST_BOX_FILTER_FILE).toURI().toURL());

			// Load RDD using spatial query (bbox)
			RDDOptions queryOpts = new RDDOptions();
			queryOpts.setQuery(query);
			GeoWaveRDD newRDD = GeoWaveRDDLoader.loadRDD(
					context,
					dataStore,
					queryOpts);
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = newRDD.getRawRDD();

			long count = javaRdd.count();
			LOGGER.warn("DataStore loaded into RDD with " + count + " features.");

			// Verify RDD count matches expected count
			Assert.assertEquals(
					expectedResults.count,
					count);
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing a bounding box query of spatial index: '"
					+ e.getLocalizedMessage() + "'");
		}
		try {
			// get expected results (polygon filter)
			final ExpectedResults expectedResults = TestUtils.getExpectedResults(new URL[] {
				new File(
						HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE).toURI().toURL()
			});

			final DistributableQuery query = TestUtils.resourceToQuery(new File(
					TEST_POLYGON_FILTER_FILE).toURI().toURL());

			// Load RDD using spatial query (poly)
			RDDOptions queryOpts = new RDDOptions();
			queryOpts.setQuery(query);
			GeoWaveRDD newRDD = GeoWaveRDDLoader.loadRDD(
					context,
					dataStore,
					queryOpts);
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = newRDD.getRawRDD();

			long count = javaRdd.count();
			LOGGER.warn("DataStore loaded into RDD with " + count + " features.");

			Assert.assertEquals(
					expectedResults.count,
					count);
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing a polygon query of spatial index: '" + e.getLocalizedMessage()
					+ "'");
		}

		// ingest test lines
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				TORNADO_TRACKS_SHAPEFILE_FILE,
				1);

		// Retrieve the adapters
		CloseableIterator<InternalDataAdapter<?>> adapterIt = dataStore.createAdapterStore().getAdapters();
		DataAdapter hailAdapter = null;
		DataAdapter tornadoAdapter = null;

		while (adapterIt.hasNext()) {
			DataAdapter adapter = adapterIt.next().getAdapter();
			String adapterName = StringUtils.stringFromBinary(adapter.getAdapterId().getBytes());

			if (adapterName.equals("hail")) {
				hailAdapter = adapter;
			}
			else {
				tornadoAdapter = adapter;
			}

			LOGGER.warn("DataStore has feature adapter: " + adapterName);
		}

		// Load RDD using hail adapter
		try {

			RDDOptions queryOpts = new RDDOptions();
			queryOpts.setQueryOptions(new QueryOptions(
					hailAdapter));
			GeoWaveRDD newRDD = GeoWaveRDDLoader.loadRDD(
					context,
					dataStore,
					queryOpts);
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = newRDD.getRawRDD();

			long count = javaRdd.count();

			Assert.assertEquals(
					HAIL_COUNT,
					count);

			LOGGER.warn("DataStore loaded into RDD with " + count + " features for adapter "
					+ StringUtils.stringFromBinary(hailAdapter.getAdapterId().getBytes()));
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while loading RDD with adapter: '" + e.getLocalizedMessage() + "'");
		}

		// Load RDD using tornado adapter
		try {
			RDDOptions queryOpts = new RDDOptions();
			queryOpts.setQueryOptions(new QueryOptions(
					tornadoAdapter));
			GeoWaveRDD newRDD = GeoWaveRDDLoader.loadRDD(
					context,
					dataStore,
					queryOpts);
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = newRDD.getRawRDD();

			long count = javaRdd.count();
			LOGGER.warn("DataStore loaded into RDD with " + count + " features for adapter "
					+ StringUtils.stringFromBinary(tornadoAdapter.getAdapterId().getBytes()));

			Assert.assertEquals(
					TORNADO_COUNT,
					count);
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while loading RDD with adapter: '" + e.getLocalizedMessage() + "'");
		}

		// Clean up
		TestUtils.deleteAll(dataStore);
	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}
}
