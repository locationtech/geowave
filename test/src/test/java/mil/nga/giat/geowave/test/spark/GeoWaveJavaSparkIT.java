/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.test.spark;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.util.Stopwatch;

import mil.nga.giat.geowave.analytic.spark.GeoWaveRDD;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.TestUtils.ExpectedResults;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import mil.nga.giat.geowave.test.basic.AbstractGeoWaveBasicVectorIT;

@RunWith(GeoWaveITRunner.class)
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
		GeoWaveStoreType.HBASE
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
	public void testLoadRDD() {
		// Set up Spark
		SparkConf sparkConf = new SparkConf();

		sparkConf.setAppName("GeoWaveRDD");
		sparkConf.setMaster("local");
		sparkConf.set("spark.kryo.registrator", "mil.nga.giat.geowave.analytic.spark.GeoWaveRegistrator");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		JavaSparkContext context = new JavaSparkContext(
				sparkConf);

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
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = GeoWaveRDD.rddForSimpleFeatures(
					context.sc(),
					dataStore,
					query);

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
			context.close();
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
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = GeoWaveRDD.rddForSimpleFeatures(
					context.sc(),
					dataStore,
					query);

			long count = javaRdd.count();
			LOGGER.warn("DataStore loaded into RDD with " + count + " features.");

			Assert.assertEquals(
					expectedResults.count,
					count);
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			context.close();
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
		CloseableIterator<DataAdapter<?>> adapterIt = dataStore.createAdapterStore().getAdapters();
		DataAdapter hailAdapter = null;
		DataAdapter tornadoAdapter = null;

		while (adapterIt.hasNext()) {
			DataAdapter adapter = adapterIt.next();
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
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = GeoWaveRDD.rddForSimpleFeatures(
					context.sc(),
					dataStore,
					null,
					new QueryOptions(
							hailAdapter));

			long count = javaRdd.count();

			Assert.assertEquals(
					HAIL_COUNT,
					count);

			LOGGER.warn("DataStore loaded into RDD with " + count + " features for adapter "
					+ StringUtils.stringFromBinary(hailAdapter.getAdapterId().getBytes()));
		}
		catch (IOException e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			context.close();
			Assert.fail("Error occurred while loading RDD with adapter: '" + e.getLocalizedMessage() + "'");
		}

		// Load RDD using tornado adapter
		try {
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = GeoWaveRDD.rddForSimpleFeatures(
					context.sc(),
					dataStore,
					null,
					new QueryOptions(
							tornadoAdapter));

			javaRdd = javaRdd.distinct();
			long count = javaRdd.count();
			LOGGER.warn("DataStore loaded into RDD with " + count + " features for adapter "
					+ StringUtils.stringFromBinary(tornadoAdapter.getAdapterId().getBytes()));

			Assert.assertEquals(
					TORNADO_COUNT,
					count);
		}
		catch (IOException e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			context.close();
			Assert.fail("Error occurred while loading RDD with adapter: '" + e.getLocalizedMessage() + "'");
		}

		// Clean up
		TestUtils.deleteAll(dataStore);

		context.close();
	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}
}
