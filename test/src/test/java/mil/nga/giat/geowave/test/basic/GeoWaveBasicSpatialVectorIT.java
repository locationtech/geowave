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
package mil.nga.giat.geowave.test.basic;

import java.io.File;
import java.net.URL;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveBasicSpatialVectorIT extends
		AbstractGeoWaveBasicVectorIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveBasicSpatialVectorIT.class);
	private static final String HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE + "hail-box-filter.shp";
	private static final String HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE
			+ "hail-polygon-filter.shp";

	private static final String TORNADO_TRACKS_EXPECTED_BOX_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE
			+ "tornado_tracks-box-filter.shp";
	private static final String TORNADO_TRACKS_EXPECTED_POLYGON_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE
			+ "tornado_tracks-polygon-filter.shp";

	private static final String TEST_BOX_FILTER_FILE = TEST_FILTER_PACKAGE + "Box-Filter.shp";
	private static final String TEST_POLYGON_FILTER_FILE = TEST_FILTER_PACKAGE + "Polygon-Filter.shp";
	private static final String CQL_DELETE_STR = "STATE = 'TX'";

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.CASSANDRA,
		GeoWaveStoreType.DYNAMODB,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStore;

	private static long startMillis;
	private static final boolean POINTS_ONLY = false;
	private static final int NUM_THREADS = 1;

	@BeforeClass
	public static void reportTestStart() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*  RUNNING GeoWaveBasicSpatialVectorIT  *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTestFinish() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED GeoWaveBasicSpatialVectorIT  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Test
	public void testMultiThreadedIngestAndQuerySpatialPointsAndLines()
			throws Exception {
		testIngestAndQuerySpatialPointsAndLines(NUM_THREADS);
	}

	public void testIngestAndQuerySpatialPointsAndLines(
			final int nthreads )
			throws Exception {
		long mark = System.currentTimeMillis();

		LOGGER.debug("Testing DataStore Type: " + dataStore.getType());

		// Ensure empty datastore
		TestUtils.deleteAll(dataStore);

		// ingest both lines and points
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				HAIL_SHAPEFILE_FILE,
				nthreads);

		long dur = (System.currentTimeMillis() - mark);
		LOGGER.debug("Ingest (points) duration = " + dur + " ms with " + nthreads + " thread(s).");

		if (!POINTS_ONLY) {
			mark = System.currentTimeMillis();

			TestUtils.testLocalIngest(
					dataStore,
					DimensionalityType.SPATIAL,
					TORNADO_TRACKS_SHAPEFILE_FILE,
					nthreads);

			dur = (System.currentTimeMillis() - mark);
			LOGGER.debug("Ingest (lines) duration = " + dur + " ms with " + nthreads + " thread(s).");
		}

		try {
			mark = System.currentTimeMillis();

			URL[] expectedResultsUrls;
			if (POINTS_ONLY) {
				expectedResultsUrls = new URL[] {
					new File(
							HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE).toURI().toURL()
				};
			}
			else {
				expectedResultsUrls = new URL[] {
					new File(
							HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE).toURI().toURL(),
					new File(
							TORNADO_TRACKS_EXPECTED_BOX_FILTER_RESULTS_FILE).toURI().toURL()
				};
			}

			testQuery(
					new File(
							TEST_BOX_FILTER_FILE).toURI().toURL(),
					expectedResultsUrls,
					TestUtils.DEFAULT_SPATIAL_INDEX,
					"bounding box constraint only");

			dur = (System.currentTimeMillis() - mark);
			LOGGER.debug("BBOX query duration = " + dur + " ms.");
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing a bounding box query of spatial index: '"
					+ e.getLocalizedMessage() + "'");
		}
		try {
			mark = System.currentTimeMillis();

			URL[] expectedResultsUrls;
			if (POINTS_ONLY) {
				expectedResultsUrls = new URL[] {
					new File(
							HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE).toURI().toURL()
				};
			}
			else {
				expectedResultsUrls = new URL[] {
					new File(
							HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE).toURI().toURL(),
					new File(
							TORNADO_TRACKS_EXPECTED_POLYGON_FILTER_RESULTS_FILE).toURI().toURL()
				};
			}

			testQuery(
					new File(
							TEST_POLYGON_FILTER_FILE).toURI().toURL(),
					expectedResultsUrls,
					TestUtils.DEFAULT_SPATIAL_INDEX,
					"polygon constraint only");

			dur = (System.currentTimeMillis() - mark);
			LOGGER.debug("POLY query duration = " + dur + " ms.");
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing a polygon query of spatial index: '" + e.getLocalizedMessage()
					+ "'");
		}
		try {
			URL[] statsInputs;
			if (POINTS_ONLY) {
				statsInputs = new URL[] {
					new File(
							HAIL_SHAPEFILE_FILE).toURI().toURL()
				};
			}
			else {
				statsInputs = new URL[] {
					new File(
							HAIL_SHAPEFILE_FILE).toURI().toURL(),
					new File(
							TORNADO_TRACKS_SHAPEFILE_FILE).toURI().toURL()
				};
			}

			testStats(
					statsInputs,
					TestUtils.DEFAULT_SPATIAL_INDEX,
					true);
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing a bounding box stats on spatial index: '"
					+ e.getLocalizedMessage() + "'");
		}

		try {
			testDeleteCQL(
					CQL_DELETE_STR,
					TestUtils.DEFAULT_SPATIAL_INDEX);
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing deletion of an entry using spatial index: '"
					+ e.getLocalizedMessage() + "'");
		}

		try {
			testDeleteSpatial(
					new File(
							TEST_POLYGON_FILTER_FILE).toURI().toURL(),
					TestUtils.DEFAULT_SPATIAL_INDEX);
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing deletion of an entry using spatial index: '"
					+ e.getLocalizedMessage() + "'");
		}

		TestUtils.deleteAll(dataStore);
	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}
}
