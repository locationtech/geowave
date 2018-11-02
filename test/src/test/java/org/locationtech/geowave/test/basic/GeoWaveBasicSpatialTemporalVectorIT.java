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
package org.locationtech.geowave.test.basic;

import java.io.File;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.vector.export.VectorLocalExportCommand;
import org.locationtech.geowave.adapter.vector.export.VectorLocalExportOptions;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.cli.config.AddStoreCommand;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveBasicSpatialTemporalVectorIT extends
		AbstractGeoWaveBasicVectorIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveBasicSpatialTemporalVectorIT.class);

	private static final String HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE
			+ "hail-box-temporal-filter.shp";
	private static final String HAIL_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE
			+ "hail-polygon-temporal-filter.shp";
	private static final String TORNADO_TRACKS_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE
			+ "tornado_tracks-box-temporal-filter.shp";
	private static final String TORNADO_TRACKS_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE
			+ "tornado_tracks-polygon-temporal-filter.shp";
	private static final String TEST_BOX_TEMPORAL_FILTER_FILE = TEST_FILTER_PACKAGE + "Box-Temporal-Filter.shp";
	private static final String TEST_POLYGON_TEMPORAL_FILTER_FILE = TEST_FILTER_PACKAGE + "Polygon-Temporal-Filter.shp";
	private static final String TEST_EXPORT_DIRECTORY = "export";
	private static final String TEST_BASE_EXPORT_FILE_NAME = "basicIT-export.avro";

	private static final SimpleDateFormat CQL_DATE_FORMAT = new SimpleDateFormat(
			"yyyy-MM-dd'T'hh:mm:ss'Z'");

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.CASSANDRA,
		GeoWaveStoreType.DYNAMODB,
		GeoWaveStoreType.HBASE,
		GeoWaveStoreType.REDIS
	})
	protected DataStorePluginOptions dataStore;
	private static long startMillis;
	private static final boolean POINTS_ONLY = false;
	private static final int NUM_THREADS = 4;

	@BeforeClass
	public static void reportTestStart() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------------");
		LOGGER.warn("*                                             *");
		LOGGER.warn("* RUNNING GeoWaveBasicSpatialTemporalVectorIT *");
		LOGGER.warn("*                                             *");
		LOGGER.warn("-----------------------------------------------");
	}

	@AfterClass
	public static void reportTestFinish() {
		LOGGER.warn("------------------------------------------------");
		LOGGER.warn("*                                              *");
		LOGGER.warn("* FINISHED GeoWaveBasicSpatialTemporalVectorIT *");
		LOGGER.warn("*                " + ((System.currentTimeMillis() - startMillis) / 1000)
				+ "s elapsed.                  *");
		LOGGER.warn("*                                              *");
		LOGGER.warn("------------------------------------------------");
	}

	@Test
	public void testIngestAndQuerySpatialTemporalPointsAndLines()
			throws Exception {
		// ingest both lines and points
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL_TEMPORAL,
				HAIL_SHAPEFILE_FILE,
				NUM_THREADS);

		if (!POINTS_ONLY) {
			TestUtils.testLocalIngest(
					dataStore,
					DimensionalityType.SPATIAL_TEMPORAL,
					TORNADO_TRACKS_SHAPEFILE_FILE,
					NUM_THREADS);
		}

		try {
			URL[] expectedResultsUrls;
			if (POINTS_ONLY) {
				expectedResultsUrls = new URL[] {
					new File(
							HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()
				};
			}
			else {
				expectedResultsUrls = new URL[] {
					new File(
							HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL(),
					new File(
							TORNADO_TRACKS_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()
				};
			}

			testQuery(
					new File(
							TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
					expectedResultsUrls,
					"bounding box and time range");
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing a bounding box and time range query of spatial temporal index: '"
					+ e.getLocalizedMessage() + "'");
		}

		try {
			URL[] expectedResultsUrls;
			if (POINTS_ONLY) {
				expectedResultsUrls = new URL[] {
					new File(
							HAIL_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()
				};
			}
			else {
				expectedResultsUrls = new URL[] {
					new File(
							HAIL_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL(),
					new File(
							TORNADO_TRACKS_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()
				};
			}

			testQuery(
					new File(
							TEST_POLYGON_TEMPORAL_FILTER_FILE).toURI().toURL(),
					expectedResultsUrls,
					"polygon constraint and time range");
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing a polygon and time range query of spatial temporal index: '"
					+ e.getLocalizedMessage() + "'");
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
					TestUtils.DEFAULT_SPATIAL_TEMPORAL_INDEX,
					(NUM_THREADS > 1));
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing a bounding box stats on spatial temporal index: '"
					+ e.getLocalizedMessage() + "'");
		}

		try {
			testSpatialTemporalLocalExportAndReingestWithCQL(new File(
					TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL());
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing deletion of an entry using spatial index: '"
					+ e.getLocalizedMessage() + "'");
		}

		try {
			testDeleteDataId(
					new File(
							TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
					TestUtils.DEFAULT_SPATIAL_TEMPORAL_INDEX);
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing deletion of an entry using spatial temporal index: '"
					+ e.getLocalizedMessage() + "'");
		}

		TestUtils.deleteAll(dataStore);
	}

	private void testSpatialTemporalLocalExportAndReingestWithCQL(
			final URL filterURL )
			throws Exception {

		final SimpleFeature savedFilter = TestUtils.resourceToFeature(filterURL);

		final Geometry filterGeometry = (Geometry) savedFilter.getDefaultGeometry();
		final Object startObj = savedFilter.getAttribute(TestUtils.TEST_FILTER_START_TIME_ATTRIBUTE_NAME);
		final Object endObj = savedFilter.getAttribute(TestUtils.TEST_FILTER_END_TIME_ATTRIBUTE_NAME);
		Date startDate = null, endDate = null;
		if ((startObj != null) && (endObj != null)) {
			// if we can resolve start and end times, make it a spatial temporal
			// query
			if (startObj instanceof Calendar) {
				startDate = ((Calendar) startObj).getTime();
			}
			else if (startObj instanceof Date) {
				startDate = (Date) startObj;
			}
			if (endObj instanceof Calendar) {
				endDate = ((Calendar) endObj).getTime();
			}
			else if (endObj instanceof Date) {
				endDate = (Date) endObj;
			}
		}
		final PersistentAdapterStore adapterStore = dataStore.createAdapterStore();
		final VectorLocalExportCommand exportCommand = new VectorLocalExportCommand();
		final VectorLocalExportOptions options = exportCommand.getOptions();
		final File exportDir = new File(
				TestUtils.TEMP_DIR,
				TEST_EXPORT_DIRECTORY);
		exportDir.delete();
		exportDir.mkdirs();

		exportCommand.setParameters("test");

		final File configFile = File.createTempFile(
				"test_export",
				null);
		final ManualOperationParams params = new ManualOperationParams();

		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);
		final AddStoreCommand addStore = new AddStoreCommand();
		addStore.setParameters("test");
		addStore.setPluginOptions(dataStore);
		addStore.execute(params);
		options.setBatchSize(10000);
		final Envelope env = filterGeometry.getEnvelopeInternal();
		final double east = env.getMaxX();
		final double west = env.getMinX();
		final double south = env.getMinY();
		final double north = env.getMaxY();
		try (CloseableIterator<InternalDataAdapter<?>> adapterIt = adapterStore.getAdapters()) {
			while (adapterIt.hasNext()) {
				final InternalDataAdapter<?> adapter = adapterIt.next();
				options.setTypeNames(new String[] {
					adapter.getTypeName()
				});
				if (adapter.getAdapter() instanceof GeotoolsFeatureDataAdapter) {
					final GeotoolsFeatureDataAdapter gtAdapter = (GeotoolsFeatureDataAdapter) adapter.getAdapter();
					final TimeDescriptors timeDesc = gtAdapter.getTimeDescriptors();

					String startTimeAttribute;
					if (timeDesc.getStartRange() != null) {
						startTimeAttribute = timeDesc.getStartRange().getLocalName();
					}
					else {
						startTimeAttribute = timeDesc.getTime().getLocalName();
					}
					final String endTimeAttribute;
					if (timeDesc.getEndRange() != null) {
						endTimeAttribute = timeDesc.getEndRange().getLocalName();
					}
					else {
						endTimeAttribute = timeDesc.getTime().getLocalName();
					}
					final String geometryAttribute = gtAdapter.getFeatureType().getGeometryDescriptor().getLocalName();

					final String cqlPredicate = String.format(
							"BBOX(\"%s\",%f,%f,%f,%f) AND \"%s\" <= '%s' AND \"%s\" >= '%s'",
							geometryAttribute,
							west,
							south,
							east,
							north,
							startTimeAttribute,
							CQL_DATE_FORMAT.format(endDate),
							endTimeAttribute,
							CQL_DATE_FORMAT.format(startDate));
					options.setOutputFile(new File(
							exportDir,
							adapter.getTypeName() + TEST_BASE_EXPORT_FILE_NAME));
					options.setCqlFilter(cqlPredicate);
					exportCommand.execute(params);
				}
			}
		}
		TestUtils.deleteAll(dataStore);
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL_TEMPORAL,
				exportDir.getAbsolutePath(),
				"avro",
				NUM_THREADS);
		try {
			URL[] expectedResultsUrls;
			if (POINTS_ONLY) {
				expectedResultsUrls = new URL[] {
					new File(
							HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()
				};
			}
			else {
				expectedResultsUrls = new URL[] {
					new File(
							HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL(),
					new File(
							TORNADO_TRACKS_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()
				};
			}

			testQuery(
					new File(
							TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
					expectedResultsUrls,
					"reingested bounding box and time range");
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert
					.fail("Error occurred on reingested dataset while testing a bounding box and time range query of spatial temporal index: '"
							+ e.getLocalizedMessage() + "'");
		}
	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}
}
