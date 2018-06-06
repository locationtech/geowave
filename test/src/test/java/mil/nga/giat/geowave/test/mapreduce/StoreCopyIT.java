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
package mil.nga.giat.geowave.test.mapreduce;

import java.io.File;
import java.net.URL;

import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.store.cli.config.AddStoreCommand;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.operations.CopyCommand;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.annotation.Environments;
import mil.nga.giat.geowave.test.annotation.Environments.Environment;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import mil.nga.giat.geowave.test.annotation.NamespaceOverride;
import mil.nga.giat.geowave.test.basic.AbstractGeoWaveBasicVectorIT;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.MAP_REDUCE
})
@GeoWaveTestStore({
	GeoWaveStoreType.ACCUMULO,
	GeoWaveStoreType.HBASE
})
public class StoreCopyIT extends
		AbstractGeoWaveBasicVectorIT
{
	private static final String HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE + "hail-box-filter.shp";
	private static final String TEST_BOX_FILTER_FILE = TEST_FILTER_PACKAGE + "Box-Filter.shp";

	@NamespaceOverride("storecopy")
	protected DataStorePluginOptions outputDataStorePluginOptions;

	protected DataStorePluginOptions inputDataStorePluginOptions;
	protected boolean testOutput = false;

	private final static Logger LOGGER = Logger.getLogger(StoreCopyIT.class);
	private static long startMillis;

	@BeforeClass
	public static void reportTestStart() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*         RUNNING StoreCopyIT           *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*      FINISHED StoreCopyIT             *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Test
	public void testStoreCopy()
			throws Exception {
		// Load some test data
		LOGGER.warn("Loading input data...");
		TestUtils.testLocalIngest(
				inputDataStorePluginOptions,
				DimensionalityType.SPATIAL,
				HAIL_SHAPEFILE_FILE,
				1);

		LOGGER.warn("Querying input data...");
		// Query the input store
		try {
			testQuery(
					new File(
							TEST_BOX_FILTER_FILE).toURI().toURL(),
					new URL[] {
						new File(
								HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE).toURI().toURL(),
					},
					TestUtils.DEFAULT_SPATIAL_INDEX,
					"bounding box constraint only");
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(inputDataStorePluginOptions);
			Assert.fail("Error occurred while querying the input store: '" + e.getLocalizedMessage() + "'");
		}

		LOGGER.warn("Execute storecopy job...");
		try {
			final MapReduceTestEnvironment env = MapReduceTestEnvironment.getInstance();

			// Set up the copy command
			final CopyCommand command = new CopyCommand();

			File configFile = File.createTempFile(
					"test_stats",
					null);
			ManualOperationParams params = new ManualOperationParams();

			params.getContext().put(
					ConfigOptions.PROPERTIES_FILE_CONTEXT,
					configFile);

			AddStoreCommand addStore = new AddStoreCommand();
			addStore.setParameters("test-store-in");
			addStore.setPluginOptions(inputDataStorePluginOptions);
			addStore.execute(params);
			addStore.setParameters("test-store-out");
			addStore.setPluginOptions(outputDataStorePluginOptions);
			addStore.execute(params);

			command.setParameters(
					"test-store-in",
					"test-store-out");

			command.getOptions().setHdfsHostPort(
					env.getHdfs());
			command.getOptions().setJobTrackerOrResourceManHostPort(
					env.getJobtracker());

			command.getOptions().setMinSplits(
					MapReduceTestUtils.MIN_INPUT_SPLITS);
			command.getOptions().setMaxSplits(
					MapReduceTestUtils.MAX_INPUT_SPLITS);
			command.getOptions().setNumReducers(
					8);

			ToolRunner.run(
					command.createRunner(params),
					new String[] {});
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(inputDataStorePluginOptions);
			Assert.fail("Error occurred while copying the datastore: '" + e.getLocalizedMessage() + "'");
		}

		LOGGER.warn("Querying output data...");
		// Query the copy store
		testOutput = true;

		try {
			testQuery(
					new File(
							TEST_BOX_FILTER_FILE).toURI().toURL(),
					new URL[] {
						new File(
								HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE).toURI().toURL(),
					},
					TestUtils.DEFAULT_SPATIAL_INDEX,
					"bounding box constraint only");
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(outputDataStorePluginOptions);
			Assert.fail("Error occurred while querying the output store: '" + e.getLocalizedMessage() + "'");
		}

		LOGGER.warn("Copy complete.");
	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return testOutput ? outputDataStorePluginOptions : inputDataStorePluginOptions;
	}
}
