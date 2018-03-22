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
package mil.nga.giat.geowave.test.services;

import javax.ws.rs.core.Response;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.service.client.ConfigServiceClient;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.annotation.Environments;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.Environments.Environment;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.SERVICES
})
public class ConfigServicesIT
{

	private static final Logger LOGGER = LoggerFactory.getLogger(ConfigServicesIT.class);
	private static ConfigServiceClient configServiceClient;

	@GeoWaveTestStore({
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStorePluginOptions;

	private static long startMillis;
	private final static String testName = "ConfigServicesIT";

	private String storeName = "test-store-name";

	@BeforeClass
	public static void setup() {
		configServiceClient = new ConfigServiceClient(
				ServicesTestEnvironment.GEOWAVE_BASE_URL);
		startMillis = System.currentTimeMillis();
		TestUtils.printStartOfTest(
				LOGGER,
				testName);
	}

	@AfterClass
	public static void reportTest() {
		TestUtils.printEndOfTest(
				LOGGER,
				testName,
				startMillis);
	}

	@Before
	public void before() {
		// remove any Geowave objects that may interfere with tests.
		configServiceClient.removeStore(storeName);
	}

	@Test
	public void testAddStore() {
		TestUtils.assertStatusCode(
				"Create Store",
				200,
				configServiceClient.addStore(
						storeName,
						dataStorePluginOptions.getType(),
						null,
						dataStorePluginOptions.getOptionsAsMap()));

		TestUtils.assertStatusCode(
				"Create Duplicate Store",
				400,
				configServiceClient.addStore(
						storeName,
						dataStorePluginOptions.getType(),
						null,
						dataStorePluginOptions.getOptionsAsMap()));
	}

	@Test
	public void testAddSpatialIndex() {

		configServiceClient.removeIndex("spatial"); // make sure that the index
													// does not exist
		Response firstAdd = configServiceClient.addSpatialIndex("spatial");

		TestUtils.assertStatusCode(
				"This index should be written to config and return 200",
				200,
				firstAdd);

		Response secondAdd = configServiceClient.addSpatialIndex("spatial");

		TestUtils.assertStatusCode(
				"This should return 400, that index already exists",
				400,
				secondAdd);
		configServiceClient.removeIndex("spatial"); // remove index at end of
													// successful test

	}

	@Test
	public void testAddSpatialTemporalIndex() {

		configServiceClient.removeIndex("spatial-temporal"); // make sure that
																// the index
		// does not exist
		Response firstAdd = configServiceClient.addSpatialTemporalIndex("spatial-temporal");

		TestUtils.assertStatusCode(
				"This index should be written to config and return 200",
				200,
				firstAdd);

		Response secondAdd = configServiceClient.addSpatialTemporalIndex("spatial-temporal");

		TestUtils.assertStatusCode(
				"This should return 400, that index already exists",
				400,
				secondAdd);
		configServiceClient.removeIndex("spatial-temporal"); // remove index at
																// end of
		// successful test

	}

	@Test
	public void testAddIndexGroup() {

		configServiceClient.removeIndexGroup("test-group"); // make sure that
															// the index group
															// does not exist
		configServiceClient.addSpatialIndex("index1");
		configServiceClient.addSpatialIndex("index2");
		String[] indexes = {
			"index1",
			"index2"
		};
		Response firstAdd = configServiceClient.addIndexGroup(
				"test-group",
				indexes);

		TestUtils.assertStatusCode(
				"This index group should be written to config and return 200",
				200,
				firstAdd);

		Response secondAdd = configServiceClient.addIndexGroup(
				"test-group",
				indexes);

		TestUtils.assertStatusCode(
				"This should return 400, that index group already exists",
				400,
				secondAdd);
		configServiceClient.removeIndexGroup("test-group"); // remove index
															// group at
															// end of
		// successful test
		String[] badIndexes = {
			"index1",
			"badIndex"
		};
		Response thirdAdd = configServiceClient.addIndexGroup(
				"test-group",
				badIndexes);

		TestUtils.assertStatusCode(
				"This should return 404, one of the indexes listed does not exist",
				404,
				thirdAdd);

		configServiceClient.removeIndexGroup("test-group"); // remove index
															// group at
															// end of
		// successful test
	}

	@Test
	public void testRemoveStore() {
		// Create a store to test the remove command
		configServiceClient.addStore(
				"test_remove_store",
				dataStorePluginOptions.getType(),
				null,
				dataStorePluginOptions.getOptionsAsMap());

		Response firstRemove = configServiceClient.removeStore("test_remove_store");
		TestUtils.assertStatusCode(
				"This store should be removed from config and return 200",
				200,
				firstRemove);

		Response secondRemove = configServiceClient.removeStore("test_remove_store");

		TestUtils.assertStatusCode(
				"This should return 404, that store does not exist",
				404,
				secondRemove);

	}

	@Test
	public void testRemoveIndex() {

		configServiceClient.addSpatialIndex("test_remove_index"); // Create an
																	// index to
																	// test the
																	// remove
																	// command

		Response firstRemove = configServiceClient.removeIndex("test_remove_index");
		TestUtils.assertStatusCode(
				"This index should be removed from config and return 200",
				200,
				firstRemove);

		Response secondRemove = configServiceClient.removeIndex("test_remove_index");

		TestUtils.assertStatusCode(
				"This should return 404, that index does not exist",
				404,
				secondRemove);

	}

	@Test
	public void testRemoveIndexGroup() {
		configServiceClient.addSpatialIndex("index1");
		configServiceClient.addSpatialIndex("index2");
		String[] indexes = {
			"index1",
			"index2"
		};
		configServiceClient.addIndexGroup(
				"test_remove_index_group",
				indexes); // Create an
		// index group to
		// test the
		// remove
		// command

		Response firstRemove = configServiceClient.removeIndexGroup("test_remove_index_group");
		TestUtils.assertStatusCode(
				"This index group should be removed from config and return 200",
				200,
				firstRemove);

		Response secondRemove = configServiceClient.removeIndexGroup("test_remove_index_group");

		TestUtils.assertStatusCode(
				"This should return 404, that index group does not exist",
				404,
				secondRemove);

	}

	@Test
	public void testHdfsConfig() {
		// Should always return 200
		Response config = configServiceClient.configHDFS("localhost:8020");
		TestUtils.assertStatusCode(
				"This should write to config and 200",
				200,
				config);
	}

	@Test
	public void testSet() {
		// Should always return 200
		Response set = configServiceClient.set(
				"Property",
				"value");
		TestUtils.assertStatusCode(
				"This should write to config and 200",
				200,
				set);
	}

	// Geoserver command does not yet work on the server
	@Test
	public void testConfigGeoServer() {

		Response configGeoserver = configServiceClient.configGeoServer("test-geoserver");
		TestUtils.assertStatusCode(
				"Should write the new GeoServer URL to config and return 200",
				200,
				configGeoserver);

	}
}
