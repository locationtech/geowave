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

import static org.junit.Assert.*;

import java.io.File;

import javax.ws.rs.core.Response;

import org.junit.AfterClass;
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

	@BeforeClass
	public static void setup() {
		// ZipUtils.unZipFile(
		// new File(
		// GeoWaveServicesIT.class.getClassLoader().getResource(
		// TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
		// TestUtils.TEST_CASE_BASE);
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

	@Test
	public void testAddAccumuloStore() {

		configServiceClient.removeStore("astore"); // make sure that the store
													// does not exist
		Response firstAdd = configServiceClient.addAccumuloStore(
				"astore",
				"zookeeper",
				"instance",
				"user",
				"password");

		TestUtils.assert200(
				"This store should be written to config and return 200",
				firstAdd.getStatus());

		Response secondAdd = configServiceClient.addAccumuloStore(
				"astore",
				"zookeeper",
				"instance",
				"user",
				"password");

		TestUtils.assert400(
				"This should return 400, that store already exists",
				secondAdd.getStatus());
		configServiceClient.removeStore("astore"); // remove store at end of
													// successful test

	}

	@Test
	public void testAddHBaseStore() {

		configServiceClient.removeStore("hstore"); // make sure that the store
													// does not exist
		Response firstAdd = configServiceClient.addHBaseStore(
				"hstore",
				"zookeeper");

		TestUtils.assert200(
				"This store should be written to config and return 200",
				firstAdd.getStatus());

		Response secondAdd = configServiceClient.addHBaseStore(
				"hstore",
				"zookeeper");

		TestUtils.assert400(
				"This should return 400, that store already exists",
				secondAdd.getStatus());
		configServiceClient.removeStore("hstore"); // remove store at end of
													// successful test

	}

	@Test
	public void testAddBigTableStore() {

		configServiceClient.removeStore("bstore"); // make sure that the store
													// does not exist
		Response firstAdd = configServiceClient.addBigTableStore("bstore");

		TestUtils.assert200(
				"This store should be written to config and return 200",
				firstAdd.getStatus());

		Response secondAdd = configServiceClient.addBigTableStore("bstore");

		TestUtils.assert400(
				"This should return 400, that store already exists",
				secondAdd.getStatus());
		configServiceClient.removeStore("bstore"); // remove store at end of
													// successful test

	}

	@Test
	public void testAddSpatialIndex() {

		configServiceClient.removeIndex("spatial"); // make sure that the index
													// does not exist
		Response firstAdd = configServiceClient.addSpatialIndex("spatial");

		TestUtils.assert200(
				"This index should be written to config and return 200",
				firstAdd.getStatus());

		Response secondAdd = configServiceClient.addSpatialIndex("spatial");

		TestUtils.assert400(
				"This should return 400, that index already exists",
				secondAdd.getStatus());
		configServiceClient.removeIndex("spatial"); // remove index at end of
													// successful test

	}

	@Test
	public void testAddSpatialTemporalIndex() {

		configServiceClient.removeIndex("spatial-temporal"); // make sure that
																// the index
		// does not exist
		Response firstAdd = configServiceClient.addSpatialTemporalIndex("spatial-temporal");

		TestUtils.assert200(
				"This index should be written to config and return 200",
				firstAdd.getStatus());

		Response secondAdd = configServiceClient.addSpatialTemporalIndex("spatial-temporal");

		TestUtils.assert400(
				"This should return 400, that index already exists",
				secondAdd.getStatus());
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

		TestUtils.assert200(
				"This index group should be written to config and return 200",
				firstAdd.getStatus());

		Response secondAdd = configServiceClient.addIndexGroup(
				"test-group",
				indexes);

		TestUtils.assert400(
				"This should return 400, that index group already exists",
				secondAdd.getStatus());
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

		TestUtils.assert404(
				"This should return 404, one of the indexes listed does not exist",
				thirdAdd.getStatus());

		configServiceClient.removeIndexGroup("test-group"); // remove index
															// group at
															// end of
		// successful test
	}

	@Test
	public void testRemoveStore() {

		configServiceClient.addBigTableStore("test_remove_store"); // Create a
																	// store to
																	// test the
																	// remove
																	// command

		Response firstRemove = configServiceClient.removeStore("test_remove_store");
		TestUtils.assert200(
				"This store should be removed from config and return 200",
				firstRemove.getStatus());

		Response secondRemove = configServiceClient.removeStore("test_remove_store");

		TestUtils.assert404(
				"This should return 404, that store does not exist",
				secondRemove.getStatus());

	}

	@Test
	public void testRemoveIndex() {

		configServiceClient.addSpatialIndex("test_remove_index"); // Create an
																	// index to
																	// test the
																	// remove
																	// command

		Response firstRemove = configServiceClient.removeIndex("test_remove_index");
		TestUtils.assert200(
				"This index should be removed from config and return 200",
				firstRemove.getStatus());

		Response secondRemove = configServiceClient.removeIndex("test_remove_index");

		TestUtils.assert404(
				"This should return 404, that index does not exist",
				secondRemove.getStatus());

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
		TestUtils.assert200(
				"This index group should be removed from config and return 200",
				firstRemove.getStatus());

		Response secondRemove = configServiceClient.removeIndexGroup("test_remove_index_group");

		TestUtils.assert404(
				"This should return 404, that index group does not exist",
				secondRemove.getStatus());

	}

	@Test
	public void testHdfsConfig() {
		// Should always return 200
		Response config = configServiceClient.configHDFS("localhost:8020");
		TestUtils.assert200(
				"This should write to config and 200",
				config.getStatus());
	}

	@Test
	public void testSet() {
		// Should always return 200
		Response set = configServiceClient.set(
				"Property",
				"value");
		TestUtils.assert200(
				"This should write to config and 200",
				set.getStatus());
	}

	// Geoserver command does not yet work on the server
	@Test
	public void testConfigGeoServer() {

		Response configGeoserver = configServiceClient.configGeoServer("test-geoserver");
		TestUtils.assert200(
				"Should write the new GeoServer URL to config and return 200",
				configGeoserver.getStatus());

	}
}
