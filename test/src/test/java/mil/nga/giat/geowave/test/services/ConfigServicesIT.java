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

import static org.junit.Assert.assertEquals;

import javax.ws.rs.core.Response;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
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
import mil.nga.giat.geowave.test.annotation.Environments.Environment;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
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
	private String spatialIndexName = "test-spatial-index-name";
	private String spatialTemporalIndexName = "test-spatial-temporal-index-name";
	private String indexGroupName = "test-indexGroup-name";

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
		configServiceClient.removeIndex(spatialIndexName);
		configServiceClient.removeIndex(spatialTemporalIndexName);
		configServiceClient.removeIndexGroup(indexGroupName);
		configServiceClient.removeIndexGroup(indexGroupName + "-bad");
	}

	@Test
	public void testAddStore() {
		TestUtils.assertStatusCode(
				"Unable to create store",
				200,
				configServiceClient.addStore(
						storeName,
						dataStorePluginOptions.getType(),
						null,
						dataStorePluginOptions.getOptionsAsMap()));

		TestUtils.assertStatusCode(
				"Should fail to create duplicate store",
				400,
				configServiceClient.addStore(
						storeName,
						dataStorePluginOptions.getType(),
						null,
						dataStorePluginOptions.getOptionsAsMap()));
	}

	@Test
	public void testAddSpatialIndex() {

		Response firstAdd = configServiceClient.addSpatialIndex(spatialIndexName);

		TestUtils.assertStatusCode(
				"Unable to create index",
				200,
				firstAdd);

		Response secondAdd = configServiceClient.addSpatialIndex(spatialIndexName);

		TestUtils.assertStatusCode(
				"Should fail to create duplicate index",
				400,
				secondAdd);

	}

	@Test
	public void testAddSpatialTemporalIndex() {

		Response firstAdd = configServiceClient.addSpatialTemporalIndex(spatialTemporalIndexName);

		TestUtils.assertStatusCode(
				"Unable to create index",
				200,
				firstAdd);

		Response secondAdd = configServiceClient.addSpatialTemporalIndex(spatialTemporalIndexName);

		TestUtils.assertStatusCode(
				"Should fail to create duplicate index",
				400,
				secondAdd);
	}

	@Test
	public void testAddIndexGroup() {

		configServiceClient.addSpatialIndex("index1");
		configServiceClient.addSpatialIndex("index2");
		String[] indexes = {
			"index1",
			"index2"
		};
		Response firstAdd = configServiceClient.addIndexGroup(
				indexGroupName,
				indexes);

		TestUtils.assertStatusCode(
				"Unable to create index group",
				200,
				firstAdd);

		Response secondAdd = configServiceClient.addIndexGroup(
				indexGroupName,
				indexes);

		TestUtils.assertStatusCode(
				"Should fail to create duplicate index group",
				400,
				secondAdd);

		String[] badIndexes = {
			"index1",
			"badIndex"
		};
		Response thirdAdd = configServiceClient.addIndexGroup(
				indexGroupName + "-bad",
				badIndexes);

		TestUtils.assertStatusCode(
				"This should return 404, one of the indexes listed does not exist",
				404,
				thirdAdd);
	}

	@Test
	public void testRemoveStore() {
		configServiceClient.addStore(
				"test_remove_store",
				dataStorePluginOptions.getType(),
				null,
				dataStorePluginOptions.getOptionsAsMap());

		Response firstRemove = configServiceClient.removeStore("test_remove_store");
		TestUtils.assertStatusCode(
				"Unable to remove store",
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

		configServiceClient.addSpatialIndex("test_remove_index");

		Response firstRemove = configServiceClient.removeIndex("test_remove_index");
		TestUtils.assertStatusCode(
				"Unable to remove index",
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
				indexes);

		Response firstRemove = configServiceClient.removeIndexGroup("test_remove_index_group");
		TestUtils.assertStatusCode(
				"Unable to remove index group",
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
				"Unable to configure HDFS",
				200,
				config);
	}

	@Test
	public void testSet()
			throws ParseException {
		// Should always return 200
		Response set = configServiceClient.set(
				"Property",
				"Value");
		TestUtils.assertStatusCode(
				"Unable to set property",
				200,
				set);
		String list = configServiceClient.list().readEntity(
				String.class);
		JSONParser parser = new JSONParser();
		JSONObject json = (JSONObject) parser.parse(list);
		JSONObject values = (JSONObject) json.get("data");

		// check to make sure that property was actually set
		assertEquals(
				"The property was not set correctly",
				"Value",
				(String) values.get("Property"));

	}

	@Test
	public void testList() {
		// Should always return 200
		Response list = configServiceClient.list();
		TestUtils.assertStatusCode(
				"Unable to return list",
				200,
				list);
	}

	@Test
	public void testConfigGeoServer()
			throws ParseException {
		// Should always return 200
		Response configGeoserver = configServiceClient.configGeoServer("test-geoserver");
		TestUtils.assertStatusCode(
				"Unable to configure GeoServer",
				200,
				configGeoserver);
		String list = configServiceClient.list().readEntity(
				String.class);
		JSONParser parser = new JSONParser();
		JSONObject json = (JSONObject) parser.parse(list);
		JSONObject values = (JSONObject) json.get("data");

		// check to make sure that geoserver was actually set
		assertEquals(
				"GeoServer was not set correctly",
				"test-geoserver",
				(String) values.get("geoserver.url"));
	}
}
