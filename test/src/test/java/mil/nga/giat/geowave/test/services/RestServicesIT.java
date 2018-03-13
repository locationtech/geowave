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

import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;

import mil.nga.giat.geowave.service.client.ConfigServiceClient;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.annotation.Environments;
import mil.nga.giat.geowave.test.annotation.Environments.Environment;


@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.SERVICES
})
public class RestServicesIT
{

	private static final Logger LOGGER = LoggerFactory.getLogger(RestServicesIT.class);
	private static ConfigServiceClient configServiceClient;

	// @GeoWaveTestStore({
	// GeoWaveStoreType.ACCUMULO,
	// GeoWaveStoreType.BIGTABLE,
	// GeoWaveStoreType.HBASE
	// })

	private static long startMillis;
	private final static String testName = "RestServicesIT";

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
	public void testAddHBaseStore() {
		configServiceClient.removeStore("hstore");
		Response r = configServiceClient.addHBaseStore(
				"hstore",
				"zooks2");

		assertEquals(
				r.getStatus(),
				200);
	}
}
