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
package mil.nga.giat.geowave.test.services;

import java.io.IOException;

import javax.ws.rs.ProcessingException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.service.client.FileUploadServiceClient;
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
public class FileUploadIT
{
	private static final Logger LOGGER = LoggerFactory.getLogger(FileUploadIT.class);

	private static FileUploadServiceClient fileUploadServiceClient;

	private final static String testName = "FileUploadIT";

	private static long startMillis;

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE,
		GeoWaveStoreType.CASSANDRA,
		GeoWaveStoreType.DYNAMODB
	})
	protected DataStorePluginOptions dataStoreOptions;

	@BeforeClass
	public static void startTimer() {
		startMillis = System.currentTimeMillis();
		TestUtils.printStartOfTest(
				LOGGER,
				testName);

		fileUploadServiceClient = new FileUploadServiceClient(
				ServicesTestEnvironment.GEOWAVE_BASE_URL);
	}

	@AfterClass
	public static void reportTest() {
		TestUtils.printEndOfTest(
				LOGGER,
				testName,
				startMillis);
	}

	@Test
	public void fileUpload() {
		TestUtils.assertStatusCode(
				"Should succeed for valid file path",
				201,
				fileUploadServiceClient.uploadFile("data/osm_gpx_test_case/public/000/992/000992764.gpx"));
	}

	@Test(expected = NullPointerException.class)
	public void fileUploadNull() {
		fileUploadServiceClient.uploadFile(null);
	}

	@Test(expected = ProcessingException.class)
	public void fileUploadDirectory() {
		fileUploadServiceClient.uploadFile("data/osm_gpx_test_case");
	}
}
