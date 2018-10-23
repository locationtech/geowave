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
package org.locationtech.geowave.test.services;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.service.client.AnalyticServiceClient;
import org.locationtech.geowave.service.client.ConfigServiceClient;
import org.locationtech.geowave.service.client.GeoServerServiceClient;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.services.ServicesTestEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.SERVICES
})
public class AnalyticIT
{
	private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticIT.class);
	private static final String WFS_URL_PREFIX = ServicesTestEnvironment.JETTY_BASE_URL + "/geoserver/wfs";

	private static final String GEOSTUFF_LAYER_FILE = "src/test/resources/wfs-requests/geostuff_layer.xml";
	private static final String INSERT_FILE = "src/test/resources/wfs-requests/insert.xml";
	private static final String LOCK_FILE = "src/test/resources/wfs-requests/lock.xml";
	private static final String QUERY_FILE = "src/test/resources/wfs-requests/query.xml";
	private static final String UPDATE_FILE = "src/test/resources/wfs-requests/update.xml";

	private AnalyticServiceClient analyticServiceClient;

	private String input_storename;
	private String output_storename;

	private final static String testName = "AnalyticIT";

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE,
		GeoWaveStoreType.CASSANDRA,
		GeoWaveStoreType.DYNAMODB,
		GeoWaveStoreType.REDIS
	})
	protected DataStorePluginOptions dataStoreOptions;

	private static long startMillis;

	@BeforeClass
	public static void startTimer() {
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
	public void initialize() {
		// Perform ingest operations here, so there is data on which to run the
		// analytics.
	}

	@After
	public void cleanupWorkspace() {
		// Remove everything created in the @Before method, so each test starts
		// with a clean slate.

		// If confident the initialization data does not change during the test,
		// you may move the setup/tear down actions to the @BeforeClass and
		// @AfterClass methods.
	}

	@Test
	@Ignore
	public void example() {
		// Tests should contain calls to the REST services methods, checking
		// them for proper response and status codes.

		// Use this method to check:

		TestUtils.assertStatusCode(
				"Should Successfully <Insert Objective Here>",
				200,
				analyticServiceClient.kmeansSpark(
						input_storename,
						output_storename));
	}

	@Test
	@Ignore
	public void dbScan() {
		// TODO: Implement this test
	}

	@Test
	@Ignore
	public void kde() {
		// TODO: Implement this test
	}

	@Test
	@Ignore
	public void kmeansspark() {
		// TODO: Implement this test
	}

	@Test
	@Ignore
	public void nn() {
		// TODO: Implement this test
	}

	@Test
	@Ignore
	public void spatialjoin() {
		// TODO: Implement this test
	}

	@Test
	@Ignore
	public void sql() {
		// TODO: Implement this test
	}

}
