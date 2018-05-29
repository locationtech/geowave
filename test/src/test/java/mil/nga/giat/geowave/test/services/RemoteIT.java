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
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.examples.ingest.SimpleIngest;
import mil.nga.giat.geowave.service.client.AnalyticServiceClient;
import mil.nga.giat.geowave.service.client.ConfigServiceClient;
import mil.nga.giat.geowave.service.client.GeoServerServiceClient;
import mil.nga.giat.geowave.service.client.RemoteServiceClient;
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
public class RemoteIT
{
	private static final Logger LOGGER = LoggerFactory.getLogger(RemoteIT.class);
	private static final String WFS_URL_PREFIX = ServicesTestEnvironment.JETTY_BASE_URL + "/geoserver/wfs";

	private static final String GEOSTUFF_LAYER_FILE = "src/test/resources/wfs-requests/geostuff_layer.xml";
	private static final String INSERT_FILE = "src/test/resources/wfs-requests/insert.xml";
	private static final String LOCK_FILE = "src/test/resources/wfs-requests/lock.xml";
	private static final String QUERY_FILE = "src/test/resources/wfs-requests/query.xml";
	private static final String UPDATE_FILE = "src/test/resources/wfs-requests/update.xml";
	private static ConfigServiceClient configServiceClient;

	private RemoteServiceClient remoteServiceClient;

	private String store_name;

	private final static String testName = "RemoteIT";

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
	 	GeoWaveStoreType.HBASE,
	 	GeoWaveStoreType.CASSANDRA,
	 	GeoWaveStoreType.DYNAMODB
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
	public void initialize()
			throws MismatchedIndexToAdapterMapping,
			IOException {
		remoteServiceClient = new RemoteServiceClient(
				ServicesTestEnvironment.GEOWAVE_BASE_URL);
		configServiceClient = new ConfigServiceClient(
				ServicesTestEnvironment.GEOWAVE_BASE_URL);
		startMillis = System.currentTimeMillis();
		TestUtils.printStartOfTest(
				LOGGER,
				testName);

		final DataStore ds = dataStoreOptions.createDataStore();
		final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();
		final PrimaryIndex idx = SimpleIngest.createSpatialIndex();
		final GeotoolsFeatureDataAdapter fda = SimpleIngest.createDataAdapter(sft);
		final List<SimpleFeature> features = SimpleIngest.getGriddedFeatures(
				new SimpleFeatureBuilder(
						sft),
				8675309);
		LOGGER.info(String.format(
				"Beginning to ingest a uniform grid of %d features",
				features.size()));
		int ingestedFeatures = 0;
		final int featuresPer5Percent = features.size() / 20;
		try (IndexWriter writer = ds.createWriter(
				fda,
				idx)) {
			for (final SimpleFeature feat : features) {
				writer.write(feat);
				ingestedFeatures++;
				if ((ingestedFeatures % featuresPer5Percent) == 0) {
					LOGGER.info(String.format(
							"Ingested %d percent of features",
							(ingestedFeatures / featuresPer5Percent) * 5));
				}
			}
		}
		configServiceClient.addStore(
				TestUtils.TEST_NAMESPACE,
				dataStoreOptions.getType(),
				TestUtils.TEST_NAMESPACE,
				dataStoreOptions.getOptionsAsMap());
		configServiceClient.addStore(
				"test",
				dataStoreOptions.getType(),
				TestUtils.TEST_NAMESPACE,
				dataStoreOptions.getOptionsAsMap());
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
				remoteServiceClient.listAdapter(store_name));
	}

	@Test
	@Ignore
	public void calcstat() {
		// TODO: Implement this test
	}

	@Test
	@Ignore
	public void clear() {
		// TODO: Implement this test
	}

	@Test
	@Ignore
	public void listadapter() {
		// TODO: Implement this test
	}

	@Test
	@Ignore
	public void listindex() {
		// TODO: Implement this test
	}

	@Test
	public void liststats() {
		TestUtils.assertStatusCode(
				"Should Successfully <Insert Objective Here>",
				200,
				remoteServiceClient.listStats("test"));
		// TODO: Make calls to non-existent stores return something other than 500
	}

	@Test
	@Ignore
	public void recalcstats() {
		// TODO: Implement this test
	}

	@Test
	@Ignore
	public void rmadapter() {
		// TODO: Implement this test
	}

	@Test
	@Ignore
	public void rmstat() {
		// TODO: Implement this test
	}

	@Test
	@Ignore
	public void version() {
		// TODO: Implement this test
	}

}
