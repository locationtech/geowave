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

	private String store_name = TestUtils.TEST_NAMESPACE;

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
	}

	@After
	public void cleanupWorkspace() {
		configServiceClient.removeStore(TestUtils.TEST_NAMESPACE);
	}

	@Test
	@Ignore
	public void calcstat() {
		// TODO: Implement this test
	}

	@Test
	public void clear1() {
		TestUtils.assertStatusCode(
				"Should successfully clear for existent store", 
				200, 
				remoteServiceClient.clear(TestUtils.TEST_NAMESPACE));
		TestUtils.assertStatusCode(
				"Should fail to clear for nonexistent store", 
				404, 
				remoteServiceClient.clear("nonexistent-store"));
	}
	
	@Test
	public void clear2() {
		TestUtils.assertStatusCode(
				"Should fail to clear for nonexistent store", 
				404, 
				remoteServiceClient.clear("nonexistent-store"));
		TestUtils.assertStatusCode(
				"Should successfully clear for existent store", 
				200, 
				remoteServiceClient.clear(TestUtils.TEST_NAMESPACE));
	}

	@Test
	public void listadapter1() {
		TestUtils.assertStatusCode(
				"Should successfully list adapters for existent store", 
				200, 
				remoteServiceClient.listAdapter(TestUtils.TEST_NAMESPACE));
		TestUtils.assertStatusCode(
				"Should fail to list adapters for nonexistent store", 
				404, 
				remoteServiceClient.listAdapter("nonexistent-store"));
	}
	
	@Test
	public void listadapter2() {
		TestUtils.assertStatusCode(
				"Should fail to list adapters for nonexistent store", 
				404, 
				remoteServiceClient.listAdapter("nonexistent-store"));
		TestUtils.assertStatusCode(
				"Should successfully list adapters for existent store", 
				200, 
				remoteServiceClient.listAdapter(TestUtils.TEST_NAMESPACE));
	}

	@Test
	public void listindex1() {
		TestUtils.assertStatusCode(
				"Should fail to list indices for nonexistent store", 
				404, 
				remoteServiceClient.listIndex("nonexistent-store"));
		TestUtils.assertStatusCode(
				"Should successfully list indices for existent store", 
				200, 
				remoteServiceClient.listIndex(TestUtils.TEST_NAMESPACE));
	}
	
	@Test
	public void listindex2() {
		TestUtils.assertStatusCode(
				"Should successfully list indices for existent store", 
				200, 
				remoteServiceClient.listIndex(TestUtils.TEST_NAMESPACE));
		TestUtils.assertStatusCode(
				"Should fail to list indices for nonexistent store", 
				404, 
				remoteServiceClient.listIndex("nonexistent-store"));
	}

	@Test
	public void liststats1() {
		TestUtils.assertStatusCode(
				"Should successfully liststats for existent store",
				200,
				remoteServiceClient.listStats(TestUtils.TEST_NAMESPACE));
		TestUtils.assertStatusCode(
				"Should fail to liststats for nonexistent store",
				404,
				remoteServiceClient.listStats("nonexistent-store"));
	}
	
	@Test
	public void liststats2() {
		TestUtils.assertStatusCode(
				"Should fail to liststats for nonexistent store",
				404,
				remoteServiceClient.listStats("nonexistent-store"));
		TestUtils.assertStatusCode(
				"Should successfully liststats for existent store",
				200,
				remoteServiceClient.listStats(TestUtils.TEST_NAMESPACE));
	}

	@Test
	@Ignore
	public void recalcstats() {
		// TODO: Implement this test
	}

	@Test
	public void rmadapter1() {
		TestUtils.assertStatusCode(
				"Should fail to remove adapter for nonexistent store",
				404,
				remoteServiceClient.removeAdapter("nonexistent-store", ""));
		TestUtils.assertStatusCode(
				"Should successfully remove adapter for existent store and existent adapter",
				200,
				remoteServiceClient.removeAdapter(TestUtils.TEST_NAMESPACE, "GridPoint"));
		TestUtils.assertStatusCode(
				"Should fail to remove adapter for existent store and nonexistent adapter",
				404,
				remoteServiceClient.removeAdapter(TestUtils.TEST_NAMESPACE, "nonexistent-adapter"));
	}
	
	@Test
	public void rmadapter2() {
		TestUtils.assertStatusCode(
				"Should successfully remove adapter for existent store and existent adapter",
				200,
				remoteServiceClient.removeAdapter(TestUtils.TEST_NAMESPACE, "GridPoint"));
		TestUtils.assertStatusCode(
				"Should fail to remove an already removed adapter",
				400,
				remoteServiceClient.removeAdapter(TestUtils.TEST_NAMESPACE, "GridPoint"));
	}
	
	@Test
	@Ignore
	public void rmstat() {
		// TODO: Implement this test
	}

	@Test
	public void version1() {
		TestUtils.assertStatusCode(
				"Should successfully return version for existent store",
				200,
				remoteServiceClient.version(TestUtils.TEST_NAMESPACE));
		
		TestUtils.assertStatusCode(
				"Should fail to return version for nonexistent store",
				200,
				remoteServiceClient.version("nonexistent-store"));
	}
	
	@Test
	public void version2() {
		TestUtils.assertStatusCode(
				"Should fail to return version for nonexistent store",
				200,
				remoteServiceClient.version("nonexistent-store"));
		TestUtils.assertStatusCode(
				"Should successfully return version for existent store",
				200,
				remoteServiceClient.version(TestUtils.TEST_NAMESPACE));
	}

}
