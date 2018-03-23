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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.service.client.ConfigServiceClient;
import mil.nga.giat.geowave.service.client.GeoServerServiceClient;
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
public class GeoServerIT
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoServerIT.class);
	private static final String WFS_URL_PREFIX = ServicesTestEnvironment.JETTY_BASE_URL + "/geoserver/wfs";

	private static final String GEOSTUFF_LAYER_FILE = "src/test/resources/wfs-requests/geostuff_layer.xml";
	private static final String INSERT_FILE = "src/test/resources/wfs-requests/insert.xml";
	private static final String LOCK_FILE = "src/test/resources/wfs-requests/lock.xml";
	private static final String QUERY_FILE = "src/test/resources/wfs-requests/query.xml";
	private static final String UPDATE_FILE = "src/test/resources/wfs-requests/update.xml";

	private GeoServerServiceClient geoServerServiceClient;
	private ConfigServiceClient configServiceClient;
	private String geostuff_layer;
	private String insert;
	private String lock;
	private String query;
	private String update;

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE
	}, options = {
		"disableServer=true",
	})
	protected DataStorePluginOptions dataStoreOptions;

	private static long startMillis;

	@BeforeClass
	public static void startTimer() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*         RUNNING GeoServerIT           *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*      FINISHED GeoServerIT             *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Before
	public void initialize()
			throws ClientProtocolException,
			IOException {
		// setup the wfs-requests
		geostuff_layer = MessageFormat.format(
				IOUtils.toString(new FileInputStream(
						GEOSTUFF_LAYER_FILE)),
				ServicesTestEnvironment.TEST_WORKSPACE);

		insert = MessageFormat.format(
				IOUtils.toString(new FileInputStream(
						INSERT_FILE)),
				ServicesTestEnvironment.TEST_WORKSPACE);

		lock = MessageFormat.format(
				IOUtils.toString(new FileInputStream(
						LOCK_FILE)),
				ServicesTestEnvironment.TEST_WORKSPACE);

		query = MessageFormat.format(
				IOUtils.toString(new FileInputStream(
						QUERY_FILE)),
				ServicesTestEnvironment.TEST_WORKSPACE);

		geoServerServiceClient = new GeoServerServiceClient(
				ServicesTestEnvironment.GEOWAVE_BASE_URL,
				ServicesTestEnvironment.GEOSERVER_USER,
				ServicesTestEnvironment.GEOSERVER_PASS);
		configServiceClient = new ConfigServiceClient(
				ServicesTestEnvironment.GEOWAVE_BASE_URL,
				ServicesTestEnvironment.GEOSERVER_USER,
				ServicesTestEnvironment.GEOSERVER_PASS);

		boolean success = true;
		configServiceClient.configGeoServer("localhost:9011");
		// create the workspace
		Response addWs = geoServerServiceClient.addWorkspace(ServicesTestEnvironment.TEST_WORKSPACE);
		success &= (addWs.getStatus() == 200);
		// enable wfs & wms
		success &= enableWfs();
		success &= enableWms();
		// create the datastore
		configServiceClient.addStore(
				TestUtils.TEST_NAMESPACE,
				dataStoreOptions.getType(),
				TestUtils.TEST_NAMESPACE,
				dataStoreOptions.getOptionsAsMap());
		Response addDs = geoServerServiceClient.addDataStore(
				TestUtils.TEST_NAMESPACE,
				ServicesTestEnvironment.TEST_WORKSPACE,
				TestUtils.TEST_NAMESPACE);
		success &= (addDs.getStatus() == 200);
		// make sure the datastore exists
		Response getDs = geoServerServiceClient.getDataStore(
				TestUtils.TEST_NAMESPACE,
				ServicesTestEnvironment.TEST_WORKSPACE);
		success &= (getDs.getStatus() == 200);

		success &= createLayers();

		if (!success) {
			LOGGER.error("Geoserver WFS setup failed.");
		}
	}

	@After
	public void cleanupWorkspace() {
		TestUtils.assertStatusCode(
				"Workspace should be removed successfully",
				200,
				geoServerServiceClient.removeWorkspace(ServicesTestEnvironment.TEST_WORKSPACE));
		TestUtils.deleteAll(dataStoreOptions);
	}

	@Test
	public void test()
			throws Exception {
		assertTrue(createPoint());
		final String lockID = lockPoint();

		// setup the lock and update messages
		update = MessageFormat.format(
				IOUtils.toString(new FileInputStream(
						UPDATE_FILE)),
				ServicesTestEnvironment.TEST_WORKSPACE,
				lockID);

		assertNotNull(lockID);
		assertTrue(updatePoint(lockID));
		assertTrue(queryPoint());
		assertTrue(queryFindPointWithTime());
		assertTrue(queryFindPointBeyondTime());
	}

	public static boolean enableWfs()
			throws ClientProtocolException,
			IOException {
		final Pair<CloseableHttpClient, HttpClientContext> clientAndContext = createClientAndContext();
		final CloseableHttpClient httpclient = clientAndContext.getLeft();
		final HttpClientContext context = clientAndContext.getRight();
		try {
			final HttpPut command = new HttpPut(
					ServicesTestEnvironment.GEOSERVER_REST_PATH + "/services/wfs/workspaces/"
							+ ServicesTestEnvironment.TEST_WORKSPACE + "/settings");
			command.setHeader(
					"Content-type",
					"text/xml");
			command.setEntity(EntityBuilder.create().setFile(
					new File(
							"src/test/resources/wfs-requests/wfs.xml")).setContentType(
					ContentType.TEXT_XML).build());
			final HttpResponse r = httpclient.execute(
					command,
					context);
			return r.getStatusLine().getStatusCode() == Status.OK.getStatusCode();
		}
		finally {
			httpclient.close();
		}
	}

	public static boolean enableWms()
			throws ClientProtocolException,
			IOException {
		final Pair<CloseableHttpClient, HttpClientContext> clientAndContext = createClientAndContext();
		final CloseableHttpClient httpclient = clientAndContext.getLeft();
		final HttpClientContext context = clientAndContext.getRight();
		try {
			final HttpPut command = new HttpPut(
					ServicesTestEnvironment.GEOSERVER_REST_PATH + "/services/wms/workspaces/"
							+ ServicesTestEnvironment.TEST_WORKSPACE + "/settings");
			command.setHeader(
					"Content-type",
					"text/xml");
			command.setEntity(EntityBuilder.create().setFile(
					new File(
							"src/test/resources/wfs-requests/wms.xml")).setContentType(
					ContentType.TEXT_XML).build());
			final HttpResponse r = httpclient.execute(
					command,
					context);
			return r.getStatusLine().getStatusCode() == Status.OK.getStatusCode();
		}
		finally {
			httpclient.close();
		}
	}

	public boolean createLayers()
			throws ClientProtocolException,
			IOException {
		final Pair<CloseableHttpClient, HttpClientContext> clientAndContext = createClientAndContext();
		final CloseableHttpClient httpclient = clientAndContext.getLeft();
		final HttpClientContext context = clientAndContext.getRight();
		try {
			final HttpPost command = new HttpPost(
					ServicesTestEnvironment.GEOSERVER_REST_PATH + "/workspaces/"
							+ ServicesTestEnvironment.TEST_WORKSPACE + "/datastores/" + TestUtils.TEST_NAMESPACE
							+ "/featuretypes");
			command.setHeader(
					"Content-type",
					"text/xml");
			command.setEntity(EntityBuilder.create().setText(
					geostuff_layer).setContentType(
					ContentType.TEXT_XML).build());
			final HttpResponse r = httpclient.execute(
					command,
					context);
			return r.getStatusLine().getStatusCode() == Status.CREATED.getStatusCode();
		}
		finally {
			httpclient.close();
		}
	}

	static protected Pair<CloseableHttpClient, HttpClientContext> createClientAndContext() {
		final CredentialsProvider provider = new BasicCredentialsProvider();
		provider.setCredentials(
				new AuthScope(
						"localhost",
						ServicesTestEnvironment.JETTY_PORT),
				new UsernamePasswordCredentials(
						ServicesTestEnvironment.GEOSERVER_USER,
						ServicesTestEnvironment.GEOSERVER_PASS));
		final AuthCache authCache = new BasicAuthCache();
		final HttpHost targetHost = new HttpHost(
				"localhost",
				ServicesTestEnvironment.JETTY_PORT,
				"http");
		authCache.put(
				targetHost,
				new BasicScheme());

		// Add AuthCache to the execution context
		final HttpClientContext context = HttpClientContext.create();
		context.setCredentialsProvider(provider);
		context.setAuthCache(authCache);
		return ImmutablePair.of(
				HttpClientBuilder.create().setDefaultCredentialsProvider(
						provider).build(),
				context);
	}

	private HttpPost createWFSTransaction(
			final HttpClient httpclient,
			final String version,
			final BasicNameValuePair... paramTuples )
			throws Exception {
		final HttpPost command = new HttpPost(
				WFS_URL_PREFIX + "/Transaction");

		final ArrayList<BasicNameValuePair> postParameters = new ArrayList<BasicNameValuePair>();
		postParameters.add(new BasicNameValuePair(
				"version",
				version));
		postParameters.add(new BasicNameValuePair(
				"typename",
				ServicesTestEnvironment.TEST_WORKSPACE + ":geostuff"));
		Collections.addAll(
				postParameters,
				paramTuples);

		command.setEntity(new UrlEncodedFormEntity(
				postParameters));

		command.setHeader(
				"Content-type",
				"text/xml");
		command.setHeader(
				"Accept",
				"text/xml");

		return command;
	}

	private HttpGet createWFSGetFeature(
			final String version,
			final BasicNameValuePair... paramTuples ) {

		final StringBuilder buf = new StringBuilder();

		final List<BasicNameValuePair> localParams = new LinkedList<BasicNameValuePair>();
		localParams.add(new BasicNameValuePair(
				"version",
				version));
		localParams.add(new BasicNameValuePair(
				"request",
				"GetFeature"));
		localParams.add(new BasicNameValuePair(
				"typeNames",
				ServicesTestEnvironment.TEST_WORKSPACE + ":geostuff"));
		localParams.add(new BasicNameValuePair(
				"service",
				"WFS"));

		for (final BasicNameValuePair aParam : paramTuples) {
			if (buf.length() > 0) {
				buf.append('&');
			}
			buf.append(
					aParam.getName()).append(
					'=').append(
					aParam.getValue());
		}
		for (final BasicNameValuePair aParam : localParams) {
			if (buf.length() > 0) {
				buf.append('&');
			}
			buf.append(
					aParam.getName()).append(
					'=').append(
					aParam.getValue());
		}
		final HttpGet command = new HttpGet(
				WFS_URL_PREFIX + "?" + buf.toString());
		return command;

	}

	public boolean createPoint()
			throws Exception {
		final Pair<CloseableHttpClient, HttpClientContext> clientAndContext = createClientAndContext();
		final CloseableHttpClient httpclient = clientAndContext.getLeft();
		final HttpClientContext context = clientAndContext.getRight();
		try {
			final HttpPost command = createWFSTransaction(
					httpclient,
					"1.1.0");
			command.setEntity(EntityBuilder.create().setText(
					insert).setContentType(
					ContentType.TEXT_XML).build());
			final HttpResponse r = httpclient.execute(
					command,
					context);
			return r.getStatusLine().getStatusCode() == Status.OK.getStatusCode();
		}
		finally {
			httpclient.close();
		}
	}

	private String getContent(
			final HttpResponse r )
			throws IOException {
		final InputStream is = r.getEntity().getContent();
		final Header encoding = r.getEntity().getContentEncoding();
		final String encodingName = encoding == null ? "UTF-8" : encoding.getName();
		return IOUtils.toString(
				is,
				encodingName);
	}

	/*
	 * @return lockID
	 */

	public String lockPoint()
			throws Exception {
		final Pair<CloseableHttpClient, HttpClientContext> clientAndContext = createClientAndContext();
		final CloseableHttpClient httpclient = clientAndContext.getLeft();
		final HttpClientContext context = clientAndContext.getRight();
		try {
			final HttpPost command = createWFSTransaction(
					httpclient,
					"1.1.0");
			command.setEntity(EntityBuilder.create().setText(
					lock).setContentType(
					ContentType.TEXT_XML).build());
			final HttpResponse r = httpclient.execute(
					command,
					context);

			final boolean result = r.getStatusLine().getStatusCode() == Status.OK.getStatusCode();
			if (result) {
				final String content = getContent(r);
				final String pattern = "lockId=\"([^\"]+)\"";

				// Create a Pattern object
				final Pattern compiledPattern = Pattern.compile(pattern);
				final Matcher matcher = compiledPattern.matcher(content);
				if (matcher.find()) {
					return matcher.group(1);
				}
				return content;
			}
			return null;
		}
		finally {
			httpclient.close();
		}
	}

	/*
	 * @return queryPOINT
	 */

	public boolean queryPoint()
			throws Exception {
		final Pair<CloseableHttpClient, HttpClientContext> clientAndContext = createClientAndContext();
		final CloseableHttpClient httpclient = clientAndContext.getLeft();
		final HttpClientContext context = clientAndContext.getRight();
		try {
			final HttpPost command = createWFSTransaction(
					httpclient,
					"1.1.0");
			command.setEntity(EntityBuilder.create().setText(
					query).setContentType(
					ContentType.TEXT_XML).build());
			final HttpResponse r = httpclient.execute(
					command,
					context);

			final boolean result = r.getStatusLine().getStatusCode() == Status.OK.getStatusCode();
			if (result) {
				final String content = getContent(r);
				System.out.println(content);
				final String patternX = "34.6815818";
				final String patternY = "35.1828408";
				// name space check as well
				return content.contains(patternX) && content.contains(patternY)
						&& content.contains(ServicesTestEnvironment.TEST_WORKSPACE + ":geometry");
			}
			return false;
		}
		finally {
			httpclient.close();
		}
	}

	public boolean updatePoint(
			final String lockID )
			throws Exception {
		final Pair<CloseableHttpClient, HttpClientContext> clientAndContext = createClientAndContext();
		final CloseableHttpClient httpclient = clientAndContext.getLeft();
		final HttpClientContext context = clientAndContext.getRight();
		try {
			final HttpPost command = createWFSTransaction(
					httpclient,
					"1.1.0");
			command.setEntity(new StringEntity(
					update));
			final LinkedList<HttpResponse> capturedResponse = new LinkedList<HttpResponse>();
			run(
					new Runnable() {
						@Override
						public void run() {
							try {
								capturedResponse.add(httpclient.execute(
										command,
										context));
							}
							catch (final Exception e) {
								throw new RuntimeException(
										"update point client failed",
										e);
							}
						}
					},
					500000);

			final HttpResponse r = capturedResponse.getFirst();

			return r.getStatusLine().getStatusCode() == Status.OK.getStatusCode();
		}
		finally {
			httpclient.close();
		}
	}

	public boolean queryFindPointWithTime()
			throws ClientProtocolException,
			IOException {
		final Pair<CloseableHttpClient, HttpClientContext> clientAndContext = createClientAndContext();
		final CloseableHttpClient httpclient = clientAndContext.getLeft();
		final HttpClientContext context = clientAndContext.getRight();
		try {
			final HttpGet command = createWFSGetFeature(
					"1.1.0",
					new BasicNameValuePair(
							"cql_filter",
							URLEncoder
									.encode(
											"BBOX(geometry,34.68,35.18,34.7,35.19) and when during 2005-05-19T00:00:00Z/2005-05-19T21:32:56Z",
											"UTF8")),
					new BasicNameValuePair(
							"srsName",
							"EPSG:4326"));
			final HttpResponse r = httpclient.execute(
					command,
					context);

			final String content = getContent(r);
			System.out.println(content);
			return content.contains("numberOfFeatures=") && !content.contains("numberOfFeatures=\"0\"");
		}
		finally {
			httpclient.close();
		}
	}

	public boolean queryFindPointBeyondTime()
			throws ClientProtocolException,
			IOException {
		final Pair<CloseableHttpClient, HttpClientContext> clientAndContext = createClientAndContext();
		final CloseableHttpClient httpclient = clientAndContext.getLeft();
		final HttpClientContext context = clientAndContext.getRight();
		try {
			final HttpGet command = createWFSGetFeature(
					"1.1.0",
					new BasicNameValuePair(
							"cql_filter",
							URLEncoder
									.encode(
											"BBOX(geometry,34.68,35.18,34.7,35.19) and when during 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z",
											"UTF8")),
					new BasicNameValuePair(
							"srsName",
							"EPSG:4326"));
			final HttpResponse r = httpclient.execute(
					command,
					context);

			final String content = getContent(r);
			return content.contains("numberOfFeatures=\"0\"");
		}
		finally {
			httpclient.close();
		}
	}

	public static void run(
			final Runnable run,
			final long waitTime )
			throws InterruptedException {
		final Thread thread = new Thread(
				run);
		thread.start();
		thread.join(waitTime);
	}
}
