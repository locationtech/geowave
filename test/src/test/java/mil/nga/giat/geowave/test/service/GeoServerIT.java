package mil.nga.giat.geowave.test.service;

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

import javax.ws.rs.core.Response.Status;

import mil.nga.giat.geowave.service.client.GeoserverServiceClient;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoServerIT extends
		ServicesTestEnvironment
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoServerIT.class);
	private static final String WFS_URL_PREFIX = JETTY_BASE_URL + "/geoserver/wfs";

	private static final String GEOSTUFF_LAYER_FILE = "src/test/resources/wfs-requests/geostuff_layer.xml";
	private static final String INSERT_FILE = "src/test/resources/wfs-requests/insert.xml";
	private static final String LOCK_FILE = "src/test/resources/wfs-requests/lock.xml";
	private static final String QUERY_FILE = "src/test/resources/wfs-requests/query.xml";
	private static final String UPDATE_FILE = "src/test/resources/wfs-requests/update.xml";

	private static GeoserverServiceClient geoserverServiceClient;

	private static String geostuff_layer;
	private static String insert;
	private static String lock;
	private static String query;
	private static String update;

	@BeforeClass
	public static void setUp()
			throws ClientProtocolException,
			IOException {
		ServicesTestEnvironment.startServices();
		try {
			accumuloOperations.deleteAll();
		}
		catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
			LOGGER.error(
					"Unable to clear accumulo namespace",
					ex);
			Assert.fail("Index not deleted successfully");
		}
		// setup the wfs-requests
		geostuff_layer = MessageFormat.format(
				IOUtils.toString(new FileInputStream(
						GEOSTUFF_LAYER_FILE)),
				TEST_WORKSPACE);

		insert = MessageFormat.format(
				IOUtils.toString(new FileInputStream(
						INSERT_FILE)),
				TEST_WORKSPACE);

		lock = MessageFormat.format(
				IOUtils.toString(new FileInputStream(
						LOCK_FILE)),
				TEST_WORKSPACE);

		query = MessageFormat.format(
				IOUtils.toString(new FileInputStream(
						QUERY_FILE)),
				TEST_WORKSPACE);

		geoserverServiceClient = new GeoserverServiceClient(
				GEOWAVE_BASE_URL);

		// create the workspace
		boolean success = geoserverServiceClient.createWorkspace(TEST_WORKSPACE);

		// enable wfs & wms
		success &= enableWfs();
		success &= enableWms();

		// create the datastore
		success &= geoserverServiceClient.publishDatastore(
				zookeeper,
				accumuloUser,
				accumuloPassword,
				accumuloInstance,
				TEST_NAMESPACE,
				null,
				null,
				null,
				TEST_WORKSPACE);

		// make sure the datastore exists
		success &= (null != geoserverServiceClient.getDatastore(
				TEST_NAMESPACE,
				TEST_WORKSPACE));

		success &= createLayers();

		if (!success) {
			LOGGER.error("Geoserver WFS setup failed.");
		}
	}

	@AfterClass
	public static void cleanup() {
		assertTrue(geoserverServiceClient.deleteWorkspace(TEST_WORKSPACE));
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
				TEST_WORKSPACE,
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
		final HttpClient httpclient = createClient();
		final HttpPut command = new HttpPut(
				GEOSERVER_REST_PATH + "/services/wfs/workspaces/" + TEST_WORKSPACE + "/settings");
		command.setHeader(
				"Content-type",
				"text/xml");
		command.setEntity(EntityBuilder.create().setFile(
				new File(
						"src/test/resources/wfs-requests/wfs.xml")).setContentType(
				ContentType.TEXT_XML).build());
		final HttpResponse r = httpclient.execute(command);
		return r.getStatusLine().getStatusCode() == Status.OK.getStatusCode();
	}

	public static boolean enableWms()
			throws ClientProtocolException,
			IOException {
		final HttpClient httpclient = createClient();
		final HttpPut command = new HttpPut(
				GEOSERVER_REST_PATH + "/services/wms/workspaces/" + TEST_WORKSPACE + "/settings");
		command.setHeader(
				"Content-type",
				"text/xml");
		command.setEntity(EntityBuilder.create().setFile(
				new File(
						"src/test/resources/wfs-requests/wms.xml")).setContentType(
				ContentType.TEXT_XML).build());
		final HttpResponse r = httpclient.execute(command);
		return r.getStatusLine().getStatusCode() == Status.OK.getStatusCode();
	}

	static public boolean createLayers()
			throws ClientProtocolException,
			IOException {
		final HttpClient httpclient = createClient();
		final HttpPost command = new HttpPost(
				GEOSERVER_REST_PATH + "/workspaces/" + TEST_WORKSPACE + "/datastores/" + TEST_NAMESPACE + "/featuretypes");
		command.setHeader(
				"Content-type",
				"text/xml");
		command.setEntity(EntityBuilder.create().setText(
				geostuff_layer).setContentType(
				ContentType.TEXT_XML).build());
		final HttpResponse r = httpclient.execute(command);
		return r.getStatusLine().getStatusCode() == Status.CREATED.getStatusCode();
	}

	static private HttpClient createClient() {
		final CredentialsProvider provider = new BasicCredentialsProvider();
		provider.setCredentials(
				AuthScope.ANY,
				new UsernamePasswordCredentials(
						GEOSERVER_USER,
						GEOSERVER_PASS));

		return HttpClientBuilder.create().setDefaultCredentialsProvider(
				provider).build();
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
				TEST_WORKSPACE + ":geostuff"));
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
				TEST_WORKSPACE + ":geostuff"));
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
		final HttpClient httpclient = createClient();

		final HttpPost command = createWFSTransaction(
				httpclient,
				"1.1.0");
		command.setEntity(EntityBuilder.create().setText(
				insert).setContentType(
				ContentType.TEXT_XML).build());
		final HttpResponse r = httpclient.execute(command);
		return r.getStatusLine().getStatusCode() == Status.OK.getStatusCode();
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
		final HttpClient httpclient = createClient();
		final HttpPost command = createWFSTransaction(
				httpclient,
				"1.1.0");
		command.setEntity(EntityBuilder.create().setText(
				lock).setContentType(
				ContentType.TEXT_XML).build());
		final HttpResponse r = httpclient.execute(command);
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

	/*
	 * @return queryPOINT
	 */

	public boolean queryPoint()
			throws Exception {
		final HttpClient httpclient = createClient();
		final HttpPost command = createWFSTransaction(
				httpclient,
				"1.1.0");
		command.setEntity(EntityBuilder.create().setText(
				query).setContentType(
				ContentType.TEXT_XML).build());
		final HttpResponse r = httpclient.execute(command);
		final boolean result = r.getStatusLine().getStatusCode() == Status.OK.getStatusCode();
		if (result) {
			final String content = getContent(r);
			final String pattern = "34.68158180311274 35.1828408241272";

			// name space check as well
			return content.contains(pattern) && content.contains(TEST_WORKSPACE + ":geometry");
		}
		return false;
	}

	public boolean updatePoint(
			final String lockID )
			throws Exception {
		final HttpClient httpclient = createClient();
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
							capturedResponse.add(httpclient.execute(command));
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

	/*
	 * @return lockID
	 */

	public boolean queryFindPointWithTime()
			throws ClientProtocolException,
			IOException {
		final HttpClient httpclient = createClient();
		final HttpGet command = createWFSGetFeature(
				"1.1.0",
				new BasicNameValuePair(
						"cql_filter",
						URLEncoder.encode(
								"BBOX(geometry,34.68,35.18,34.7,35.19) and when during 2005-05-19T00:00:00Z/2005-05-19T21:32:56Z",
								"UTF8")),
				new BasicNameValuePair(
						"srsName",
						"EPSG:4326"));
		final HttpResponse r = httpclient.execute(command);
		final String content = getContent(r);
		System.out.println(content);
		return content.contains("numberOfFeatures=") && !content.contains("numberOfFeatures=\"0\"");
	}

	public boolean queryFindPointBeyondTime()
			throws ClientProtocolException,
			IOException {
		final HttpClient httpclient = createClient();
		final HttpGet command = createWFSGetFeature(
				"1.1.0",
				new BasicNameValuePair(
						"cql_filter",
						URLEncoder.encode(
								"BBOX(geometry,34.68,35.18,34.7,35.19) and when during 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z",
								"UTF8")),
				new BasicNameValuePair(
						"srsName",
						"EPSG:4326"));
		final HttpResponse r = httpclient.execute(command);
		final String content = getContent(r);
		return content.contains("numberOfFeatures=\"0\"");
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
