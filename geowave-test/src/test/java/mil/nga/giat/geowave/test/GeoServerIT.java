package mil.nga.giat.geowave.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.HttpParams;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.xml.XmlConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoServerIT extends
		GeoWaveTestEnvironment
{
	static final Logger log = LoggerFactory.getLogger(GeoServerIT.class);
	static final String resturlPrefix = JETTY_BASE_URL + "/geoserver/rest/workspaces/geowave/datastores";
	static final String wfsurlPrefix = JETTY_BASE_URL + "/geoserver/wfs";

	@BeforeClass
	public static void setUp()
			throws ClientProtocolException,
			IOException {
		checkStore();
		createLayers();
	}

	@Test
	public void test()
			throws ClientProtocolException,
			IOException,
			InterruptedException,
			AuthenticationException {
		assertTrue(createPoint());
		final String lockID = lockPoint();
		assertNotNull(lockID);
		assertTrue(updatePoint(lockID));
		assertTrue(queryPoint());
		assertTrue(queryFindPointWithTime());
		assertTrue(queryFindPointBeyondTime());
	}

	private static Credentials getCredentials() {
		return new UsernamePasswordCredentials(
				"admin",
				"geoserver");
	}

	static public boolean checkStore()
			throws ClientProtocolException,
			IOException {
		final DefaultHttpClient httpclient = createClient();
		final HttpGet target = new HttpGet(
				resturlPrefix);

		final HttpResponse r = httpclient.execute(target);
		return r.getStatusLine().getStatusCode() == 200;
	}

	static public boolean createLayers()
			throws ClientProtocolException,
			IOException {
		final DefaultHttpClient httpclient = createClient();

		final HttpPost command = new HttpPost(
				resturlPrefix + "/test/featuretypes");
		command.setHeader(
				"Content-type",
				"text/xml");
		command.setEntity(new FileEntity(
				new File(
						"src/test/resources/wfs-requests/geostuff_layer.xml"),
				"text/xml"));
		final HttpResponse r = httpclient.execute(command);
		return r.getStatusLine().getStatusCode() == 201;

	}

	static private DefaultHttpClient createClient() {
		final DefaultHttpClient httpclient = new DefaultHttpClient();
		httpclient.getParams();
		final CredentialsProvider provider = new BasicCredentialsProvider();
		final Credentials credentials = new UsernamePasswordCredentials(
				"admin",
				"geoserver");
		provider.setCredentials(
				AuthScope.ANY,
				credentials);
		httpclient.setCredentialsProvider(provider);

		return httpclient;
	}

	private HttpPost createWFSTransaction(
			final DefaultHttpClient httpclient,
			final String version,
			final Tuple... paramTuples )
			throws AuthenticationException {
		final HttpPost command = new HttpPost(
				wfsurlPrefix + "/Transaction");
		final HttpParams params = command.getParams();
		params.setParameter(
				"version",
				version);
		params.setParameter(
				"typename",
				"geowave:geostuff");
		for (final Tuple aParam : paramTuples) {
			params.setParameter(
					aParam.name,
					aParam.value);
		}
		command.setParams(params);
		command.setHeader(
				"Content-type",
				"text/xml");
		command.setHeader(
				"Accept",
				"text/xml");

		command.addHeader(new BasicScheme().authenticate(
				getCredentials(),
				command));
		return command;
	}

	private HttpGet createWFSGetFeature(
			final String version,
			final Tuple... paramTuples ) {

		final StringBuffer buf = new StringBuffer();

		final List<Tuple> localParams = new LinkedList<Tuple>();
		localParams.add(new Tuple(
				"version",
				version));
		localParams.add(new Tuple(
				"request",
				"GetFeature"));
		localParams.add(new Tuple(
				"typeNames",
				"geowave:geostuff"));
		localParams.add(new Tuple(
				"service",
				"WFS"));

		for (final Tuple aParam : paramTuples) {
			if (buf.length() > 0) {
				buf.append('&');
			}
			buf.append(
					aParam.name).append(
					'=').append(
					aParam.value);
		}
		for (final Tuple aParam : localParams) {
			if (buf.length() > 0) {
				buf.append('&');
			}
			buf.append(
					aParam.name).append(
					'=').append(
					aParam.value);
		}
		final HttpGet command = new HttpGet(
				wfsurlPrefix + "?" + buf.toString());
		return command;

	}

	private class Tuple
	{
		String name;
		String value;

		public Tuple(
				final String name,
				final String value ) {
			super();
			this.name = name;
			this.value = value;
		}

	}

	public boolean createPoint()
			throws ClientProtocolException,
			IOException,
			AuthenticationException {
		final DefaultHttpClient httpclient = createClient();

		final HttpPost command = createWFSTransaction(
				httpclient,
				"1.1.0");
		command.setEntity(new FileEntity(
				new File(
						"src/test/resources/wfs-requests/insert.xml"),
				"text/xml"));
		final HttpResponse r = httpclient.execute(command);
		return r.getStatusLine().getStatusCode() == 200;
	}

	public boolean createTimePoint()
			throws ClientProtocolException,
			IOException,
			AuthenticationException {
		final DefaultHttpClient httpclient = createClient();
		final HttpPost command = createWFSTransaction(
				httpclient,
				"1.1.0");
		command.setEntity(new FileEntity(
				new File(
						"src/test/resources/wfs-requests/insert_with_time.xml"),
				"text/xml"));
		final HttpResponse r = httpclient.execute(command);
		return r.getStatusLine().getStatusCode() == 200;
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
			throws ClientProtocolException,
			IOException,
			AuthenticationException {
		final DefaultHttpClient httpclient = createClient();
		final HttpPost command = createWFSTransaction(
				httpclient,
				"1.1.0");
		command.setEntity(new FileEntity(
				new File(
						"src/test/resources/wfs-requests/lock.xml"),
				"text/xml"));
		final HttpResponse r = httpclient.execute(command);
		final boolean result = r.getStatusLine().getStatusCode() == 200;
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
			throws ClientProtocolException,
			IOException,
			AuthenticationException {
		final DefaultHttpClient httpclient = createClient();
		final HttpPost command = createWFSTransaction(
				httpclient,
				"1.1.0");
		command.setEntity(new FileEntity(
				new File(
						"src/test/resources/wfs-requests/query.xml"),
				"text/xml"));
		final HttpResponse r = httpclient.execute(command);
		final boolean result = r.getStatusLine().getStatusCode() == 200;
		if (result) {
			final String content = getContent(r);
			final String pattern = "35.1828408241272 34.68158180311274";

			// name space check as well
			return content.contains(pattern) && content.contains("geowave:geometry");
		}
		return false;

	}

	private static final String updateFormat = "<?xml version=\"1.0\"?>\n" + "<wfs:Transaction xsi:schemaLocation=\"http://www.opengis.net/wfs http://schemas.opengis.net/wfs/1.1.0/WFS-transaction.xsd\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:ogc=\"http://www.opengis.net/ogc\" xmlns:gml=\"http://www.opengis.net/gml\" xmlns:wfs=\"http://www.opengis.net/wfs\" xmlns:geowave=\"http://localhost:9090/geowave\" version=\"1.1.0\" service=\"WFS\" releaseAction=\"ALL\" lockId=\"{0}\">\n" + "<wfs:LockId>{0}</wfs:LockId>" + "<wfs:Update typeName=\"geowave:geostuff\">\n" + "<wfs:Property><wfs:Name>geometry</wfs:Name><wfs:Value>\n" + "<gml:Point srsName=\"urn:x-ogc:def:crs:EPSG:4326\" srsDimension=\"2\">\n" + "<gml:coordinates ts=\" \" cs=\",\" decimal=\".\">34.681581803112744,35.1828408241272</gml:coordinates>\n" + "</gml:Point></wfs:Value></wfs:Property>\n"
			+ "<ogc:Filter><PropertyIsEqualTo><PropertyName>pid</PropertyName><Literal>24bda997-3182-76ae-9716-6cf662044094</Literal></PropertyIsEqualTo></ogc:Filter>\n" + "</wfs:Update>\n" + "</wfs:Transaction>";

	public boolean updatePoint(
			final String lockID )
			throws IOException,
			InterruptedException,
			AuthenticationException {
		final DefaultHttpClient httpclient = createClient();
		final HttpPost command = createWFSTransaction(
				httpclient,
				"1.1.0");
		final String updateMsgWithLockId = MessageFormat.format(
				updateFormat,
				lockID);
		command.setEntity(new StringEntity(
				updateMsgWithLockId));
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
		return r.getStatusLine().getStatusCode() == 200;
	}

	/*
	 * @return lockID
	 */

	public boolean queryFindPointWithTime()
			throws ClientProtocolException,
			IOException {
		final DefaultHttpClient httpclient = createClient();
		final HttpGet command = createWFSGetFeature(
				"1.1.0",
				new Tuple(
						"cql_filter",
						URLEncoder.encode("BBOX(geometry,34.68,35.18,34.7,35.19) and when during 2005-05-19T00:00:00Z/2005-05-19T21:32:56Z")),
				new Tuple(
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
		final DefaultHttpClient httpclient = createClient();
		final HttpGet command = createWFSGetFeature(
				"1.1.0",
				new Tuple(
						"cql_filter",
						URLEncoder.encode("BBOX(geometry,34.68,35.18,34.7,35.19) and when during 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z")),
				new Tuple(
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
