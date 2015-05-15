package mil.nga.giat.geowave.test.service;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.examples.ingest.SimpleIngest;
import mil.nga.giat.geowave.service.client.GeoserverServiceClient;
import mil.nga.giat.geowave.service.client.InfoServiceClient;
import mil.nga.giat.geowave.service.client.IngestServiceClient;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.resources.image.ImageUtilities;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import javax.imageio.ImageIO;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class GeoWaveIngestGeoserverIT extends
		ServicesTestEnvironment
{

	private static final Logger LOGGER = Logger.getLogger(GeoWaveIngestGeoserverIT.class);

	private static final String WORKSPACE = "testomatic";
	private static final String NAMESPACE = "exampleIngest";
	private static final String WMS_VERSION = "1.3";

	private static InfoServiceClient infoServiceClient = null;
	private static GeoserverServiceClient geoserverServiceClient = null;
	private static IngestServiceClient ingestServiceClient = null;

	private static final String WMS_URL_PREFIX = "/geoserver/wms";
	private static final String REFERENCE_26_WMS_IMAGE_PATH = "src/test/resources/wms/wms-grid-2.6.gif";
	private static final String REFERENCE_25_WMS_IMAGE_PATH = "src/test/resources/wms/wms-grid-2.5.gif";

	@BeforeClass
	public static void setupIngestTest()
			throws URISyntaxException {

		ServicesTestEnvironment.startServices();
		geoserverServiceClient = new GeoserverServiceClient(
				GEOWAVE_BASE_URL);
		infoServiceClient = new InfoServiceClient(
				GEOWAVE_BASE_URL);
		ingestServiceClient = new IngestServiceClient(
				GEOWAVE_BASE_URL);

	}

	@Test
	public void testExamplesIngest()
			throws IOException,
			SchemaException,
			URISyntaxException {
		BasicAccumuloOperations bao = null;
		try {
			bao = new BasicAccumuloOperations(
					zookeeper,
					accumuloInstance,
					accumuloUser,
					accumuloPassword,
					NAMESPACE);
		}
		catch (AccumuloException e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
		catch (AccumuloSecurityException e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
		AccumuloDataStore ds = new AccumuloDataStore(
				bao);
		SimpleFeatureType sft = SimpleIngest.createPointFeatureType();
		Index idx = SimpleIngest.createSpatialIndex();
		FeatureDataAdapter fda = SimpleIngest.createDataAdapter(sft);
		List<SimpleFeature> features = SimpleIngest.getGriddedFeatures(
				new SimpleFeatureBuilder(
						sft),
				8675309);
		LOGGER.info(String.format(
				"Beginning to ingest a uniform grid of %d features",
				features.size()));
		int ingestedFeatures = 0;
		int featuresPer5Percent = features.size() / 20;
		for (SimpleFeature feat : features) {
			ds.ingest(
					fda,
					idx,
					feat);
			ingestedFeatures++;
			if (ingestedFeatures % featuresPer5Percent == 0) {
				LOGGER.info(String.format(
						"Ingested %d percent of features",
						(ingestedFeatures / featuresPer5Percent) * 5));
			}
		}

		Assert.assertTrue(geoserverServiceClient.createWorkspace(WORKSPACE));
		Assert.assertTrue(geoserverServiceClient.publishDatastore(
				zookeeper,
				accumuloUser,
				accumuloPassword,
				accumuloInstance,
				NAMESPACE,
				null,
				null,
				null,
				WORKSPACE));
		Assert.assertTrue(geoserverServiceClient.publishLayer(
				NAMESPACE,
				"default_point",
				SimpleIngest.FEATURE_NAME,
				WORKSPACE));

		BufferedImage bi = getWMSSingleTile(
				-180,
				180,
				-90,
				90,
				"GridPoint",
				"",
				920,
				360,
				null);

		BufferedImage ref = null;

		String geoserverVersion = System.getProperty("geoserver.version");

		Assert.assertNotNull(geoserverVersion);

		if (geoserverVersion.startsWith("2.5") || geoserverVersion.equals("2.6.0") || geoserverVersion.equals("2.6.1")) {
			ref = ImageIO.read(new File(
					REFERENCE_25_WMS_IMAGE_PATH));
		}
		else {
			ref = ImageIO.read(new File(
					REFERENCE_26_WMS_IMAGE_PATH));
		}

		Assert.assertNotNull(ref);
		Assert.assertEquals(
				ref.getWidth(),
				bi.getWidth());
		Assert.assertEquals(
				ref.getHeight(),
				bi.getHeight());

		for (int x = 0; x < ref.getWidth(); x++) {
			for (int y = 0; y < ref.getHeight(); y++) {
				if (!(bi.getRGB(
						x,
						y) == ref.getRGB(
						x,
						y))) {
					Assert.fail(String.format(
							"[%d,%d] failed to match ref=%d gen=%d",
							x,
							y,
							ref.getRGB(
									x,
									y),
							bi.getRGB(
									x,
									y)));
				}
			}
		}

	}

	private static BufferedImage getWMSSingleTile(
			double minlon,
			double maxlon,
			double minlat,
			double maxlat,
			String layer,
			String style,
			int width,
			int height,
			String outputFormat )
			throws IOException,
			URISyntaxException {
		final List<BasicNameValuePair> params = new ArrayList<>(
				4);

		URIBuilder builder = new URIBuilder();
		builder.setScheme(
				"http").setHost(
				"localhost").setPort(
				JETTY_PORT).setPath(
				WMS_URL_PREFIX).setParameter(
				"service",
				"WMS").setParameter(
				"version",
				WMS_VERSION).setParameter(
				"request",
				"GetMap").setParameter(
				"layers",
				layer).setParameter(
				"styles",
				style == null ? "" : style).setParameter(
				"crs",
				"EPSG:4326").setParameter(
				"bbox",
				String.format(
						"%.2f, %.2f, %.2f, %.2f",
						minlon,
						minlat,
						maxlon,
						maxlat)).setParameter(
				"format",
				outputFormat == null ? "image/gif" : outputFormat).setParameter(
				"width",
				String.valueOf(width)).setParameter(
				"height",
				String.valueOf(height));

		final HttpGet command = new HttpGet(
				builder.build());

		final HttpClient httpClient = createClient();
		final HttpResponse resp = httpClient.execute(command);
		try (InputStream is = resp.getEntity().getContent()) {

			BufferedImage image = ImageIO.read(is);

			Assert.assertNotNull(image);
			Assert.assertTrue(image.getWidth() == width);
			Assert.assertTrue(image.getHeight() == height);
			return image;
		}
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

}
