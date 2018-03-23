package mil.nga.giat.geowave.test.services;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.List;

import javax.imageio.ImageIO;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.examples.ingest.SimpleIngest;
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
public class GeoServerIngestIT
{

	private static final Logger LOGGER = LoggerFactory.getLogger(GeoServerIngestIT.class);
	private static GeoServerServiceClient geoServerServiceClient;
	private static ConfigServiceClient configServiceClient;
	private static final String WORKSPACE = "testomatic";
	private static final String WMS_VERSION = "1.3";
	private static final String WMS_URL_PREFIX = "/geoserver/wms";
	private static final String REFERENCE_26_WMS_IMAGE_PATH = "src/test/resources/wms/wms-grid-2.6.gif";
	private static final String REFERENCE_25_WMS_IMAGE_PATH = "src/test/resources/wms/wms-grid-2.5.gif";

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStorePluginOptions;

	private static long startMillis;
	private final static String testName = "GeoServerServicesIT";

	@BeforeClass
	public static void setup() {
		geoServerServiceClient = new GeoServerServiceClient(
				ServicesTestEnvironment.GEOWAVE_BASE_URL);

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
	public void testExamplesIngest()
			throws IOException,
			URISyntaxException {
		final DataStore ds = dataStorePluginOptions.createDataStore();
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
		TestUtils.assertStatusCode(
				"Unable to create 'testomatic' workspace",
				200,
				geoServerServiceClient.addWorkspace("testomatic"));
		configServiceClient.addStore(
				TestUtils.TEST_NAMESPACE,
				dataStorePluginOptions.getType(),
				TestUtils.TEST_NAMESPACE,
				dataStorePluginOptions.getOptionsAsMap());
		TestUtils.assertStatusCode(
				"Unable to add " + TestUtils.TEST_NAMESPACE + " datastore",
				200,
				geoServerServiceClient.addDataStore(
						TestUtils.TEST_NAMESPACE,
						"testomatic",
						TestUtils.TEST_NAMESPACE));

		TestUtils.assertStatusCode(
				"Unable to publish '" + ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE + "' style",
				200,
				geoServerServiceClient.addStyle(
						ServicesTestEnvironment.TEST_SLD_NO_DIFFERENCE_FILE,
						ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE));
		TestUtils.assertStatusCode(
				"Unable to publish '" + ServicesTestEnvironment.TEST_STYLE_NAME_MINOR_SUBSAMPLE + "' style",
				200,
				geoServerServiceClient.addStyle(
						ServicesTestEnvironment.TEST_SLD_MINOR_SUBSAMPLE_FILE,
						ServicesTestEnvironment.TEST_STYLE_NAME_MINOR_SUBSAMPLE));
		TestUtils.assertStatusCode(
				"Unable to publish '" + ServicesTestEnvironment.TEST_STYLE_NAME_MAJOR_SUBSAMPLE + "' style",
				200,
				geoServerServiceClient.addStyle(
						ServicesTestEnvironment.TEST_SLD_MAJOR_SUBSAMPLE_FILE,
						ServicesTestEnvironment.TEST_STYLE_NAME_MAJOR_SUBSAMPLE));
		TestUtils.assertStatusCode(
				"Unable to publish '" + ServicesTestEnvironment.TEST_STYLE_NAME_DISTRIBUTED_RENDER + "' style",
				200,
				geoServerServiceClient.addStyle(
						ServicesTestEnvironment.TEST_SLD_DISTRIBUTED_RENDER_FILE,
						ServicesTestEnvironment.TEST_STYLE_NAME_DISTRIBUTED_RENDER));
		Response r = geoServerServiceClient.addLayer(
				TestUtils.TEST_NAMESPACE,
				WORKSPACE,
				null,
				null,
				"point");
		TestUtils.assertStatusCode(
				"Unable to publish '" + SimpleIngest.FEATURE_NAME + "' layer",
				200,
				r);

		final BufferedImage biDirectRender = getWMSSingleTile(
				-180,
				180,
				-90,
				90,
				SimpleIngest.FEATURE_NAME,
				"point",
				920,
				360,
				null);

		BufferedImage ref = null;

		final String geoserverVersion = (System.getProperty("geoserver.version") != null) ? System
				.getProperty("geoserver.version") : "";

		Assert.assertNotNull(geoserverVersion);

		if (geoserverVersion.startsWith("2.5") || geoserverVersion.equals("2.6.0") || geoserverVersion.equals("2.6.1")) {
			ref = ImageIO.read(new File(
					REFERENCE_25_WMS_IMAGE_PATH));
		}
		else {
			ref = ImageIO.read(new File(
					REFERENCE_26_WMS_IMAGE_PATH));
		}
		// being a little lenient because of differences in O/S rendering
		TestUtils.testTileAgainstReference(
				biDirectRender,
				ref,
				0,
				0.07);

		final BufferedImage biSubsamplingWithoutError = getWMSSingleTile(
				-180,
				180,
				-90,
				90,
				SimpleIngest.FEATURE_NAME,
				ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE,
				920,
				360,
				null);
		Assert.assertNotNull(ref);

		// being a little lenient because of differences in O/S rendering
		TestUtils.testTileAgainstReference(
				biSubsamplingWithoutError,
				ref,
				0,
				0.07);

		final BufferedImage biSubsamplingWithExpectedError = getWMSSingleTile(
				-180,
				180,
				-90,
				90,
				SimpleIngest.FEATURE_NAME,
				ServicesTestEnvironment.TEST_STYLE_NAME_MINOR_SUBSAMPLE,
				920,
				360,
				null);
		TestUtils.testTileAgainstReference(
				biSubsamplingWithExpectedError,
				ref,
				0.05,
				0.15);

		final BufferedImage biSubsamplingWithLotsOfError = getWMSSingleTile(
				-180,
				180,
				-90,
				90,
				SimpleIngest.FEATURE_NAME,
				ServicesTestEnvironment.TEST_STYLE_NAME_MAJOR_SUBSAMPLE,
				920,
				360,
				null);
		TestUtils.testTileAgainstReference(
				biSubsamplingWithLotsOfError,
				ref,
				0.3,
				0.35);
		final BufferedImage biDistributedRendering = getWMSSingleTile(
				-180,
				180,
				-90,
				90,
				SimpleIngest.FEATURE_NAME,
				ServicesTestEnvironment.TEST_STYLE_NAME_DISTRIBUTED_RENDER,
				920,
				360,
				null);
		TestUtils.testTileAgainstReference(
				biDistributedRendering,
				ref,
				0,
				0.07);
	}

	private static BufferedImage getWMSSingleTile(
			final double minlon,
			final double maxlon,
			final double minlat,
			final double maxlat,
			final String layer,
			final String style,
			final int width,
			final int height,
			final String outputFormat )
			throws IOException,
			URISyntaxException {
		final URIBuilder builder = new URIBuilder();
		builder.setScheme(
				"http").setHost(
				"localhost").setPort(
				ServicesTestEnvironment.JETTY_PORT).setPath(
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

		final Pair<CloseableHttpClient, HttpClientContext> clientAndContext = GeoServerIT.createClientAndContext();
		final CloseableHttpClient httpClient = clientAndContext.getLeft();
		final HttpClientContext context = clientAndContext.getRight();
		try {
			final HttpResponse resp = httpClient.execute(
					command,
					context);
			try (InputStream is = resp.getEntity().getContent()) {

				final BufferedImage image = ImageIO.read(is);

				Assert.assertNotNull(image);
				Assert.assertTrue(image.getWidth() == width);
				Assert.assertTrue(image.getHeight() == height);
				return image;
			}
		}
		finally {
			httpClient.close();
		}
	}

	@Before
	public void setUp() {
		configServiceClient.configGeoServer("localhost:9011");
	}

	@After
	public void cleanup() {

		final Response layer = geoServerServiceClient.removeFeatureLayer(SimpleIngest.FEATURE_NAME);
		final Response datastore = geoServerServiceClient.removeDataStore(
				TestUtils.TEST_NAMESPACE,
				WORKSPACE);
		final Response styleNoDifference = geoServerServiceClient
				.removeStyle(ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE);
		final Response styleMinor = geoServerServiceClient
				.removeStyle(ServicesTestEnvironment.TEST_STYLE_NAME_MINOR_SUBSAMPLE);
		final Response styleMajor = geoServerServiceClient
				.removeStyle(ServicesTestEnvironment.TEST_STYLE_NAME_MAJOR_SUBSAMPLE);
		final Response workspace = geoServerServiceClient.removeWorkspace(WORKSPACE);

		TestUtils.assertStatusCode(
				"Unable to delete layer '" + SimpleIngest.FEATURE_NAME + "'",
				200,
				layer);
		TestUtils.assertStatusCode(
				"Unable to delete datastore '" + TestUtils.TEST_NAMESPACE + "'",
				200,
				datastore);
		TestUtils.assertStatusCode(
				"Unable to delete style '" + ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE + "'",
				200,
				styleNoDifference);

		TestUtils.assertStatusCode(
				"Unable to delete style '" + ServicesTestEnvironment.TEST_STYLE_NAME_MINOR_SUBSAMPLE + "'",
				200,
				styleMinor);
		TestUtils.assertStatusCode(
				"Unable to delete style '" + ServicesTestEnvironment.TEST_STYLE_NAME_MAJOR_SUBSAMPLE + "'",
				200,
				styleMajor);
		TestUtils.assertStatusCode(
				"Unable to delete workspace '" + WORKSPACE + "'",
				200,
				workspace);

		TestUtils.assertStatusCode(
				"Unable to delete style '" + ServicesTestEnvironment.TEST_STYLE_NAME_DISTRIBUTED_RENDER + "'",
				200,
				geoServerServiceClient.removeStyle(ServicesTestEnvironment.TEST_STYLE_NAME_DISTRIBUTED_RENDER));
	}

}
