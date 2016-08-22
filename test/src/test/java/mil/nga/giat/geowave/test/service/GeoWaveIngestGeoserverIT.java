package mil.nga.giat.geowave.test.service;

import static org.junit.Assert.assertTrue;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.writer.IndexWriter;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.examples.ingest.SimpleIngest;
import mil.nga.giat.geowave.service.client.GeoserverServiceClient;
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
public class GeoWaveIngestGeoserverIT
{

	private static final Logger LOGGER = Logger.getLogger(GeoWaveIngestGeoserverIT.class);

	private static final String WORKSPACE = "testomatic";
	private static final String WMS_VERSION = "1.3";
	private static final String WMS_URL_PREFIX = "/geoserver/wms";
	private static final String REFERENCE_26_WMS_IMAGE_PATH = "src/test/resources/wms/wms-grid-2.6.gif";
	private static final String REFERENCE_25_WMS_IMAGE_PATH = "src/test/resources/wms/wms-grid-2.5.gif";

	private static GeoserverServiceClient geoserverServiceClient = null;
	@GeoWaveTestStore({
		GeoWaveStoreType.ACCUMULO
	})
	protected DataStorePluginOptions dataStoreOptions;

	@BeforeClass
	public static void setupIngestTest()
			throws URISyntaxException {
		geoserverServiceClient = new GeoserverServiceClient(
				ServicesTestEnvironment.GEOWAVE_BASE_URL);

	}

	@Test
	public void testExamplesIngest()
			throws IOException,
			SchemaException,
			URISyntaxException {
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

		Assert.assertTrue(
				"Unable to create 'testomatic' workspace",
				geoserverServiceClient.createWorkspace(WORKSPACE));
		Assert.assertTrue(
				"Unable to publish '" + dataStoreOptions.getType() + "' data store",
				geoserverServiceClient.publishDatastore(
						dataStoreOptions.getType(),
						dataStoreOptions.getFactoryOptionsAsMap(),
						TestUtils.TEST_NAMESPACE,
						null,
						null,
						null,
						null,
						WORKSPACE));
		assertTrue(
				"Unable to publish '" + ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE + "' style",
				geoserverServiceClient.publishStyle(new File[] {
					new File(
							ServicesTestEnvironment.TEST_SLD_NO_DIFFERENCE_FILE)
				}));
		assertTrue(
				"Unable to publish '" + ServicesTestEnvironment.TEST_SLD_MINOR_SUBSAMPLE_FILE + "' style",
				geoserverServiceClient.publishStyle(new File[] {
					new File(
							ServicesTestEnvironment.TEST_SLD_MINOR_SUBSAMPLE_FILE)
				}));
		assertTrue(
				"Unable to publish '" + ServicesTestEnvironment.TEST_SLD_MAJOR_SUBSAMPLE_FILE + "' style",
				geoserverServiceClient.publishStyle(new File[] {
					new File(
							ServicesTestEnvironment.TEST_SLD_MAJOR_SUBSAMPLE_FILE)
				}));
		Assert.assertTrue(
				"Unable to publish '" + SimpleIngest.FEATURE_NAME + "' layer",
				geoserverServiceClient.publishLayer(
						TestUtils.TEST_NAMESPACE,
						"point",
						SimpleIngest.FEATURE_NAME,
						WORKSPACE));

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

		TestUtils.testTileAgainstReference(
				biDirectRender,
				ref,
				0,
				0);

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
		TestUtils.testTileAgainstReference(
				biSubsamplingWithoutError,
				ref,
				0,
				0);

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
				0.1);

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

		assertTrue(
				"Unable to delete layer '" + SimpleIngest.FEATURE_NAME + "'",
				geoserverServiceClient.deleteLayer(SimpleIngest.FEATURE_NAME));
		assertTrue(
				"Unable to delete datastore '" + TestUtils.TEST_NAMESPACE + "'",
				geoserverServiceClient.deleteDatastore(
						TestUtils.TEST_NAMESPACE,
						WORKSPACE));
		assertTrue(
				"Unable to delete style '" + ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE + "'",
				geoserverServiceClient.deleteStyle(ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE));

		assertTrue(
				"Unable to delete style '" + ServicesTestEnvironment.TEST_STYLE_NAME_MINOR_SUBSAMPLE + "'",
				geoserverServiceClient.deleteStyle(ServicesTestEnvironment.TEST_STYLE_NAME_MINOR_SUBSAMPLE));
		assertTrue(
				"Unable to delete style '" + ServicesTestEnvironment.TEST_STYLE_NAME_MAJOR_SUBSAMPLE + "'",
				geoserverServiceClient.deleteStyle(ServicesTestEnvironment.TEST_STYLE_NAME_MAJOR_SUBSAMPLE));
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

		final HttpClient httpClient = createClient();
		final HttpResponse resp = httpClient.execute(command);
		try (InputStream is = resp.getEntity().getContent()) {

			final BufferedImage image = ImageIO.read(is);

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
						ServicesTestEnvironment.GEOSERVER_USER,
						ServicesTestEnvironment.GEOSERVER_PASS));

		return HttpClientBuilder.create().setDefaultCredentialsProvider(
				provider).build();
	}

}
