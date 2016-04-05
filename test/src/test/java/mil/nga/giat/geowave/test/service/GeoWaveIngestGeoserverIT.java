package mil.nga.giat.geowave.test.service;

import static org.junit.Assert.assertTrue;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.List;

import javax.imageio.ImageIO;

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
import org.apache.log4j.Logger;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloStoreFactoryFamily;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.examples.ingest.SimpleIngest;
import mil.nga.giat.geowave.service.client.GeoserverServiceClient;

public class GeoWaveIngestGeoserverIT extends
		ServicesTestEnvironment
{

	private static final Logger LOGGER = Logger.getLogger(GeoWaveIngestGeoserverIT.class);

	private static final String WORKSPACE = "testomatic";
	private static final String WMS_VERSION = "1.3";

	private static GeoserverServiceClient geoserverServiceClient = null;
	private static final String WMS_URL_PREFIX = "/geoserver/wms";
	private static final String REFERENCE_26_WMS_IMAGE_PATH = "src/test/resources/wms/wms-grid-2.6.gif";
	private static final String REFERENCE_25_WMS_IMAGE_PATH = "src/test/resources/wms/wms-grid-2.5.gif";

	@BeforeClass
	public static void setupIngestTest()
			throws URISyntaxException {
		geoserverServiceClient = new GeoserverServiceClient(
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
					TEST_NAMESPACE);
		}
		catch (final AccumuloException e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
		catch (final AccumuloSecurityException e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
		final AccumuloDataStore ds = new AccumuloDataStore(
				bao);
		final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();
		final PrimaryIndex idx = SimpleIngest.createSpatialIndex();
		final FeatureDataAdapter fda = SimpleIngest.createDataAdapter(sft);
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
				"Unable to publish Accumulo data store",
				geoserverServiceClient.publishDatastore(
						new AccumuloStoreFactoryFamily().getName(),
						getAccumuloConfig(),
						TEST_NAMESPACE,
						null,
						null,
						null,
						null,
						WORKSPACE));
		assertTrue(
				"Unable to publish '" + TEST_STYLE_NAME_NO_DIFFERENCE + "' style",
				geoserverServiceClient.publishStyle(new File[] {
					new File(
							TEST_SLD_NO_DIFFERENCE_FILE)
				}));
		assertTrue(
				"Unable to publish '" + TEST_SLD_MINOR_SUBSAMPLE_FILE + "' style",
				geoserverServiceClient.publishStyle(new File[] {
					new File(
							TEST_SLD_MINOR_SUBSAMPLE_FILE)
				}));
		assertTrue(
				"Unable to publish '" + TEST_SLD_MAJOR_SUBSAMPLE_FILE + "' style",
				geoserverServiceClient.publishStyle(new File[] {
					new File(
							TEST_SLD_MAJOR_SUBSAMPLE_FILE)
				}));
		Assert.assertTrue(
				"Unable to publish '" + SimpleIngest.FEATURE_NAME + "' layer",
				geoserverServiceClient.publishLayer(
						TEST_NAMESPACE,
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

		final String geoserverVersion = (System.getProperty("geoserver.version") != null) ? System.getProperty("geoserver.version") : "";

		Assert.assertNotNull(geoserverVersion);

		if (geoserverVersion.startsWith("2.5") || geoserverVersion.equals("2.6.0") || geoserverVersion.equals("2.6.1")) {
			ref = ImageIO.read(new File(
					REFERENCE_25_WMS_IMAGE_PATH));
		}
		else {
			ref = ImageIO.read(new File(
					REFERENCE_26_WMS_IMAGE_PATH));
		}

		testTileAgainstReference(
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
				TEST_STYLE_NAME_NO_DIFFERENCE,
				920,
				360,
				null);
		Assert.assertNotNull(ref);
		testTileAgainstReference(
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
				TEST_STYLE_NAME_MINOR_SUBSAMPLE,
				920,
				360,
				null);
		testTileAgainstReference(
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
				TEST_STYLE_NAME_MAJOR_SUBSAMPLE,
				920,
				360,
				null);
		testTileAgainstReference(
				biSubsamplingWithLotsOfError,
				ref,
				0.3,
				0.35);

		assertTrue(
				"Unable to delete layer '" + SimpleIngest.FEATURE_NAME + "'",
				geoserverServiceClient.deleteLayer(SimpleIngest.FEATURE_NAME));
		assertTrue(
				"Unable to delete datastore '" + TEST_NAMESPACE + "'",
				geoserverServiceClient.deleteDatastore(
						TEST_NAMESPACE,
						WORKSPACE));
		assertTrue(
				"Unable to delete style '" + TEST_STYLE_NAME_NO_DIFFERENCE + "'",
				geoserverServiceClient.deleteStyle(TEST_STYLE_NAME_NO_DIFFERENCE));

		assertTrue(
				"Unable to delete style '" + TEST_STYLE_NAME_MINOR_SUBSAMPLE + "'",
				geoserverServiceClient.deleteStyle(TEST_STYLE_NAME_MINOR_SUBSAMPLE));
		assertTrue(
				"Unable to delete style '" + TEST_STYLE_NAME_MAJOR_SUBSAMPLE + "'",
				geoserverServiceClient.deleteStyle(TEST_STYLE_NAME_MAJOR_SUBSAMPLE));
	}

	/**
	 * 
	 * @param bi
	 *            sample
	 * @param ref
	 *            reference
	 * @param minPctError
	 *            used for testing subsampling - to ensure we are properly
	 *            subsampling we want there to be some error if subsampling is
	 *            aggressive (10 pixels)
	 * @param maxPctError
	 *            used for testing subsampling - we want to ensure at most we
	 *            are off by this percentile
	 */
	private static void testTileAgainstReference(
			final BufferedImage bi,
			final BufferedImage ref,
			final double minPctError,
			final double maxPctError ) {
		Assert.assertEquals(
				ref.getWidth(),
				bi.getWidth());
		Assert.assertEquals(
				ref.getHeight(),
				bi.getHeight());
		final int totalPixels = ref.getWidth() * ref.getHeight();
		final int minErrorPixels = (int) Math.round(minPctError * totalPixels);
		final int maxErrorPixels = (int) Math.round(maxPctError * totalPixels);
		int errorPixels = 0;
		// test under default style
		for (int x = 0; x < ref.getWidth(); x++) {
			for (int y = 0; y < ref.getHeight(); y++) {
				if (!(bi.getRGB(
						x,
						y) == ref.getRGB(
						x,
						y))) {
					errorPixels++;
					if (errorPixels > maxErrorPixels) {
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
		if (errorPixels < minErrorPixels) {
			Assert.fail(String.format(
					"Subsampling did not work as expected; error pixels (%d) did not exceed the minimum threshold (%d)",
					errorPixels,
					minErrorPixels));
		}
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
						GEOSERVER_USER,
						GEOSERVER_PASS));

		return HttpClientBuilder.create().setDefaultCredentialsProvider(
				provider).build();
	}

}
