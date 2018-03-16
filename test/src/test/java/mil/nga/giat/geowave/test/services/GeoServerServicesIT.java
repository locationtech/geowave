package mil.nga.giat.geowave.test.services;

import javax.ws.rs.core.Response;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.service.client.BaseServiceClient;
import mil.nga.giat.geowave.service.client.ConfigServiceClient;
import mil.nga.giat.geowave.service.client.GeoServerServiceClient;
import mil.nga.giat.geowave.service.client.IngestServiceClient;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.annotation.Environments;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.Environments.Environment;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.SERVICES
})
public class GeoServerServicesIT
{

	private static final Logger LOGGER = LoggerFactory.getLogger(GeoServerServicesIT.class);
	private static GeoServerServiceClient geoServerServiceClient;
	private static ConfigServiceClient configServiceClient;

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
		// ZipUtils.unZipFile(
		// new File(
		// GeoWaveServicesIT.class.getClassLoader().getResource(
		// TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
		// TestUtils.TEST_CASE_BASE);
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

	@Before
	public void setUp() {
		// ingest data here
		// create store
		configServiceClient.configGeoServer("localhost:9011/geoserver");
	}

	@Test
	public void addCoverageStore() {

	}

	@Test
	public void addCoverage() {

	}

	@Test
	public void addFeatureLayer() {

	}

	@Test
	public void addLayer() {

	}

	@Test
	public void addStyle() {

	}

	@Test
	public void addRemoveWorkspace() {
		geoServerServiceClient.removeWorkspace("test-workspace");

		Response addWs = geoServerServiceClient.addWorkspace("test-workspace");
		String s1 = addWs.readEntity(String.class);
	TestUtils.assert200(
				"The workspace should be created successfully and return 200",
				addWs.getStatus());

		Response addWs2 = geoServerServiceClient.addWorkspace("test-workspace");
		String s2 = addWs2.readEntity(String.class);
		TestUtils.assert400(
				"The workspace should already exist so this should return 400",
				addWs2.getStatus());

		Response rmWs = geoServerServiceClient.removeWorkspace("test-workspace");
		TestUtils.assert200(
				"The workspace should be deleted successfully and return 200",
				rmWs.getStatus());
		String s3 = rmWs.readEntity(String.class);

		Response rmWs2 = geoServerServiceClient.removeWorkspace("test-workspace");
		TestUtils.assert404(
				"Should return 404 because that workspace does not exist",
				rmWs2.getStatus());
		String s4 = rmWs2.readEntity(String.class);
	}
}
