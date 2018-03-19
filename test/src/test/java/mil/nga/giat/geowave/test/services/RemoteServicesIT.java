package mil.nga.giat.geowave.test.services;

import javax.ws.rs.core.Response;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.service.client.ConfigServiceClient;
import mil.nga.giat.geowave.service.client.RemoteServiceClient;
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
public class RemoteServicesIT
{

	private static final Logger LOGGER = LoggerFactory.getLogger(GeoServerServicesIT.class);
	private static RemoteServiceClient remoteServiceClient;
	private static ConfigServiceClient configServiceClient;

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStorePluginOptions;

	private static long startMillis;
	private final static String testName = "RemoteServicesIT";

	private String storeName = "store-under-remote-test";

	@BeforeClass
	public static void setup() {
		// ZipUtils.unZipFile(
		// new File(
		// GeoWaveServicesIT.class.getClassLoader().getResource(
		// TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
		// TestUtils.TEST_CASE_BASE);
		configServiceClient = new ConfigServiceClient(
				ServicesTestEnvironment.GEOWAVE_BASE_URL);
		remoteServiceClient = new RemoteServiceClient(
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
	public void simpleTest() {
		Response r = remoteServiceClient.listStats(storeName);
		TestUtils.assert404(
				"Should not be able to find a non-existent store",
				r.getStatus());
		System.out.println(r.readEntity(String.class));
	}

}
