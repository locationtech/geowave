package mil.nga.giat.geowave.test.services;

import static org.junit.Assert.*;

import javax.ws.rs.core.Response;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloRequiredOptions;
import mil.nga.giat.geowave.service.client.BaseServiceClient;
import mil.nga.giat.geowave.service.client.ConfigServiceClient;
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
public class IngestServicesIT
{
	private static final Logger LOGGER = LoggerFactory.getLogger(IngestServicesIT.class);
	private static IngestServiceClient ingestServiceClient;
	private static ConfigServiceClient configServiceClient;
	private static BaseServiceClient baseServiceClient;

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStorePluginOptions;

	private static String uid;
	private static long startMillis;
	private final static String testName = "IngestServicesIT";

	@BeforeClass
	public static void setup() {
		ingestServiceClient = new IngestServiceClient(
				ServicesTestEnvironment.GEOWAVE_BASE_URL);
		configServiceClient = new ConfigServiceClient(
				ServicesTestEnvironment.GEOWAVE_BASE_URL);
		baseServiceClient = new BaseServiceClient(
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
	public void testLocalToGW()
			throws ParseException,
			InterruptedException {
		configServiceClient.addStore(
				"test-store",
				dataStorePluginOptions.getType(),
				null,
				dataStorePluginOptions.getOptionsAsMap());
		configServiceClient.addSpatialIndex(
				"test-index",
				false,
				null,
				32,
				"ROUND_ROBIN",
				null,
				null);
		Response ingest = ingestServiceClient.localToGW(
				"data/hail_test_case/hail.shp",
				"test-stork",
				"test-index",
				null,
				null,
				null,
				"geotools-vector");

		TestUtils.assert200(
				"The ingest should begin and return 200",
				ingest.getStatus());

		String ingestResponse = ingest.readEntity(String.class);
		JSONObject ingestResponseObj = (JSONObject) new JSONParser().parse(ingestResponse);
		uid = (String) ingestResponseObj.get("data");
		assertTrue(uid != null);

		Response status = baseServiceClient.operation_status(uid);
		String statusResponse = status.readEntity(String.class);
		JSONObject statusObj = (JSONObject) new JSONParser().parse("statusResponse");
		String opStatus = (String) statusObj.get("status");

		while (opStatus.equals("RUNNING")) {
			Thread.sleep(5000);
			status = baseServiceClient.operation_status(uid);
			statusResponse = status.readEntity(String.class);
			statusObj = (JSONObject) new JSONParser().parse(statusResponse);
			opStatus = (String) statusObj.get("status");
		}

		assertTrue(
				"The status should be returning COMPLETE, instead returns " + opStatus,
				opStatus.equals("COMPLETE"));

	}

}
