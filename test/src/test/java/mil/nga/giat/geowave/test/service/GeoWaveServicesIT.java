package mil.nga.giat.geowave.test.service;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.io.IOUtils;
import org.geotools.feature.SchemaException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloStoreFactoryFamily;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloRequiredOptions;
import mil.nga.giat.geowave.format.gpx.GpxUtils;
import mil.nga.giat.geowave.service.client.GeoserverServiceClient;
import mil.nga.giat.geowave.service.client.InfoServiceClient;
import mil.nga.giat.geowave.service.client.IngestServiceClient;
import mil.nga.giat.geowave.test.GeoWaveTestEnvironment;
import mil.nga.giat.geowave.test.mapreduce.MapReduceTestEnvironment;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class GeoWaveServicesIT extends
		ServicesTestEnvironment
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveServicesIT.class);

	protected static final String TEST_DATA_ZIP_RESOURCE_PATH = TEST_RESOURCE_PACKAGE + "mapreduce-testdata.zip";
	protected static final String TEST_CASE_GENERAL_GPX_BASE = TEST_CASE_BASE + "general_gpx_test_case/";
	protected static final String GENERAL_GPX_INPUT_GPX_DIR = TEST_CASE_GENERAL_GPX_BASE + "input_gpx/";
	private static final String ASHLAND_GPX_FILE = GENERAL_GPX_INPUT_GPX_DIR + "ashland.gpx";
	private static final String ASHLAND_INGEST_TYPE = "gpx";

	private static InfoServiceClient infoServiceClient;
	private static GeoserverServiceClient geoserverServiceClient;
	private static IngestServiceClient ingestServiceClient;

	@BeforeClass
	public static void extractTestFiles()
			throws URISyntaxException {
		GeoWaveTestEnvironment.unZipFile(
				new File(
						MapReduceTestEnvironment.class.getClassLoader().getResource(
								TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
				TEST_CASE_BASE);
	}

	@Test
	public void testServices()
			throws IOException,
			SchemaException {

		// initialize the service clients
		geoserverServiceClient = new GeoserverServiceClient(
				GEOWAVE_BASE_URL);
		infoServiceClient = new InfoServiceClient(
				GEOWAVE_BASE_URL);
		ingestServiceClient = new IngestServiceClient(
				GEOWAVE_BASE_URL);

		// *****************************************************
		// Ingest Service Client Test
		// *****************************************************

		boolean success = false;

		// ingest data using the local ingest service
		LOGGER.info("Ingesting data using the local ingest service.");

		success = ingestServiceClient.localIngest(
				new File[] {
					new File(
							ASHLAND_GPX_FILE)
				},
				ACCUMULO_STORE_NAME,
				TEST_NAMESPACE,
				null,
				ASHLAND_INGEST_TYPE,
				null,
				false);

		assertTrue(
				"Unable to ingest '" + ASHLAND_GPX_FILE + "'",
				success);
		success = false;

		try {
			accumuloOperations.deleteAll();
		}
		catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
			LOGGER.error(
					"Unable to clear accumulo namespace",
					ex);
			Assert.fail("Index not deleted successfully");
		}

		// ingest data using the local ingest service
		LOGGER.info("Ingesting data using the hdfs ingest service.");
		success = ingestServiceClient.hdfsIngest(
				new File[] {
					new File(
							ASHLAND_GPX_FILE)
				},
				ACCUMULO_STORE_NAME,
				TEST_NAMESPACE,
				null,
				ASHLAND_INGEST_TYPE,
				null,
				false);
		assertTrue(
				"Unable to ingest '" + ASHLAND_GPX_FILE + "'",
				success);
		success = false;

		// verify the adapter type
		LOGGER.info("Verify the adapter type.");
		final JSONArray adapters = infoServiceClient.getAdapters(
				TEST_NAMESPACE).getJSONArray(
				"adapters");
		for (int i = 0; i < adapters.size(); i++) {
			if (adapters.getJSONObject(
					i).getString(
					"name").equals(
					GpxUtils.GPX_WAYPOINT_FEATURE)) {
				success = true;
				break;
			}
		}
		assertTrue(
				"Unable to find adapter '" + GpxUtils.GPX_WAYPOINT_FEATURE + "'",
				success);
		success = false;

		// verify the index type
		LOGGER.info("Verify the index type.");
		final JSONArray indices = infoServiceClient.getIndices(
				TEST_NAMESPACE).getJSONArray(
				"indices");
		final String expectedIndex = new SpatialDimensionalityTypeProvider().getDimensionalityTypeName();
		for (int i = 0; i < indices.size(); i++) {
			if (indices.getJSONObject(
					i).getString(
					"name").equals(
					expectedIndex)) {
				success = true;
				break;
			}
		}
		assertTrue(
				"Unable to find index '" + expectedIndex + "'",
				success);
		success = false;

		// *****************************************************
		// Geowave Service Client Test
		// *****************************************************

		// create the workspace
		LOGGER.info("Create the test workspace.");
		assertTrue(
				"Unable to create workspace '" + TEST_WORKSPACE + "'",
				geoserverServiceClient.createWorkspace(TEST_WORKSPACE));

		// verify that the workspace was created
		LOGGER.info("Verify that the workspace was created.");
		final JSONArray workspaces = geoserverServiceClient.getWorkspaces().getJSONArray(
				"workspaces");
		for (int i = 0; i < workspaces.size(); i++) {
			if (workspaces.getJSONObject(
					i).getString(
					"name").equals(
					TEST_WORKSPACE)) {
				success = true;
				break;
			}
		}
		assertTrue(
				"Unable to find workspace '" + TEST_WORKSPACE + "'",
				success);
		success = false;

		// upload the default style
		LOGGER.info("Upload the default style.");
		assertTrue(
				"Unable to publish style '" + TEST_STYLE_NAME_NO_DIFFERENCE + "'",
				geoserverServiceClient.publishStyle(new File[] {
					new File(
							TEST_SLD_NO_DIFFERENCE_FILE)
				}));
		assertTrue(
				"Unable to publish style '" + TEST_STYLE_NAME_MINOR_SUBSAMPLE + "'",
				geoserverServiceClient.publishStyle(new File[] {
					new File(
							TEST_SLD_MINOR_SUBSAMPLE_FILE)
				}));

		// verify that the style was uploaded
		LOGGER.info("Verify that the style was uploaded.");
		final JSONArray styles = geoserverServiceClient.getStyles().getJSONArray(
				"styles");
		for (int i = 0; i < styles.size(); i++) {
			if (styles.getJSONObject(
					i).getString(
					"name").equals(
					TEST_STYLE_NAME_NO_DIFFERENCE)) {
				success = true;
				break;
			}
		}
		assertTrue(
				"Unable to find style '" + TEST_STYLE_NAME_NO_DIFFERENCE + "'",
				success);
		success = false;
		for (int i = 0; i < styles.size(); i++) {
			if (styles.getJSONObject(
					i).getString(
					"name").equals(
					TEST_STYLE_NAME_MINOR_SUBSAMPLE)) {
				success = true;
				break;
			}
		}
		assertTrue(
				"Unable to find style '" + TEST_STYLE_NAME_MINOR_SUBSAMPLE + "'",
				success);
		success = false;
		// verify that we can recall the stored style
		LOGGER.info("Verify that we can recall the stored style.");
		final String style = IOUtils.toString(geoserverServiceClient.getStyle(TEST_SLD_NO_DIFFERENCE_FILE));
		assertTrue(
				"Unable to get style '" + TEST_STYLE_NAME_NO_DIFFERENCE + "'",
				(style != null) && !style.isEmpty());

		// verify that we can publish a datastore
		LOGGER.info("Verify that we can publish a datastore.");
		assertTrue(
				"Unable to publish accumulo datastore",
				geoserverServiceClient.publishDatastore(
						new AccumuloStoreFactoryFamily().getName(),
						getAccumuloConfig(),
						TEST_NAMESPACE,
						null,
						null,
						null,
						null,
						TEST_WORKSPACE));

		// verify that the datastore was published
		LOGGER.info("Verify that the datastore was published.");
		final JSONArray datastores = geoserverServiceClient.getDatastores(
				TEST_WORKSPACE).getJSONArray(
				"dataStores");

		JSONObject dsInfo = null;
		for (int i = 0; i < datastores.size(); i++) {
			if (datastores.getJSONObject(
					i).getString(
					"name").equals(
					TEST_NAMESPACE)) {
				dsInfo = datastores.getJSONObject(i);
				success = true;
				break;
			}
		}
		assertTrue(
				"Unable to get accumulo datastore",
				success);
		success = false;

		if (dsInfo != null) {

			assertTrue(
					"Unable to get accumulo datastore namespace",
					dsInfo.getString(
							StoreFactoryOptions.GEOWAVE_NAMESPACE_OPTION).equals(
							TEST_NAMESPACE));

			assertTrue(
					"Unable to publish accumulo datastore zookeeper",
					dsInfo.getString(
							AccumuloRequiredOptions.ZOOKEEPER_CONFIG_KEY).equals(
							zookeeper));

			assertTrue(
					"Unable to publish accumulo datastore instance",
					dsInfo.getString(
							AccumuloRequiredOptions.INSTANCE_CONFIG_KEY).equals(
							accumuloInstance));
		}

		// verify that we can recall the datastore
		LOGGER.info("Verify that we can recall the datastore.");
		final JSONObject datastore = geoserverServiceClient.getDatastore(
				TEST_NAMESPACE,
				TEST_WORKSPACE);
		assertTrue(
				"Unable to publish accumulo datastore",
				datastore.getJSONObject(
						"dataStore").getString(
						"name").equals(
						TEST_NAMESPACE));

		// verify that we can publish a layer
		LOGGER.info("Verify that we can publish a layer.");
		assertTrue(
				"Unable to publish layer '" + GpxUtils.GPX_WAYPOINT_FEATURE + "'",
				geoserverServiceClient.publishLayer(
						TEST_NAMESPACE,
						TEST_STYLE_NAME_NO_DIFFERENCE,
						GpxUtils.GPX_WAYPOINT_FEATURE,
						TEST_WORKSPACE));

		// verify that the layer was published
		LOGGER.info("Verify that the layer was published.");
		final JSONArray layers = geoserverServiceClient.getLayers().getJSONArray(
				"layers").getJSONObject(
				0).getJSONArray(
				"layers");
		for (int i = 0; i < layers.size(); i++) {
			if (layers.getJSONObject(
					i).getString(
					"name").equals(
					GpxUtils.GPX_WAYPOINT_FEATURE)) {
				success = true;
				break;
			}
		}
		assertTrue(
				"Unable to get layer '" + GpxUtils.GPX_WAYPOINT_FEATURE + "'",
				success);
		success = false;

		// verify that we can recall the layer
		LOGGER.info("Verify that we can recall the layer.");
		final JSONObject layer = geoserverServiceClient.getLayer(GpxUtils.GPX_WAYPOINT_FEATURE);
		assertTrue(
				"Unable to publish accumulo datastore",
				layer.getJSONObject(
						"layer").getString(
						"name").equals(
						GpxUtils.GPX_WAYPOINT_FEATURE));

		// verify that we are able to delete
		LOGGER.info("Verify that we are able to clean up.");
		assertTrue(
				"Unable to delete layer '" + GpxUtils.GPX_WAYPOINT_FEATURE + "'",
				geoserverServiceClient.deleteLayer(GpxUtils.GPX_WAYPOINT_FEATURE));
		assertTrue(
				"Unable to delete datastore",
				geoserverServiceClient.deleteDatastore(
						TEST_NAMESPACE,
						TEST_WORKSPACE));
		assertTrue(
				"Unable to delete style '" + TEST_STYLE_NAME_NO_DIFFERENCE + "'",
				geoserverServiceClient.deleteStyle(TEST_STYLE_NAME_NO_DIFFERENCE));

		assertTrue(
				"Unable to delete style '" + TEST_STYLE_NAME_MINOR_SUBSAMPLE + "'",
				geoserverServiceClient.deleteStyle(TEST_STYLE_NAME_MINOR_SUBSAMPLE));
		assertTrue(
				"Unable to delete workspace",
				geoserverServiceClient.deleteWorkspace(TEST_WORKSPACE));
	}
}
