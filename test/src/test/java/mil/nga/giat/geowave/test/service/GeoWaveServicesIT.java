package mil.nga.giat.geowave.test.service;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.format.gpx.GpxUtils;
import mil.nga.giat.geowave.service.client.GeoserverServiceClient;
import mil.nga.giat.geowave.service.client.InfoServiceClient;
import mil.nga.giat.geowave.service.client.IngestServiceClient;
import mil.nga.giat.geowave.test.GeoWaveTestEnvironment;
import mil.nga.giat.geowave.test.mapreduce.MapReduceTestEnvironment;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

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

public class GeoWaveServicesIT extends
		ServicesTestEnvironment
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveServicesIT.class);

	protected static final String TEST_DATA_ZIP_RESOURCE_PATH = TEST_RESOURCE_PACKAGE + "mapreduce-testdata.zip";
	protected static final String TEST_CASE_GENERAL_GPX_BASE = TEST_CASE_BASE + "general_gpx_test_case/";
	protected static final String GENERAL_GPX_INPUT_GPX_DIR = TEST_CASE_GENERAL_GPX_BASE + "input_gpx/";
	private static final String ASHLAND_GPX_FILE = GENERAL_GPX_INPUT_GPX_DIR + "ashland.gpx";
	private static final String ASHLAND_INGEST_TYPE = "gpx";
	private static final String TEST_STYLE_NAME = "DecimatePoints";
	private static final String TEST_STYLE_PATH = "src/test/resources/sld/";
	private static final String TEST_SLD_FILE = TEST_STYLE_PATH + TEST_STYLE_NAME + ".sld";

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
				TEST_NAMESPACE,
				null,
				ASHLAND_INGEST_TYPE,
				null,
				false);
		assertTrue(success);
		success = false;

		// verify that the namespace was created
		LOGGER.info("Verify that the namespace was created via localIngest.");
		JSONArray namespaces = infoServiceClient.getNamespaces().getJSONArray(
				"namespaces");
		for (int i = 0; i < namespaces.size(); i++) {
			if (namespaces.getJSONObject(
					i).getString(
					"name").equals(
					TEST_NAMESPACE)) {
				success = true;
				break;
			}
		}
		assertTrue(success);
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
				TEST_NAMESPACE,
				null,
				ASHLAND_INGEST_TYPE,
				null,
				false);
		assertTrue(success);
		success = false;

		// verify that the namespace was created
		LOGGER.info("Verify that the namespace was created via localIngest.");
		namespaces = infoServiceClient.getNamespaces().getJSONArray(
				"namespaces");
		for (int i = 0; i < namespaces.size(); i++) {
			if (namespaces.getJSONObject(
					i).getString(
					"name").equals(
					TEST_NAMESPACE)) {
				success = true;
				break;
			}
		}
		assertTrue(success);
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
		assertTrue(success);
		success = false;

		// verify the index type
		LOGGER.info("Verify the index type.");
		final JSONArray indices = infoServiceClient.getIndices(
				TEST_NAMESPACE).getJSONArray(
				"indices");
		for (int i = 0; i < indices.size(); i++) {
			if (indices.getJSONObject(
					i).getString(
					"name").equals(
					IndexType.SPATIAL_VECTOR.getDefaultId())) {
				success = true;
				break;
			}
		}
		assertTrue(success);
		success = false;

		// *****************************************************
		// Geowave Service Client Test
		// *****************************************************

		// create the workspace
		LOGGER.info("Create the test workspace.");
		assertTrue(geoserverServiceClient.createWorkspace(TEST_WORKSPACE));

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
		assertTrue(success);
		success = false;

		// upload the default style
		LOGGER.info("Upload the default style.");
		assertTrue(geoserverServiceClient.publishStyle(new File[] {
			new File(
					TEST_SLD_FILE)
		}));

		// verify that the style was uploaded
		LOGGER.info("Verify that the style was uploaded.");
		final JSONArray styles = geoserverServiceClient.getStyles().getJSONArray(
				"styles");
		for (int i = 0; i < styles.size(); i++) {
			if (styles.getJSONObject(
					i).getString(
					"name").equals(
					TEST_STYLE_NAME)) {
				success = true;
				break;
			}
		}
		assertTrue(success);
		success = false;

		// verify that we can recall the stored style
		LOGGER.info("Verify that we can recall the stored style.");
		final String style = IOUtils.toString(geoserverServiceClient.getStyle(TEST_STYLE_NAME));
		assertTrue((style != null) && !style.isEmpty());

		// verify that we can publish a datastore
		LOGGER.info("Verify that we can publish a datastore.");
		assertTrue(geoserverServiceClient.publishDatastore(
				zookeeper,
				accumuloUser,
				accumuloPassword,
				accumuloInstance,
				TEST_NAMESPACE,
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
		assertTrue(success);
		success = false;

		if (dsInfo != null) {

			assertTrue(dsInfo.getString(
					"Namespace").equals(
					TEST_NAMESPACE));

			assertTrue(dsInfo.getString(
					"ZookeeperServers").equals(
					zookeeper));

			assertTrue(dsInfo.getString(
					"InstanceName").equals(
					accumuloInstance));
		}

		// verify that we can recall the datastore
		LOGGER.info("Verify that we can recall the datastore.");
		final JSONObject datastore = geoserverServiceClient.getDatastore(
				TEST_NAMESPACE,
				TEST_WORKSPACE);
		assertTrue(datastore.getJSONObject(
				"dataStore").getString(
				"name").equals(
				TEST_NAMESPACE));

		// verify that we can publish a layer
		LOGGER.info("Verify that we can publish a layer.");
		assertTrue(geoserverServiceClient.publishLayer(
				TEST_NAMESPACE,
				TEST_STYLE_NAME,
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
		assertTrue(success);
		success = false;

		// verify that we can recall the layer
		LOGGER.info("Verify that we can recall the layer.");
		final JSONObject layer = geoserverServiceClient.getLayer(GpxUtils.GPX_WAYPOINT_FEATURE);
		assertTrue(layer.getJSONObject(
				"layer").getString(
				"name").equals(
				GpxUtils.GPX_WAYPOINT_FEATURE));

		// verify that we are able to delete
		LOGGER.info("Verify that we are able to clean up.");
		assertTrue(geoserverServiceClient.deleteLayer(GpxUtils.GPX_WAYPOINT_FEATURE));
		assertTrue(geoserverServiceClient.deleteDatastore(
				TEST_NAMESPACE,
				TEST_WORKSPACE));
		assertTrue(geoserverServiceClient.deleteStyle(TEST_STYLE_NAME));
		assertTrue(geoserverServiceClient.deleteWorkspace(TEST_WORKSPACE));
	}
}
