package mil.nga.giat.geowave.test.service;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.geotools.feature.SchemaException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;

import mil.nga.giat.geowave.adapter.raster.util.ZipUtils;
import mil.nga.giat.geowave.core.store.config.ConfigOption;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.format.gpx.GpxUtils;
import mil.nga.giat.geowave.service.client.GeoserverServiceClient;
import mil.nga.giat.geowave.service.client.InfoServiceClient;
import mil.nga.giat.geowave.service.client.IngestServiceClient;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestDataStoreOptions;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.annotation.Environments;
import mil.nga.giat.geowave.test.annotation.Environments.Environment;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.SERVICES
})
public class GeoWaveServicesIT
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveServicesIT.class);

	protected static final String TEST_DATA_ZIP_RESOURCE_PATH = TestUtils.TEST_RESOURCE_PACKAGE
			+ "mapreduce-testdata.zip";
	protected static final String TEST_CASE_GENERAL_GPX_BASE = TestUtils.TEST_CASE_BASE + "general_gpx_test_case/";
	protected static final String GENERAL_GPX_INPUT_GPX_DIR = TEST_CASE_GENERAL_GPX_BASE + "input_gpx/";
	private static final String ASHLAND_GPX_FILE = GENERAL_GPX_INPUT_GPX_DIR + "ashland.gpx";
	private static final String ASHLAND_INGEST_TYPE = "gpx";

	private static InfoServiceClient infoServiceClient;
	private static GeoserverServiceClient geoserverServiceClient;
	private static IngestServiceClient ingestServiceClient;

	@GeoWaveTestStore({
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStoreOptions;

	private static long startMillis;

	@BeforeClass
	public static void extractTestFiles()
			throws URISyntaxException {
		ZipUtils.unZipFile(
				new File(
						GeoWaveServicesIT.class.getClassLoader().getResource(
								TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
				TestUtils.TEST_CASE_BASE);

		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*         RUNNING GeoWaveServicesIT     *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*      FINISHED GeoWaveServicesIT       *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Test
	public void testServices()
			throws IOException,
			SchemaException {

		// initialize the service clients
		// just use the default user, password
		geoserverServiceClient = new GeoserverServiceClient(
				ServicesTestEnvironment.GEOWAVE_BASE_URL,
				ServicesTestEnvironment.GEOSERVER_USER,
				ServicesTestEnvironment.GEOSERVER_PASS);
		infoServiceClient = new InfoServiceClient(
				ServicesTestEnvironment.GEOWAVE_BASE_URL);
		ingestServiceClient = new IngestServiceClient(
				ServicesTestEnvironment.GEOWAVE_BASE_URL);

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
				((TestDataStoreOptions) dataStoreOptions).getStoreType().name(),
				TestUtils.TEST_NAMESPACE,
				null,
				ASHLAND_INGEST_TYPE,
				null,
				false);

		assertTrue(
				"Unable to ingest '" + ASHLAND_GPX_FILE + "'",
				success);
		success = false;

		TestUtils.deleteAll(dataStoreOptions);

		// ingest data using the local ingest service
		LOGGER.info("Ingesting data using the hdfs ingest service.");
		success = ingestServiceClient.hdfsIngest(
				new File[] {
					new File(
							ASHLAND_GPX_FILE)
				},
				((TestDataStoreOptions) dataStoreOptions).getStoreType().name(),
				TestUtils.TEST_NAMESPACE,
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
				((TestDataStoreOptions) dataStoreOptions).getStoreType().name()).getJSONArray(
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
				((TestDataStoreOptions) dataStoreOptions).getStoreType().name()).getJSONArray(
				"indices");
		for (int i = 0; i < indices.size(); i++) {
			if (indices.getJSONObject(
					i).getString(
					"name").equals(
					TestUtils.DEFAULT_SPATIAL_INDEX.getId().getString())) {
				success = true;
				break;
			}
		}
		assertTrue(
				"Unable to find index '" + TestUtils.DEFAULT_SPATIAL_INDEX.getId().getString() + "'",
				success);
		success = false;

		// *****************************************************
		// Geowave Service Client Test
		// *****************************************************

		// create the workspace
		LOGGER.info("Create the test workspace.");
		assertTrue(
				"Unable to create workspace '" + ServicesTestEnvironment.TEST_WORKSPACE + "'",
				geoserverServiceClient.createWorkspace(ServicesTestEnvironment.TEST_WORKSPACE));

		// verify that the workspace was created
		LOGGER.info("Verify that the workspace was created.");
		final JSONArray workspaces = geoserverServiceClient.getWorkspaces().getJSONArray(
				"workspaces");
		for (int i = 0; i < workspaces.size(); i++) {
			if (workspaces.getJSONObject(
					i).getString(
					"name").equals(
					ServicesTestEnvironment.TEST_WORKSPACE)) {
				success = true;
				break;
			}
		}
		assertTrue(
				"Unable to find workspace '" + ServicesTestEnvironment.TEST_WORKSPACE + "'",
				success);
		success = false;

		// upload the default style
		LOGGER.info("Upload the default style.");
		assertTrue(
				"Unable to publish style '" + ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE + "'",
				geoserverServiceClient.publishStyle(new File[] {
					new File(
							ServicesTestEnvironment.TEST_SLD_NO_DIFFERENCE_FILE)
				}));
		assertTrue(
				"Unable to publish style '" + ServicesTestEnvironment.TEST_STYLE_NAME_MINOR_SUBSAMPLE + "'",
				geoserverServiceClient.publishStyle(new File[] {
					new File(
							ServicesTestEnvironment.TEST_SLD_MINOR_SUBSAMPLE_FILE)
				}));

		// verify that the style was uploaded
		LOGGER.info("Verify that the style was uploaded.");
		final JSONArray styles = geoserverServiceClient.getStyles().getJSONArray(
				"styles");
		for (int i = 0; i < styles.size(); i++) {
			if (styles.getJSONObject(
					i).getString(
					"name").equals(
					ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE)) {
				success = true;
				break;
			}
		}
		assertTrue(
				"Unable to find style '" + ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE + "'",
				success);
		success = false;
		for (int i = 0; i < styles.size(); i++) {
			if (styles.getJSONObject(
					i).getString(
					"name").equals(
					ServicesTestEnvironment.TEST_STYLE_NAME_MINOR_SUBSAMPLE)) {
				success = true;
				break;
			}
		}
		assertTrue(
				"Unable to find style '" + ServicesTestEnvironment.TEST_STYLE_NAME_MINOR_SUBSAMPLE + "'",
				success);
		success = false;
		// verify that we can recall the stored style
		LOGGER.info("Verify that we can recall the stored style.");
		final String style = IOUtils.toString(geoserverServiceClient
				.getStyle(ServicesTestEnvironment.TEST_SLD_NO_DIFFERENCE_FILE));
		assertTrue(
				"Unable to get style '" + ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE + "'",
				(style != null) && !style.isEmpty());

		// verify that we can publish a datastore
		LOGGER.info("Verify that we can publish a datastore.");
		assertTrue(
				"Unable to publish datastore",
				geoserverServiceClient.publishDatastore(
						dataStoreOptions.getType(),
						dataStoreOptions.getOptionsAsMap(),
						TestUtils.TEST_NAMESPACE,
						null,
						null,
						null,
						null,
						ServicesTestEnvironment.TEST_WORKSPACE));

		// verify that the datastore was published
		LOGGER.info("Verify that the datastore was published.");
		final JSONArray datastores = geoserverServiceClient.getDatastores(
				ServicesTestEnvironment.TEST_WORKSPACE).getJSONArray(
				"dataStores");

		JSONObject dsInfo = null;
		for (int i = 0; i < datastores.size(); i++) {
			if (datastores.getJSONObject(
					i).getString(
					"name").equals(
					TestUtils.TEST_NAMESPACE)) {
				dsInfo = datastores.getJSONObject(i);
				success = true;
				break;
			}
		}
		assertTrue(
				"Unable to get datastore",
				success);
		success = false;

		if (dsInfo != null) {
			final Map<String, String> options = dataStoreOptions.getOptionsAsMap();
			final List<ConfigOption> configOptions = Arrays.asList(ConfigUtils.createConfigOptionsFromJCommander(
					dataStoreOptions,
					false));
			final Collection<String> nonPasswordRequiredFields = Collections2.transform(
					Collections2.filter(
							configOptions,
							new Predicate<ConfigOption>() {

								@Override
								public boolean apply(
										final ConfigOption input ) {
									return !input.isPassword() && !input.isOptional();
								}
							}),
					new Function<ConfigOption, String>() {

						@Override
						public String apply(
								final ConfigOption input ) {
							return input.getName();
						}
					});
			for (final Entry<String, String> entry : options.entrySet()) {
				if (nonPasswordRequiredFields.contains(entry.getKey())) {
					assertTrue(
							"Unable to get datastore option '" + entry.getKey() + "'",
							dsInfo.getString(
									entry.getKey()).equals(
									entry.getValue()));
				}
			}
		}

		// verify that we can recall the datastore
		LOGGER.info("Verify that we can recall the datastore.");
		final JSONObject datastore = geoserverServiceClient.getDatastore(
				TestUtils.TEST_NAMESPACE,
				ServicesTestEnvironment.TEST_WORKSPACE);
		assertTrue(
				"Unable to publish datastore",
				datastore.getJSONObject(
						"dataStore").getString(
						"name").equals(
						TestUtils.TEST_NAMESPACE));

		// verify that we can publish a layer
		LOGGER.info("Verify that we can publish a layer.");
		assertTrue(
				"Unable to publish layer '" + GpxUtils.GPX_WAYPOINT_FEATURE + "'",
				geoserverServiceClient.publishLayer(
						TestUtils.TEST_NAMESPACE,
						ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE,
						GpxUtils.GPX_WAYPOINT_FEATURE,
						ServicesTestEnvironment.TEST_WORKSPACE));

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
				"Unable to get layer",
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
						TestUtils.TEST_NAMESPACE,
						ServicesTestEnvironment.TEST_WORKSPACE));
		assertTrue(
				"Unable to delete style '" + ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE + "'",
				geoserverServiceClient.deleteStyle(ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE));

		assertTrue(
				"Unable to delete style '" + ServicesTestEnvironment.TEST_STYLE_NAME_MINOR_SUBSAMPLE + "'",
				geoserverServiceClient.deleteStyle(ServicesTestEnvironment.TEST_STYLE_NAME_MINOR_SUBSAMPLE));
		assertTrue(
				"Unable to delete workspace",
				geoserverServiceClient.deleteWorkspace(ServicesTestEnvironment.TEST_WORKSPACE));
	}
}
