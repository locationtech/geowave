package mil.nga.giat.geowave.test.osm;

import java.io.File;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.cli.osm.operations.IngestOSMToGeoWaveCommand;
import mil.nga.giat.geowave.cli.osm.operations.StageOSMToHDFSCommand;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloRequiredOptions;
import mil.nga.giat.geowave.test.GeoWaveDFSTestEnvironment;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;

public class MapReduceIT extends
		GeoWaveDFSTestEnvironment
{

	private final static Logger LOGGER = LoggerFactory.getLogger(MapReduceIT.class);

	protected static final String TEST_RESOURCE_DIR = new File(
			"./src/test/resources/osm/").getAbsolutePath().toString();
	protected static final String TEST_DATA_ZIP_RESOURCE_PATH = TEST_RESOURCE_DIR + "/" + "andorra-latest.zip";
	protected static final String TEST_DATA_BASE_DIR = new File(
			"./target/data/").getAbsoluteFile().toString();

	@BeforeClass
	public static void setupTestData()
			throws ZipException {
		ZipFile data = new ZipFile(
				new File(
						TEST_DATA_ZIP_RESOURCE_PATH));
		data.extractAll(TEST_DATA_BASE_DIR);

	}

	@Test
	public void testIngestOSMPBF()
			throws Exception {

		// NOTE: This will probably fail unless you bump up the memory for the
		// tablet
		// servers, for whatever reason, using the
		// miniAccumuloConfig.setMemory() function.

		String hdfsPath = "/user/" + System.getProperty("user.name") + "/osm_stage/";

		StageOSMToHDFSCommand stage = new StageOSMToHDFSCommand();
		stage.setParameters(
				TEST_DATA_BASE_DIR,
				NAME_NODE,
				hdfsPath);
		stage.execute(new ManualOperationParams());

		Connector conn = new ZooKeeperInstance(
				accumuloInstance,
				zookeeper).getConnector(
				accumuloUser,
				new PasswordToken(
						accumuloPassword));
		Authorizations auth = new Authorizations(
				new String[] {
					"public"
				});
		conn.securityOperations().changeUserAuthorizations(
				accumuloUser,
				auth);

		DataStorePluginOptions pluginOptions = new DataStorePluginOptions();
		pluginOptions.selectPlugin("accumulo");
		AccumuloRequiredOptions opts = new AccumuloRequiredOptions();
		opts.setZookeeper(zookeeper);
		opts.setInstance(accumuloInstance);
		opts.setUser(accumuloUser);
		opts.setPassword(accumuloPassword);
		opts.setGeowaveNamespace("osmnamespace");
		pluginOptions.setFactoryOptions(opts);

		IngestOSMToGeoWaveCommand ingest = new IngestOSMToGeoWaveCommand();
		ingest.setParameters(
				NAME_NODE,
				hdfsPath,
				null);
		ingest.setInputStoreOptions(pluginOptions);

		ingest.getIngestOptions().setJobName(
				"ConversionTest");

		// Execute for node's ways, and relations.
		ingest.getIngestOptions().setMapperType(
				"NODE");
		ingest.execute(new ManualOperationParams());
		System.out.println("finished accumulo ingest Node");

		ingest.getIngestOptions().setMapperType(
				"WAY");
		ingest.execute(new ManualOperationParams());
		System.out.println("finished accumulo ingest Way");

		ingest.getIngestOptions().setMapperType(
				"RELATION");
		ingest.execute(new ManualOperationParams());
		System.out.println("finished accumulo ingest Relation");
	}
}
