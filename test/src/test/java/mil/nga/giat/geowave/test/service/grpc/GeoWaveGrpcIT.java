package mil.nga.giat.geowave.test.service.grpc;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.adapter.raster.util.ZipUtils;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.ingest.operations.ConfigAWSCommand;
import mil.nga.giat.geowave.core.store.cli.config.AddIndexCommand;
import mil.nga.giat.geowave.core.store.cli.config.AddStoreCommand;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.cli.remote.options.IndexPluginOptions.PartitionStrategy;
import mil.nga.giat.geowave.core.store.cli.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.memory.MemoryRequiredOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.BasicIndexOptions;
import mil.nga.giat.geowave.datastore.hbase.cli.config.HBaseRequiredOptions;
import mil.nga.giat.geowave.service.grpc.GeoWaveGrpcServiceOptions;
import mil.nga.giat.geowave.service.grpc.protobuf.Feature;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.annotation.Environments;
import mil.nga.giat.geowave.test.annotation.Environments.Environment;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import mil.nga.giat.geowave.test.basic.AbstractGeoWaveBasicVectorIT;
import mil.nga.giat.geowave.test.kafka.BasicKafkaIT;
import mil.nga.giat.geowave.test.kafka.KafkaTestUtils;
import mil.nga.giat.geowave.test.mapreduce.MapReduceTestEnvironment;
import mil.nga.giat.geowave.test.mapreduce.MapReduceTestUtils;
import mil.nga.giat.geowave.test.service.grpc.GeoWaveGrpcTestClient;
import mil.nga.giat.geowave.test.service.grpc.GeoWaveGrpcTestServer;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.MAP_REDUCE,
	Environment.KAFKA,
	Environment.SPARK
})
@GeoWaveTestStore(value = {
	GeoWaveStoreType.HBASE
})
public class GeoWaveGrpcIT extends
		AbstractGeoWaveBasicVectorIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcIT.class);
	private static File configFile = null;
	private static GeoWaveGrpcTestServer server = null;
	private static GeoWaveGrpcTestClient client = null;

	protected static DataStorePluginOptions dataStore;
	public static ManualOperationParams operationParams = null;
	private static long startMillis;
	private static final boolean POINTS_ONLY = false;
	private static final int NUM_THREADS = 1;

	protected static final String TEST_DATA_ZIP_RESOURCE_PATH = TestUtils.TEST_RESOURCE_PACKAGE
			+ "mapreduce-testdata.zip";
	protected static final String OSM_GPX_INPUT_DIR = TestUtils.TEST_CASE_BASE + "osm_gpx_test_case/";

	@BeforeClass
	public static void reportTestStart()
			throws Exception {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*  RUNNING GeoWaveGrpcIT  *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn(ConfigOptions.getDefaultPropertyFile().getName());

		init();

	}

	@AfterClass
	public static void reportTestFinish() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED GeoWaveGrpcIT  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

		if (configFile.exists()) {
			configFile.delete();
		}
	}

	@Test
	public void testVectorIngest()
			throws InterruptedException,
			UnsupportedEncodingException {
		testVectorIngest(NUM_THREADS);
	}

	public void testVectorIngest(
			final int nthreads )
			throws InterruptedException,
			UnsupportedEncodingException {

		LOGGER.debug("Testing DataStore Type: " + dataStore.getType());

		// Ensure empty datastore
		TestUtils.deleteAll(dataStore);

		// variables for storing results and test returns
		String result = "";
		Map<String, String> map = null;

		// Core Mapreduce Tests
		client.configHDFSCommand();
		map = client.listCommand();
		Assert.assertEquals(
				GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfs(),
				map.get("hdfs.defaultFS.url"));

		// Core Ingest Tests
		Assert.assertTrue(client.LocalToHdfsCommand());
		Assert.assertTrue(client.LocalToGeowaveCommand());
		Assert.assertTrue(client.LocalToKafkaCommand());
		Assert.assertTrue(client.KafkaToGeowaveCommand());
		Assert.assertTrue(client.MapReduceToGeowaveCommand());
		List<String> plugins = client.ListPluginsCommand();
		Assert.assertNotEquals(
				0,
				plugins.size());
		Assert.assertTrue(client.LocalToMapReduceToGeowaveCommand());
		Assert.assertTrue(client.SparkToGeowaveCommand());

		// Vector Service Tests
		client.vectorIngest();
		Assert.assertEquals(
				2701,
				client.numFeaturesProcessed);

		ArrayList<Feature> features = client.vectorQuery();
		Assert.assertEquals(
				36,
				features.size());

		features = client.cqlQuery();
		Assert.assertEquals(
				36,
				features.size());

		features = client.spatialQuery();
		Assert.assertEquals(
				36,
				features.size());

		// This test doesn't actually use time as part of the query but we just
		// want to make sure grpc gets data back
		// it does use CONTAINS as part of query though so features on any
		// geometry borders will be discarded
		features = client.spatialTemporalQuery();
		Assert.assertEquals(
				16,
				features.size());

		// Core Cli Tests
		client.setCommand(
				"TEST_KEY",
				"TEST_VAL");
		map = client.listCommand();
		Assert.assertEquals(
				"TEST_VAL",
				map.get("TEST_KEY"));

		// Analytic Mapreduce Tests
		Assert.assertTrue(client.nearestNeighborCommand());
		Assert.assertTrue(client.kdeCommand());
		Assert.assertTrue(client.dbScanCommand());

		// Analytic Spark Tests
		Assert.assertTrue(client.KmeansSparkCommand());
		Assert.assertTrue(client.SparkSqlCommand());
		Assert.assertTrue(client.SpatialJoinCommand());

		// Core store Tests
		Assert.assertTrue(client.VersionCommand());

		result = client.ListAdapterCommand();
		Assert.assertTrue(result.contains(GeoWaveGrpcTestUtils.adapterId));

		result = client.ListIndexCommand();
		Assert.assertTrue(result.contains("SPATIAL_IDX_ROUND_ROBIN_32"));

		result = client.ListStatsCommand();
		Assert.assertTrue(result.contains("minX=-180.0, maxX=180.0"));

		result = client.AddIndexGroupCommand();
		Assert.assertTrue(result.contains("indexgroup." + GeoWaveGrpcTestUtils.indexId + "-group.opts."
				+ GeoWaveGrpcTestUtils.indexId + ".numPartitions=32"));

		Assert.assertTrue(client.CalculateStatCommand());
		Assert.assertTrue(client.RecalculateStatsCommand());

		Assert.assertTrue(client.RemoveStatCommand());

		Assert.assertTrue(client.ClearCommand());

		result = client.RemoveIndexGroupCommand();
		Assert.assertEquals(
				"indexgroup." + GeoWaveGrpcTestUtils.indexId + "-group successfully removed",
				result);

		result = client.RemoveIndexCommand();
		Assert.assertEquals(
				"index." + GeoWaveGrpcTestUtils.indexId + " successfully removed",
				result);

		Assert.assertTrue(client.RemoveAdapterCommand());

		result = client.RemoveStoreCommand();
		Assert.assertEquals(
				"store." + GeoWaveGrpcTestUtils.storeName + " successfully removed",
				result);

		TestUtils.deleteAll(dataStore);
	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}

	static protected void init()
			throws Exception {
		ZipUtils.unZipFile(
				new File(
						BasicKafkaIT.class.getClassLoader().getResource(
								TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
				TestUtils.TEST_CASE_BASE);

		// KafkaTestUtils.testKafkaStage(OSM_GPX_INPUT_DIR);
		// MapReduceTestUtils.testMapReduceStage(
		// OSM_GPX_INPUT_DIR);

		// set up the config file for the services
		configFile = File.createTempFile(
				"test_config",
				null);
		GeoWaveGrpcServiceOptions.geowaveConfigFile = configFile;

		operationParams = new ManualOperationParams();
		operationParams.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		// add a store and index manually before we try to ingest
		// this accurately simulates how the services will perform the ingest
		// from config file parameters (as opposed to programatic
		// creation/loading)
		final AddStoreCommand command = new AddStoreCommand();
		command.setParameters(GeoWaveGrpcTestUtils.storeName);
		command.setMakeDefault(true);
		command.setStoreType("hbase");
		command.prepare(operationParams);

		final DataStorePluginOptions options = command.getPluginOptions();
		final HBaseRequiredOptions opts = (HBaseRequiredOptions) options.getFactoryOptions();
		opts.setGeowaveNamespace("geowave.grpc");
		opts.setZookeeper("127.0.0.1:2181");
		command.execute(operationParams);

		final AddIndexCommand indexCommand = new AddIndexCommand();
		indexCommand.setType("spatial");
		indexCommand.setParameters(GeoWaveGrpcTestUtils.indexId);
		BasicIndexOptions basicIndexOpts = new BasicIndexOptions();
		basicIndexOpts.setNumPartitions(32);
		basicIndexOpts.setPartitionStrategy(PartitionStrategy.ROUND_ROBIN);
		indexCommand.getPluginOptions().setBasicIndexOptions(
				basicIndexOpts);
		indexCommand.prepare(operationParams);
		indexCommand.execute(operationParams);

		// finally add an output store for things like KDE etc
		final AddStoreCommand commandOut = new AddStoreCommand();
		commandOut.setParameters(GeoWaveGrpcTestUtils.outputStoreName);
		commandOut.setMakeDefault(true);
		commandOut.setStoreType("hbase");
		commandOut.prepare(operationParams);

		final DataStorePluginOptions optionsOut = commandOut.getPluginOptions();
		final HBaseRequiredOptions optsOut = (HBaseRequiredOptions) optionsOut.getFactoryOptions();
		optsOut.setGeowaveNamespace("geowave.grpc_out");
		optsOut.setZookeeper("127.0.0.1:2181");
		commandOut.execute(operationParams);

		// set up s3
		final ConfigAWSCommand configS3 = new ConfigAWSCommand();
		configS3.setS3UrlParameter("s3.amazonaws.com");
		configS3.execute(operationParams);

		// setup and start gRPC test server
		server = new GeoWaveGrpcTestServer(
				GeoWaveGrpcServiceOptions.port);
		server.start();

		// fire up the client
		client = new GeoWaveGrpcTestClient(
				GeoWaveGrpcServiceOptions.host,
				GeoWaveGrpcServiceOptions.port);
	}

	static protected void shutdown() {
		try {
			client.shutdown();
			server.stop();
		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered.",
					e);
		}
	}
}