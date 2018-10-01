/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.test.services.grpc;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.raster.util.ZipUtils;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.core.ingest.operations.ConfigAWSCommand;
import org.locationtech.geowave.core.store.cli.config.AddIndexCommand;
import org.locationtech.geowave.core.store.cli.config.AddStoreCommand;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.IndexPluginOptions.PartitionStrategy;
import org.locationtech.geowave.core.store.operations.remote.options.BasicIndexOptions;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServer;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceOptions;
import org.locationtech.geowave.service.grpc.cli.StartGrpcServerCommand;
import org.locationtech.geowave.service.grpc.cli.StartGrpcServerCommandOptions;
import org.locationtech.geowave.service.grpc.cli.StopGrpcServerCommand;
import org.locationtech.geowave.service.grpc.protobuf.Feature;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveBasicVectorIT;
import org.locationtech.geowave.test.kafka.BasicKafkaIT;
import org.locationtech.geowave.test.services.grpc.GeoWaveGrpcTestClient;
import org.locationtech.geowave.test.services.grpc.GeoWaveGrpcTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.MAP_REDUCE,
	Environment.KAFKA,
})
@GeoWaveTestStore(value = {
	GeoWaveStoreType.ACCUMULO,
	GeoWaveStoreType.BIGTABLE,
	GeoWaveStoreType.CASSANDRA,
	GeoWaveStoreType.DYNAMODB,
	GeoWaveStoreType.HBASE
})
public class GeoWaveGrpcIT extends
		AbstractGeoWaveBasicVectorIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcIT.class);
	private static File configFile = null;
	private static GeoWaveGrpcTestClient client = null;

	protected DataStorePluginOptions dataStore;
	public static ManualOperationParams operationParams = null;
	private static long startMillis;
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

		if (configFile != null && configFile.exists()) {
			configFile.delete();
		}
	}

	@Test
	public void testGrpcServices()
			throws Exception {
		init();
		testGrpcServices(NUM_THREADS);
	}

	public void testGrpcServices(
			final int nthreads )
			throws InterruptedException,
			UnsupportedEncodingException,
			ParseException {

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
		org.apache.log4j.Logger.getRootLogger().setLevel(
				Level.WARN);

		// Core Ingest Tests
		Assert.assertTrue(client.LocalToHdfsCommand());
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED LocalToHdfsCommand  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

		Assert.assertTrue(client.LocalToGeowaveCommand());
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED LocalToGeowaveCommand  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

		Assert.assertTrue(client.LocalToKafkaCommand());
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED LocalToKafkaCommand  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

		Assert.assertTrue(client.KafkaToGeowaveCommand());
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED KafkaToGeowaveCommand  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

		Assert.assertTrue(client.MapReduceToGeowaveCommand());
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED MapReduceToGeowaveCommand  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

		String plugins = client.ListPluginsCommand();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED ListPluginsCommand  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

		Assert.assertTrue(
				"several plugins expected",
				countLines(plugins) > 10);
		Assert.assertTrue(client.LocalToMapReduceToGeowaveCommand());
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED LocalToMapReduceToGeowaveCommand  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

		Assert.assertTrue(client.SparkToGeowaveCommand());
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED SparkToGeowaveCommand  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

		// Vector Service Tests
		client.vectorIngest(
				-90,
				90,
				-180,
				180,
				5,
				5);

		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED vectorIngest  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

		Assert.assertNotEquals(
				0,
				client.numFeaturesProcessed);

		ArrayList<Feature> features = client.vectorQuery();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED vectorQuery  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

		Assert.assertNotEquals(
				0,
				features.size());

		features.clear();
		features = client.cqlQuery();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED cqlQuery  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

		Assert.assertNotEquals(
				0,
				features.size());

		features.clear();
		features = client.spatialQuery();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED spatialQuery  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

		Assert.assertNotEquals(
				0,
				features.size());

		// This test doesn't actually use time as part of the query but we just
		// want to make sure grpc gets data back
		// it does use CONTAINS as part of query though so features on any
		// geometry borders will be discarded
		features.clear();
		features = client.spatialTemporalQuery();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED spatialTemporalQuery  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

		Assert.assertNotEquals(
				0,
				features.size());

		// Core Cli Tests
		client.setCommand(
				"TEST_KEY",
				"TEST_VAL");
		map = client.listCommand();
		Assert.assertEquals(
				"TEST_VAL",
				map.get("TEST_KEY"));
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED core cli tests  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

		// clear out the stores and ingest a smaller sample
		// set for the more demanding operations
		TestUtils.deleteAll(dataStore);
		client.vectorIngest(
				0,
				10,
				0,
				10,
				5,
				5);

		// Analytic Mapreduce Tests
		Assert.assertTrue(client.nearestNeighborCommand());
		Assert.assertTrue(client.kdeCommand());
		Assert.assertTrue(client.dbScanCommand());
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED analytic mapreduce tests  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

		// Analytic Spark Tests
		Assert.assertTrue(client.KmeansSparkCommand());
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED spark kmeans *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

		Assert.assertTrue(client.SparkSqlCommand());
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED spark sql *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

		Assert.assertTrue(client.SpatialJoinCommand());
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED spatial join  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

		// Core store Tests
		Assert.assertTrue(client.VersionCommand());

		result = client.ListAdapterCommand();
		Assert.assertTrue(result.contains(GeoWaveGrpcTestUtils.adapterId));

		result = client.ListIndexCommand();
		Assert.assertTrue(result.contains("SPATIAL_IDX"));

		result = client.ListStatsCommand();
		Assert.assertTrue(!result.equalsIgnoreCase(""));

		result = client.AddIndexGroupCommand();
		Assert.assertTrue(result.contains("indexgroup." + GeoWaveGrpcTestUtils.indexId + "-group.opts."
				+ GeoWaveGrpcTestUtils.indexId + ".numPartitions=1"));

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
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED core store tests *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

		TestUtils.deleteAll(dataStore);
	}

	private static int countLines(
			String str ) {
		String[] lines = str.split("\r\n|\r|\n");
		return lines.length;
	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}

	protected void init()
			throws Exception {
		org.apache.log4j.Logger.getRootLogger().setLevel(
				Level.WARN);
		ZipUtils.unZipFile(
				new File(
						BasicKafkaIT.class.getClassLoader().getResource(
								TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
				TestUtils.TEST_CASE_BASE);

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
		command.setPluginOptions(dataStore);
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
		commandOut.setPluginOptions(dataStore);
		commandOut.execute(operationParams);

		// set up s3
		final ConfigAWSCommand configS3 = new ConfigAWSCommand();
		configS3.setS3UrlParameter("s3.amazonaws.com");
		configS3.execute(operationParams);

		// mimic starting the server from command line
		StartGrpcServerCommand startCmd = new StartGrpcServerCommand();
		StartGrpcServerCommandOptions grpcCmdOpts = new StartGrpcServerCommandOptions();
		grpcCmdOpts.setPort(GeoWaveGrpcServiceOptions.port);
		grpcCmdOpts.setNonBlocking(true);
		startCmd.setCommandOptions(grpcCmdOpts);
		startCmd.execute(operationParams);

		// fire up the client
		client = new GeoWaveGrpcTestClient(
				GeoWaveGrpcServiceOptions.host,
				GeoWaveGrpcServiceOptions.port);

		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED Init  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	static protected void shutdown() {
		try {
			client.shutdown();

			// mimic terminating the server from cli
			StopGrpcServerCommand stopCmd = new StopGrpcServerCommand();
			stopCmd.execute(operationParams);
		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered.",
					e);
		}
	}
}
