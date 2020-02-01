/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
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
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.ingest.operations.ConfigAWSCommand;
import org.locationtech.geowave.core.store.cli.store.AddStoreCommand;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceOptions;
import org.locationtech.geowave.service.grpc.cli.StartGrpcServerCommand;
import org.locationtech.geowave.service.grpc.cli.StartGrpcServerCommandOptions;
import org.locationtech.geowave.service.grpc.cli.StopGrpcServerCommand;
import org.locationtech.geowave.service.grpc.protobuf.FeatureProtos;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveBasicVectorIT;
import org.locationtech.geowave.test.kafka.BasicKafkaIT;
import org.locationtech.geowave.test.spark.SparkTestEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
@Environments({Environment.MAP_REDUCE, Environment.KAFKA,})
@GeoWaveTestStore(
    value = {
        GeoWaveStoreType.ACCUMULO,
        GeoWaveStoreType.BIGTABLE,
        GeoWaveStoreType.CASSANDRA,
        GeoWaveStoreType.DYNAMODB,
        GeoWaveStoreType.HBASE,
        GeoWaveStoreType.KUDU,
        GeoWaveStoreType.REDIS,
        GeoWaveStoreType.ROCKSDB})
public class GeoWaveGrpcIT extends AbstractGeoWaveBasicVectorIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcIT.class);
  private static File configFile = null;
  private static GeoWaveGrpcTestClient client = null;

  protected DataStorePluginOptions dataStore;
  public static ManualOperationParams operationParams = null;
  private static long startMillis;
  private static final int NUM_THREADS = 1;

  protected static final String TEST_DATA_ZIP_RESOURCE_PATH =
      TestUtils.TEST_RESOURCE_PACKAGE + "mapreduce-testdata.zip";
  protected static final String OSM_GPX_INPUT_DIR = TestUtils.TEST_CASE_BASE + "osm_gpx_test_case/";

  @BeforeClass
  public static void reportTestStart() throws Exception {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  RUNNING GeoWaveGrpcIT  *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn(ConfigOptions.getDefaultPropertyFile().getName());
    try {
      SparkTestEnvironment.getInstance().tearDown();
    } catch (final Exception e) {
      LOGGER.warn("Unable to tear down default spark session", e);
    }
  }

  @AfterClass
  public static void reportTestFinish() {
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("* FINISHED GeoWaveGrpcIT  *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    if ((configFile != null) && configFile.exists()) {
      configFile.delete();
    }
  }

  @Test
  public void testGrpcServices() throws Exception {
    init();
    testGrpcServices(NUM_THREADS);
  }

  public void testGrpcServices(final int nthreads)
      throws InterruptedException, UnsupportedEncodingException, ParseException {

    LOGGER.debug("Testing DataStore Type: " + dataStore.getType());

    // Ensure empty datastore
    TestUtils.deleteAll(dataStore);

    // Create the index
    SpatialDimensionalityTypeProvider.SpatialIndexBuilder indexBuilder =
        new SpatialDimensionalityTypeProvider.SpatialIndexBuilder();
    indexBuilder.setName(GeoWaveGrpcTestUtils.indexName);

    IndexStore indexStore = dataStore.createIndexStore();
    indexStore.addIndex(indexBuilder.createIndex());


    // variables for storing results and test returns
    String result = "";
    Map<String, String> map = null;

    // Core Mapreduce Tests
    client.configHDFSCommand();
    map = client.listCommand();
    Assert.assertEquals(
        GeoWaveGrpcTestUtils.getMapReduceTestEnv().getHdfs(),
        map.get("hdfs.defaultFS.url"));
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.WARN);

    // Core Ingest Tests
    Assert.assertTrue(client.LocalToHdfsCommand());
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  FINISHED LocalToHdfsCommand          *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    Assert.assertTrue(client.LocalToGeoWaveCommand());
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  FINISHED LocalToGeoWaveCommand       *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    Assert.assertTrue(client.LocalToKafkaCommand());
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  FINISHED LocalToKafkaCommand         *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    Assert.assertTrue(client.KafkaToGeoWaveCommand());
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  FINISHED KafkaToGeoWaveCommand       *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    Assert.assertTrue(client.MapReduceToGeoWaveCommand());
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  FINISHED MapReduceToGeoWaveCommand   *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    String plugins = client.ListIngestPluginsCommand();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  FINISHED ListIngestPluginsCommand    *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    plugins = client.ListIndexPluginsCommand();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  FINISHED ListIndexPluginsCommand     *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    plugins = client.ListStorePluginsCommand();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  FINISHED ListStorePluginsCommand     *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    Assert.assertTrue("several plugins expected", countLines(plugins) > 10);
    Assert.assertTrue(client.LocalToMapReduceToGeoWaveCommand());
    LOGGER.warn("-----------------------------------------------");
    LOGGER.warn("*                                             *");
    LOGGER.warn("*  FINISHED LocalToMapReduceToGeoWaveCommand  *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                       *");
    LOGGER.warn("*                                             *");
    LOGGER.warn("-----------------------------------------------");

    Assert.assertTrue(client.SparkToGeoWaveCommand());
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  FINISHED SparkToGeoWaveCommand       *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    // Vector Service Tests
    client.vectorIngest(0, 10, 0, 10, 5, 5);

    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  FINISHED vectorIngest                *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    Assert.assertNotEquals(0, client.numFeaturesProcessed);

    ArrayList<FeatureProtos> features = client.vectorQuery();
    Assert.assertTrue(features.size() > 0);
    features.clear();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  FINISHED vectorQuery                 *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    features = client.cqlQuery();
    Assert.assertTrue(features.size() > 0);
    features.clear();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  FINISHED cqlQuery                    *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    features = client.spatialQuery();
    Assert.assertTrue(features.size() > 0);
    features.clear();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  FINISHED spatialQuery                *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    // This test doesn't actually use time as part of the query but we just
    // want to make sure grpc gets data back
    // it does use CONTAINS as part of query though so features on any
    // geometry borders will be discarded
    features = client.spatialTemporalQuery();
    Assert.assertTrue(features.size() > 0);
    features.clear();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  FINISHED spatialTemporalQuery        *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    // Core Cli Tests
    client.setCommand("TEST_KEY", "TEST_VAL");
    map = client.listCommand();
    Assert.assertEquals("TEST_VAL", map.get("TEST_KEY"));
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  FINISHED core cli tests              *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    // clear out the stores and ingest a smaller sample
    // set for the more demanding operations
    TestUtils.deleteAll(dataStore);

    // Add the index again
    // Index store could be invalid after running deleteAll, so recreate it
    indexStore = dataStore.createIndexStore();
    indexStore.addIndex(indexBuilder.createIndex());

    client.vectorIngest(0, 10, 0, 10, 5, 5);

    // Analytic Mapreduce Tests
    Assert.assertTrue(client.nearestNeighborCommand());
    Assert.assertTrue(client.kdeCommand());
    Assert.assertTrue(client.dbScanCommand());
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  FINISHED analytic mapreduce tests    *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    // Analytic Spark Tests
    Assert.assertTrue(client.KmeansSparkCommand());
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  FINISHED spark kmeans                *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    Assert.assertTrue(client.SparkSqlCommand());
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  FINISHED spark sql                   *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    Assert.assertTrue(client.SpatialJoinCommand());
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  FINISHED spatial join                *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    // Core store Tests
    Assert.assertTrue(client.VersionCommand());

    result = client.ListAdapterCommand();
    Assert.assertTrue(result.contains(GeoWaveGrpcTestUtils.typeName));

    result = client.ListIndexCommand();
    Assert.assertTrue(result.contains("SPATIAL_IDX"));

    result = client.ListStatsCommand();
    Assert.assertTrue(!result.equalsIgnoreCase(""));

    Assert.assertTrue(client.CalculateStatCommand());
    Assert.assertTrue(client.RecalculateStatsCommand());

    Assert.assertTrue(client.RemoveStatCommand());

    Assert.assertTrue(client.ClearCommand());

    // Re-add the index
    // Index store could be invalid after running deleteAll, so recreate it
    indexStore = dataStore.createIndexStore();
    indexStore.addIndex(indexBuilder.createIndex());

    result = client.RemoveIndexCommand();
    Assert.assertEquals(
        "index." + GeoWaveGrpcTestUtils.indexName + " successfully removed",
        result);

    Assert.assertTrue(client.RemoveAdapterCommand());

    result = client.RemoveStoreCommand();
    Assert.assertEquals(
        "store." + GeoWaveGrpcTestUtils.storeName + " successfully removed",
        result);
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*  FINISHED core store tests            *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");

    TestUtils.deleteAll(dataStore);
  }

  private static int countLines(final String str) {
    final String[] lines = str.split("\r\n|\r|\n");
    return lines.length;
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStore;
  }

  protected void init() throws Exception {
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.WARN);
    ZipUtils.unZipFile(
        new File(
            BasicKafkaIT.class.getClassLoader().getResource(TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
        TestUtils.TEST_CASE_BASE);

    // set up the config file for the services
    configFile = File.createTempFile("test_config", null);
    GeoWaveGrpcServiceOptions.geowaveConfigFile = configFile;

    operationParams = new ManualOperationParams();
    operationParams.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    // add a store and index manually before we try to ingest
    // this accurately simulates how the services will perform the ingest
    // from config file parameters (as opposed to programatic
    // creation/loading)
    final AddStoreCommand command = new AddStoreCommand();
    command.setParameters(GeoWaveGrpcTestUtils.storeName);
    command.setPluginOptions(dataStore);
    command.execute(operationParams);

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
    final StartGrpcServerCommand startCmd = new StartGrpcServerCommand();
    final StartGrpcServerCommandOptions grpcCmdOpts = new StartGrpcServerCommandOptions();
    grpcCmdOpts.setPort(GeoWaveGrpcServiceOptions.port);
    grpcCmdOpts.setNonBlocking(true);
    startCmd.setCommandOptions(grpcCmdOpts);
    startCmd.execute(operationParams);

    // fire up the client
    client =
        new GeoWaveGrpcTestClient(GeoWaveGrpcServiceOptions.host, GeoWaveGrpcServiceOptions.port);

    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("* FINISHED Init  *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  protected static void shutdown() {
    try {
      client.shutdown();

      // mimic terminating the server from cli
      final StopGrpcServerCommand stopCmd = new StopGrpcServerCommand();
      stopCmd.execute(operationParams);
    } catch (final Exception e) {
      LOGGER.error("Exception encountered.", e);
    }
  }
}
