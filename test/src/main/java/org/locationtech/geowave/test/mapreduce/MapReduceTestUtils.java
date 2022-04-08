/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.mapreduce;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.locationtech.geowave.adapter.vector.export.VectorMRExportCommand;
import org.locationtech.geowave.adapter.vector.export.VectorMRExportOptions;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.core.ingest.operations.LocalToMapReduceToGeoWaveCommand;
import org.locationtech.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.cli.store.AddStoreCommand;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexPluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.mapreduce.operations.ConfigHDFSCommand;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class MapReduceTestUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(MapReduceTestUtils.class);
  public static final String TEST_EXPORT_DIRECTORY = "basicMapReduceIT-export";

  public static final String EXPECTED_RESULTS_KEY = "EXPECTED_RESULTS";
  public static final int MIN_INPUT_SPLITS = 3;
  public static final int MAX_INPUT_SPLITS = 5;

  public static void testMapReduceIngest(
      final DataStorePluginOptions dataStore,
      final DimensionalityType dimensionalityType,
      final String ingestFilePath) throws Exception {
    testMapReduceIngest(dataStore, dimensionalityType, "gpx", ingestFilePath);
  }

  public static void testMapReduceExport(final DataStorePluginOptions inputStorePluginOptions)
      throws Exception {
    testMapReduceExport(inputStorePluginOptions, TEST_EXPORT_DIRECTORY);
  }

  public static void testMapReduceExport(
      final DataStorePluginOptions inputStorePluginOptions,
      final String directory) throws Exception {
    final VectorMRExportCommand exportCommand = new VectorMRExportCommand();
    final VectorMRExportOptions options = exportCommand.getMrOptions();

    exportCommand.setStoreOptions(inputStorePluginOptions);

    final MapReduceTestEnvironment env = MapReduceTestEnvironment.getInstance();
    final String exportPath = env.getHdfsBaseDirectory() + "/" + directory;

    final File exportDir = new File(exportPath.replace("file:", ""));
    if (exportDir.exists()) {
      boolean deleted = false;
      int attempts = 5;
      while (!deleted && (attempts-- > 0)) {
        try {
          FileUtils.deleteDirectory(exportDir);
          deleted = true;
        } catch (final Exception e) {
          LOGGER.error("Export directory not deleted, trying again in 10s: " + e);
          Thread.sleep(10000);
        }
      }
    }
    exportCommand.setParameters(exportPath, null);
    options.setBatchSize(10000);
    options.setMinSplits(MapReduceTestUtils.MIN_INPUT_SPLITS);
    options.setMaxSplits(MapReduceTestUtils.MAX_INPUT_SPLITS);
    options.setResourceManagerHostPort(env.getJobtracker());

    final Configuration conf = MapReduceTestUtils.getConfiguration();
    MapReduceTestUtils.filterConfiguration(conf);
    final int res =
        ToolRunner.run(conf, exportCommand.createRunner(env.getOperationParams()), new String[] {});
    Assert.assertTrue("Export Vector Data map reduce job failed", res == 0);

    TestUtils.deleteAll(inputStorePluginOptions);
  }

  public static void testMapReduceExportAndReingest(
      final DataStorePluginOptions inputStorePluginOptions,
      final DataStorePluginOptions outputStorePluginOptions,
      final DimensionalityType dimensionalityType) throws Exception {
    testMapReduceExport(inputStorePluginOptions);
    MapReduceTestUtils.testMapReduceIngest(
        outputStorePluginOptions,
        dimensionalityType,
        "avro",
        TestUtils.TEMP_DIR
            + File.separator
            + MapReduceTestEnvironment.HDFS_BASE_DIRECTORY
            + File.separator
            + TEST_EXPORT_DIRECTORY);
  }

  public static void testMapReduceIngest(
      final DataStorePluginOptions dataStore,
      final DimensionalityType dimensionalityType,
      final String format,
      final String ingestFilePath) throws Exception {
    // ingest gpx data directly into GeoWave using the
    // ingest framework's main method and pre-defined commandline arguments
    LOGGER.warn("Ingesting '" + ingestFilePath + "' - this may take several minutes...");

    final Thread progressLogger = startProgressLogger();

    // Indexes
    final String[] indexTypes = dimensionalityType.getDimensionalityArg().split(",");
    final List<IndexPluginOptions> indexOptions = new ArrayList<>(indexTypes.length);
    for (final String indexType : indexTypes) {
      final IndexPluginOptions indexOption = new IndexPluginOptions();
      indexOption.selectPlugin(indexType);
      indexOptions.add(indexOption);
    }
    // Ingest Formats
    final MapReduceTestEnvironment env = MapReduceTestEnvironment.getInstance();
    final IngestFormatPluginOptions ingestFormatOptions = new IngestFormatPluginOptions();
    ingestFormatOptions.selectPlugin(format);

    // create temporary config file and use it for hdfs FS URL config

    final File configFile = File.createTempFile("test_mr", null);
    final ManualOperationParams operationParams = new ManualOperationParams();
    operationParams.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    final ConfigHDFSCommand configHdfs = new ConfigHDFSCommand();
    configHdfs.setHdfsUrlParameter(env.getHdfs());
    configHdfs.execute(operationParams);

    final LocalToMapReduceToGeoWaveCommand mrGw = new LocalToMapReduceToGeoWaveCommand();

    final AddStoreCommand addStore = new AddStoreCommand();
    addStore.setParameters("test-store");
    addStore.setPluginOptions(dataStore);
    addStore.execute(operationParams);

    final IndexStore indexStore = dataStore.createIndexStore();
    final DataStore geowaveDataStore = dataStore.createDataStore();

    final StringBuilder indexParam = new StringBuilder();
    for (int i = 0; i < indexOptions.size(); i++) {
      String indexName = "testIndex" + i;
      if (indexStore.getIndex(indexName) == null) {
        indexOptions.get(i).setName(indexName);
        geowaveDataStore.addIndex(indexOptions.get(i).createIndex(geowaveDataStore));
      }
      indexParam.append(indexName + ",");
    }

    mrGw.setPluginFormats(ingestFormatOptions);
    mrGw.setParameters(
        ingestFilePath,
        env.getHdfsBaseDirectory(),
        "test-store",
        indexParam.toString());
    mrGw.getMapReduceOptions().setJobTrackerHostPort(env.getJobtracker());

    mrGw.execute(operationParams);

    progressLogger.interrupt();
  }

  private static Thread startProgressLogger() {
    final Runnable r = new Runnable() {
      @Override
      public void run() {
        final long start = System.currentTimeMillis();
        try {
          while (true) {
            final long now = System.currentTimeMillis();
            LOGGER.warn("Ingest running, progress: " + ((now - start) / 1000) + "s.");
            Thread.sleep(60000);
          }
        } catch (final InterruptedException e) {
          // Do nothing; thread is designed to be interrupted when
          // ingest completes
        }
      }
    };

    final Thread t = new Thread(r);

    t.start();

    return t;
  }

  public static void filterConfiguration(final Configuration conf) {
    // final parameters, can't be overriden
    conf.unset("mapreduce.job.end-notification.max.retry.interval");
    conf.unset("mapreduce.job.end-notification.max.attempts");

    // deprecated parameters (added in by default since we used the
    // Configuration() constructor (everything is set))
    conf.unset("session.id");
    conf.unset("mapred.jar");
    conf.unset("fs.default.name");
    conf.unset("mapred.map.tasks.speculative.execution");
    conf.unset("mapred.reduce.tasks");
    conf.unset("mapred.reduce.tasks.speculative.execution");
    conf.unset("mapred.mapoutput.value.class");
    conf.unset("mapred.used.genericoptionsparser");
    conf.unset("mapreduce.map.class");
    conf.unset("mapred.job.name");
    conf.unset("mapreduce.inputformat.class");
    conf.unset("mapred.input.dir");
    conf.unset("mapreduce.outputformat.class");
    conf.unset("mapred.map.tasks");
    conf.unset("mapred.mapoutput.key.class");
    conf.unset("mapred.working.dir");
  }

  public static Configuration getConfiguration() {
    final Configuration conf = new Configuration();
    final MapReduceTestEnvironment env = MapReduceTestEnvironment.getInstance();
    conf.set("fs.defaultFS", env.getHdfs());
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("mapreduce.jobtracker.address", env.getJobtracker());

    filterConfiguration(conf);

    return conf;
  }
}
