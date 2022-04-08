/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.hdfs.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.VisibilityOptions;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.ingest.DataAdapterProvider;
import org.locationtech.geowave.core.store.ingest.IngestUtils;
import org.locationtech.geowave.mapreduce.GeoWaveConfiguratorBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This class actually executes the ingestion of intermediate data into GeoWave that had been staged
 * in HDFS.
 */
public class IngestFromHdfsDriver {
  private static final Logger LOGGER = LoggerFactory.getLogger(IngestFromHdfsDriver.class);
  private static final int NUM_CONCURRENT_JOBS = 5;
  private static final int DAYS_TO_AWAIT_COMPLETION = 999;
  protected final DataStorePluginOptions storeOptions;
  protected final List<Index> indices;
  protected final VisibilityOptions ingestOptions;
  private final MapReduceCommandLineOptions mapReduceOptions;
  private final Map<String, IngestFromHdfsPlugin<?, ?>> ingestPlugins;
  private final String hdfsHostPort;
  private final String basePath;

  private static ExecutorService singletonExecutor;

  public IngestFromHdfsDriver(
      final DataStorePluginOptions storeOptions,
      final List<Index> indices,
      final VisibilityOptions ingestOptions,
      final MapReduceCommandLineOptions mapReduceOptions,
      final Map<String, IngestFromHdfsPlugin<?, ?>> ingestPlugins,
      final String hdfsHostPort,
      final String basePath) {
    this.storeOptions = storeOptions;
    this.indices = indices;
    this.ingestOptions = ingestOptions;
    this.mapReduceOptions = mapReduceOptions;
    this.ingestPlugins = ingestPlugins;
    this.hdfsHostPort = hdfsHostPort;
    this.basePath = basePath;
  }

  private static synchronized ExecutorService getSingletonExecutorService() {
    if ((singletonExecutor == null) || singletonExecutor.isShutdown()) {
      singletonExecutor = Executors.newFixedThreadPool(NUM_CONCURRENT_JOBS);
    }
    return singletonExecutor;
  }

  private boolean checkIndexesAgainstProvider(
      final String providerName,
      final DataAdapterProvider<?> adapterProvider) {
    boolean valid = true;
    for (final Index index : indices) {
      if (!IngestUtils.isCompatible(adapterProvider, index)) {
        // HP Fortify "Log Forging" false positive
        // What Fortify considers "user input" comes only
        // from users with OS-level access anyway
        LOGGER.warn(
            "HDFS file ingest plugin for ingest type '"
                + providerName
                + "' is not supported by index '"
                + index.getName()
                + "'");
        valid = false;
      }
    }
    return valid;
  }

  public boolean runOperation() {

    final Path hdfsBaseDirectory = new Path(basePath);
    try {
      final Configuration conf = new Configuration();
      GeoWaveConfiguratorBase.setRemoteInvocationParams(
          hdfsHostPort,
          mapReduceOptions.getJobTrackerOrResourceManagerHostPort(),
          conf);
      mapReduceOptions.applyConfigurationProperties(conf);
      try (FileSystem fs = FileSystem.get(conf)) {
        if (!fs.exists(hdfsBaseDirectory)) {
          LOGGER.error("HDFS base directory {} does not exist", hdfsBaseDirectory);
          return false;
        }
        for (final Entry<String, IngestFromHdfsPlugin<?, ?>> pluginProvider : ingestPlugins.entrySet()) {
          // if an appropriate sequence file does not exist, continue

          // TODO: we should probably clean up the type name to make
          // it HDFS path safe in case there are invalid characters
          final Path inputFile = new Path(hdfsBaseDirectory, pluginProvider.getKey());
          if (!fs.exists(inputFile)) {
            LOGGER.warn(
                "HDFS file '"
                    + inputFile
                    + "' does not exist for ingest type '"
                    + pluginProvider.getKey()
                    + "'");
            continue;
          }

          final IngestFromHdfsPlugin<?, ?> ingestFromHdfsPlugin = pluginProvider.getValue();
          IngestWithReducer ingestWithReducer = null;
          IngestWithMapper ingestWithMapper = null;

          // first find one preferred method of ingest from HDFS
          // (exclusively setting one or the other instance above)
          if (ingestFromHdfsPlugin.isUseReducerPreferred()) {
            ingestWithReducer = ingestFromHdfsPlugin.ingestWithReducer();
            if (ingestWithReducer == null) {
              LOGGER.warn(
                  "Plugin provider '"
                      + pluginProvider.getKey()
                      + "' prefers ingest with reducer but it is unimplemented");
            }
          }
          if (ingestWithReducer == null) {
            // check for ingest with mapper
            ingestWithMapper = ingestFromHdfsPlugin.ingestWithMapper();
            if ((ingestWithMapper == null) && !ingestFromHdfsPlugin.isUseReducerPreferred()) {

              ingestWithReducer = ingestFromHdfsPlugin.ingestWithReducer();
              if (ingestWithReducer == null) {
                LOGGER.warn(
                    "Plugin provider '"
                        + pluginProvider.getKey()
                        + "' does not does not support ingest from HDFS");
                continue;
              } else {
                LOGGER.warn(
                    "Plugin provider '"
                        + pluginProvider.getKey()
                        + "' prefers ingest with mapper but it is unimplemented");
              }
            }
          }

          AbstractMapReduceIngest jobRunner = null;
          if (ingestWithReducer != null) {
            if (!checkIndexesAgainstProvider(pluginProvider.getKey(), ingestWithReducer)) {
              continue;
            }
            jobRunner =
                new IngestWithReducerJobRunner(
                    storeOptions,
                    indices,
                    ingestOptions,
                    inputFile,
                    pluginProvider.getKey(),
                    ingestFromHdfsPlugin,
                    ingestWithReducer);

          } else if (ingestWithMapper != null) {
            if (!checkIndexesAgainstProvider(pluginProvider.getKey(), ingestWithMapper)) {
              continue;
            }
            jobRunner =
                new IngestWithMapperJobRunner(
                    storeOptions,
                    indices,
                    ingestOptions,
                    inputFile,
                    pluginProvider.getKey(),
                    ingestFromHdfsPlugin,
                    ingestWithMapper);
          }
          if (jobRunner != null) {
            try {
              runJob(conf, jobRunner);
            } catch (final Exception e) {
              LOGGER.warn("Error running ingest job", e);
              return false;
            }
          }
        }
      }
    } catch (final IOException e) {
      LOGGER.warn("Error in accessing HDFS file system", e);
      return false;
    } finally {

      final ExecutorService executorService = getSingletonExecutorService();
      executorService.shutdown();
      // do we want to just exit once our jobs are submitted or wait?
      // for now let's just wait a REALLY long time until all of the
      // submitted jobs complete
      try {
        executorService.awaitTermination(DAYS_TO_AWAIT_COMPLETION, TimeUnit.DAYS);
      } catch (final InterruptedException e) {
        LOGGER.error("Error waiting for submitted jobs to complete", e);
      }
    }
    // we really do not know if the service failed...bummer
    return true;
  }

  private void runJob(final Configuration conf, final AbstractMapReduceIngest jobRunner)
      throws Exception {
    final ExecutorService executorService = getSingletonExecutorService();
    executorService.execute(new Runnable() {

      @Override
      public void run() {
        try {
          final int res = ToolRunner.run(conf, jobRunner, new String[0]);
          if (res != 0) {
            LOGGER.error(
                "Mapper ingest job '"
                    + jobRunner.getJobName()
                    + "' exited with error code: "
                    + res);
          }
        } catch (final Exception e) {
          LOGGER.error("Error running mapper ingest job: " + jobRunner.getJobName(), e);
        }
      }
    });
  }
}
