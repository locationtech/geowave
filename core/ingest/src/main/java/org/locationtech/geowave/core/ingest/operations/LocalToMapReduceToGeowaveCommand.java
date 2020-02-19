/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.ingest.avro.GeoWaveAvroFormatPlugin;
import org.locationtech.geowave.core.ingest.hdfs.StageToHdfsDriver;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsDriver;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.MapReduceCommandLineOptions;
import org.locationtech.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.VisibilityOptions;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.store.StoreLoader;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.ingest.LocalInputCommandLineOptions;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.mapreduce.operations.ConfigHDFSCommand;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "localToMrGW", parentOperation = IngestSection.class)
@Parameters(
    commandDescription = "Copy supported files from local file system to HDFS and ingest from HDFS")
public class LocalToMapReduceToGeowaveCommand extends ServiceEnabledCommand<Void> {

  @Parameter(
      description = "<file or directory> <path to base directory to write to> <store name> <comma delimited index list>")
  private List<String> parameters = new ArrayList<>();

  @ParametersDelegate
  private VisibilityOptions ingestOptions = new VisibilityOptions();

  @ParametersDelegate
  private MapReduceCommandLineOptions mapReduceOptions = new MapReduceCommandLineOptions();

  @ParametersDelegate
  private LocalInputCommandLineOptions localInputOptions = new LocalInputCommandLineOptions();

  // This helper is used to load the list of format SPI plugins that will be
  // used
  @ParametersDelegate
  private IngestFormatPluginOptions pluginFormats = new IngestFormatPluginOptions();

  private DataStorePluginOptions inputStoreOptions = null;

  private List<Index> inputIndices = null;

  @Override
  public boolean prepare(final OperationParams params) {

    // Based on the selected formats, select the format plugins
    pluginFormats.selectPlugin(localInputOptions.getFormats());

    return true;
  }

  /**
   * Prep the driver & run the operation.
   *
   * @throws Exception
   */
  @Override
  public void execute(final OperationParams params) throws Exception {

    // Ensure we have all the required arguments
    if (parameters.size() != 4) {
      throw new ParameterException(
          "Requires arguments: <file or directory> <path to base directory to write to> <store name> <comma delimited index list>");
    }

    computeResults(params);
  }

  @Override
  public boolean runAsync() {
    return true;
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(
      final String fileOrDirectory,
      final String pathToBaseDirectory,
      final String storeName,
      final String indexList) {
    parameters = new ArrayList<>();
    parameters.add(fileOrDirectory);
    parameters.add(pathToBaseDirectory);
    parameters.add(storeName);
    parameters.add(indexList);
  }

  public DataStorePluginOptions getInputStoreOptions() {
    return inputStoreOptions;
  }

  public List<Index> getInputIndices() {
    return inputIndices;
  }

  public VisibilityOptions getIngestOptions() {
    return ingestOptions;
  }

  public void setIngestOptions(final VisibilityOptions ingestOptions) {
    this.ingestOptions = ingestOptions;
  }

  public MapReduceCommandLineOptions getMapReduceOptions() {
    return mapReduceOptions;
  }

  public void setMapReduceOptions(final MapReduceCommandLineOptions mapReduceOptions) {
    this.mapReduceOptions = mapReduceOptions;
  }

  public LocalInputCommandLineOptions getLocalInputOptions() {
    return localInputOptions;
  }

  public void setLocalInputOptions(final LocalInputCommandLineOptions localInputOptions) {
    this.localInputOptions = localInputOptions;
  }

  public IngestFormatPluginOptions getPluginFormats() {
    return pluginFormats;
  }

  public void setPluginFormats(final IngestFormatPluginOptions pluginFormats) {
    this.pluginFormats = pluginFormats;
  }

  @Override
  public Void computeResults(final OperationParams params) throws Exception {
    if (mapReduceOptions.getJobTrackerOrResourceManagerHostPort() == null) {
      throw new ParameterException(
          "Requires job tracker or resource manager option (try geowave help <command>...)");
    }

    final String inputPath = parameters.get(0);
    final String basePath = parameters.get(1);
    final String inputStoreName = parameters.get(2);
    final String indexList = parameters.get(3);

    // Config file
    final File configFile = getGeoWaveConfigFile(params);
    final Properties configProperties = ConfigOptions.loadProperties(configFile);
    final String hdfsHostPort = ConfigHDFSCommand.getHdfsUrl(configProperties);

    final StoreLoader inputStoreLoader = new StoreLoader(inputStoreName);
    if (!inputStoreLoader.loadFromConfig(configFile)) {
      throw new ParameterException("Cannot find store name: " + inputStoreLoader.getStoreName());
    }
    inputStoreOptions = inputStoreLoader.getDataStorePlugin();

    final IndexStore indexStore = inputStoreOptions.createIndexStore();
    inputIndices = DataStoreUtils.loadIndices(indexStore, indexList);

    // Ingest Plugins
    final Map<String, GeoWaveAvroFormatPlugin<?, ?>> avroIngestPlugins =
        pluginFormats.createAvroPlugins();

    // Ingest Plugins
    final Map<String, IngestFromHdfsPlugin<?, ?>> hdfsIngestPlugins =
        pluginFormats.createHdfsIngestPlugins();

    {

      // Driver
      final StageToHdfsDriver driver =
          new StageToHdfsDriver(avroIngestPlugins, hdfsHostPort, basePath, localInputOptions);

      // Execute
      if (!driver.runOperation(inputPath, configFile)) {
        throw new RuntimeException("Ingest failed to execute");
      }
    }

    {
      // Driver
      final IngestFromHdfsDriver driver =
          new IngestFromHdfsDriver(
              inputStoreOptions,
              inputIndices,
              ingestOptions,
              mapReduceOptions,
              hdfsIngestPlugins,
              hdfsHostPort,
              basePath);

      // Execute
      if (!driver.runOperation()) {
        throw new RuntimeException("Ingest failed to execute");
      }
    }
    return null;
  };
}
