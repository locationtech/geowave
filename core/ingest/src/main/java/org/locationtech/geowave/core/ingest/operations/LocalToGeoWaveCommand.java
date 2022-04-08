/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
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
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.ingest.local.LocalFileIngestCLIDriver;
import org.locationtech.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.VisibilityOptions;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.locationtech.geowave.core.store.ingest.LocalInputCommandLineOptions;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "localToGW", parentOperation = IngestSection.class)
@Parameters(
    commandDescription = "Ingest supported files in local file system directly, from S3 or from HDFS")
public class LocalToGeoWaveCommand extends ServiceEnabledCommand<Void> {

  @Parameter(description = "<file or directory> <store name> <comma delimited index list>")
  private List<String> parameters = new ArrayList<>();

  @ParametersDelegate
  private VisibilityOptions visibilityOptions = new VisibilityOptions();

  @ParametersDelegate
  private LocalInputCommandLineOptions localInputOptions = new LocalInputCommandLineOptions();

  // This helper is used to load the list of format SPI plugins that will be
  // used
  @ParametersDelegate
  private IngestFormatPluginOptions pluginFormats = new IngestFormatPluginOptions();

  @Parameter(
      names = {"-t", "--threads"},
      description = "number of threads to use for ingest, default to 1 (optional)")
  private int threads = 1;

  private DataStorePluginOptions inputStoreOptions = null;

  private List<Index> inputIndices = null;

  @Override
  public boolean prepare(final OperationParams params) {

    // Based on the selected formats, select the format plugins
    pluginFormats.selectPlugin(localInputOptions.getFormats());

    return true;
  }

  /** Prep the driver & run the operation. */
  @Override
  public void execute(final OperationParams params) {
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
      final String storeName,
      final String commaDelimitedIndexes) {
    parameters = new ArrayList<>();
    parameters.add(fileOrDirectory);
    parameters.add(storeName);
    parameters.add(commaDelimitedIndexes);
  }

  public VisibilityOptions getVisibilityOptions() {
    return visibilityOptions;
  }

  public void setVisibilityOptions(final VisibilityOptions visibilityOptions) {
    this.visibilityOptions = visibilityOptions;
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

  public int getThreads() {
    return threads;
  }

  public void setThreads(final int threads) {
    this.threads = threads;
  }

  public DataStorePluginOptions getInputStoreOptions() {
    return inputStoreOptions;
  }

  public List<Index> getInputIndices() {
    return inputIndices;
  }

  @Override
  public Void computeResults(final OperationParams params) {
    // Ensure we have all the required arguments
    if (parameters.size() != 3) {
      throw new ParameterException(
          "Requires arguments: <file or directory> <storename> <comma delimited index list>");
    }

    final String inputPath = parameters.get(0);
    final String inputStoreName = parameters.get(1);
    final String indexList = parameters.get(2);

    // Config file
    final File configFile = getGeoWaveConfigFile(params);

    inputStoreOptions = CLIUtils.loadStore(inputStoreName, configFile, params.getConsole());

    final IndexStore indexStore = inputStoreOptions.createIndexStore();

    inputIndices = DataStoreUtils.loadIndices(indexStore, indexList);

    // Ingest Plugins
    final Map<String, LocalFileIngestPlugin<?>> ingestPlugins =
        pluginFormats.createLocalIngestPlugins();

    // Driver
    final LocalFileIngestCLIDriver driver =
        new LocalFileIngestCLIDriver(
            inputStoreOptions,
            inputIndices,
            ingestPlugins,
            visibilityOptions,
            localInputOptions,
            threads);

    // Execute
    if (!driver.runOperation(inputPath, configFile)) {
      throw new RuntimeException("Ingest failed to execute");
    }
    return null;
  }
}
