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
import java.util.Properties;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.ingest.avro.GeoWaveAvroFormatPlugin;
import org.locationtech.geowave.core.ingest.hdfs.StageToHdfsDriver;
import org.locationtech.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import org.locationtech.geowave.core.store.ingest.LocalInputCommandLineOptions;
import org.locationtech.geowave.mapreduce.operations.ConfigHDFSCommand;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "localToHdfs", parentOperation = IngestSection.class)
@Parameters(commandDescription = "Stage supported files in local file system to HDFS")
public class LocalToHdfsCommand extends ServiceEnabledCommand<Void> {

  @Parameter(description = "<file or directory> <path to base directory to write to>")
  private List<String> parameters = new ArrayList<>();

  // This helper is used to load the list of format SPI plugins that will be
  // used
  @ParametersDelegate
  private IngestFormatPluginOptions pluginFormats = new IngestFormatPluginOptions();

  @ParametersDelegate
  private LocalInputCommandLineOptions localInputOptions = new LocalInputCommandLineOptions();

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

    computeResults(params);
  }

  @Override
  public boolean runAsync() {
    return true;
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String fileOrDirectory, final String hdfsPath) {
    parameters = new ArrayList<>();
    parameters.add(fileOrDirectory);
    parameters.add(hdfsPath);
  }

  public IngestFormatPluginOptions getPluginFormats() {
    return pluginFormats;
  }

  public void setPluginFormats(final IngestFormatPluginOptions pluginFormats) {
    this.pluginFormats = pluginFormats;
  }

  public LocalInputCommandLineOptions getLocalInputOptions() {
    return localInputOptions;
  }

  public void setLocalInputOptions(final LocalInputCommandLineOptions localInputOptions) {
    this.localInputOptions = localInputOptions;
  }

  @Override
  public Void computeResults(final OperationParams params) throws Exception {
    // Ensure we have all the required arguments
    if (parameters.size() != 2) {
      throw new ParameterException(
          "Requires arguments: <file or directory> <path to base directory to write to>");
    }
    // Config file
    final File configFile = getGeoWaveConfigFile(params);
    final Properties configProperties = ConfigOptions.loadProperties(configFile);
    final String hdfsHostPort = ConfigHDFSCommand.getHdfsUrl(configProperties);
    final String inputPath = parameters.get(0);
    final String basePath = parameters.get(1);

    // Ingest Plugins
    final Map<String, GeoWaveAvroFormatPlugin<?, ?>> ingestPlugins =
        pluginFormats.createAvroPlugins();

    // Driver
    final StageToHdfsDriver driver =
        new StageToHdfsDriver(ingestPlugins, hdfsHostPort, basePath, localInputOptions);

    // Execute
    if (!driver.runOperation(inputPath, configFile)) {
      throw new RuntimeException("Ingest failed to execute");
    }
    return null;
  }
}
