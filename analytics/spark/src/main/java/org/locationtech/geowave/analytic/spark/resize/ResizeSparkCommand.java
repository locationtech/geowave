/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.resize;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.adapter.raster.operations.RasterSection;
import org.locationtech.geowave.adapter.raster.operations.options.RasterTileResizeCommandLineOptions;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "resizespark", parentOperation = RasterSection.class)
@Parameters(commandDescription = "Resize raster tiles using Spark")
public class ResizeSparkCommand extends DefaultOperation implements Command {

  @Parameter(description = "<input store name> <output store name>")
  private List<String> parameters = new ArrayList<>();

  @Parameter(names = {"-n", "--name"}, description = "The spark application name")
  private String appName = "RasterResizeRunner";

  @Parameter(names = {"-ho", "--host"}, description = "The spark driver host")
  private String host = "localhost";

  @Parameter(names = {"-m", "--master"}, description = "The spark master designation")
  private String master = "yarn";

  @ParametersDelegate
  private RasterTileResizeCommandLineOptions options = new RasterTileResizeCommandLineOptions();

  private DataStorePluginOptions inputStoreOptions = null;

  private DataStorePluginOptions outputStoreOptions = null;

  @Override
  public void execute(final OperationParams params) throws Exception {
    createRunner(params).run();
  }

  public RasterTileResizeSparkRunner createRunner(final OperationParams params) {
    // Ensure we have all the required arguments
    if (parameters.size() != 2) {
      throw new ParameterException("Requires arguments: <input store name> <output store name>");
    }

    final String inputStoreName = parameters.get(0);
    final String outputStoreName = parameters.get(1);

    // Config file
    final File configFile = getGeoWaveConfigFile(params);

    // Attempt to load input store.
    inputStoreOptions = CLIUtils.loadStore(inputStoreName, configFile, params.getConsole());

    // Attempt to load output store.
    outputStoreOptions = CLIUtils.loadStore(outputStoreName, configFile, params.getConsole());


    final RasterTileResizeSparkRunner runner =
        new RasterTileResizeSparkRunner(inputStoreOptions, outputStoreOptions, options);
    runner.setHost(host);
    runner.setAppName(appName);
    runner.setMaster(master);
    return runner;
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String inputStore, final String outputStore) {
    parameters = new ArrayList<>();
    parameters.add(inputStore);
    parameters.add(outputStore);
  }

  public RasterTileResizeCommandLineOptions getOptions() {
    return options;
  }

  public void setOptions(final RasterTileResizeCommandLineOptions options) {
    this.options = options;
  }

  public DataStorePluginOptions getInputStoreOptions() {
    return inputStoreOptions;
  }

  public DataStorePluginOptions getOutputStoreOptions() {
    return outputStoreOptions;
  }

  public void setAppName(final String appName) {
    this.appName = appName;
  }

  public void setHost(final String host) {
    this.host = host;
  }

  public void setMaster(final String master) {
    this.master = master;
  }
}
