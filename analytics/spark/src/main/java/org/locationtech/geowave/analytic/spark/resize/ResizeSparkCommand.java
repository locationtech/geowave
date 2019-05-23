/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
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
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.StoreLoader;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "resize-mr", parentOperation = RasterSection.class)
@Parameters(commandDescription = "Resize Raster Tiles in MapReduce")
public class ResizeSparkCommand extends DefaultOperation implements Command {

  @Parameter(description = "<input store name> <output store name>")
  private List<String> parameters = new ArrayList<>();

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
    final StoreLoader inputStoreLoader = new StoreLoader(inputStoreName);
    if (!inputStoreLoader.loadFromConfig(configFile)) {
      throw new ParameterException("Cannot find store name: " + inputStoreLoader.getStoreName());
    }
    inputStoreOptions = inputStoreLoader.getDataStorePlugin();

    // Attempt to load output store.
    final StoreLoader outputStoreLoader = new StoreLoader(outputStoreName);
    if (!outputStoreLoader.loadFromConfig(configFile)) {
      throw new ParameterException("Cannot find store name: " + outputStoreLoader.getStoreName());
    }
    outputStoreOptions = outputStoreLoader.getDataStorePlugin();


    final RasterTileResizeSparkRunner runner =
        new RasterTileResizeSparkRunner(inputStoreOptions, outputStoreOptions, options);
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
}
