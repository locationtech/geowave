/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.operations;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.analytic.mapreduce.kde.KDECommandLineOptions;
import org.locationtech.geowave.analytic.mapreduce.kde.KDEJobRunner;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "kde", parentOperation = AnalyticSection.class)
@Parameters(commandDescription = "Kernel density estimate")
public class KdeCommand extends ServiceEnabledCommand<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(KdeCommand.class);

  @Parameter(description = "<input store name> <output store name>")
  private List<String> parameters = new ArrayList<>();

  @ParametersDelegate
  private KDECommandLineOptions kdeOptions = new KDECommandLineOptions();

  private DataStorePluginOptions inputStoreOptions = null;

  private DataStorePluginOptions outputStoreOptions = null;

  @Override
  public void execute(final OperationParams params) throws Exception {
    computeResults(params);
  }

  @Override
  public boolean runAsync() {
    return true;
  }

  public KDEJobRunner createRunner(final OperationParams params) throws IOException {
    // Ensure we have all the required arguments
    if (parameters.size() != 2) {
      throw new ParameterException("Requires arguments: <input store name> <output store name>");
    }

    final String inputStore = parameters.get(0);
    final String outputStore = parameters.get(1);
    // Config file
    final File configFile = getGeoWaveConfigFile(params);
    Index outputPrimaryIndex = null;

    // Attempt to load input store.
    inputStoreOptions = CLIUtils.loadStore(inputStore, configFile, params.getConsole());

    // Attempt to load output store.
    outputStoreOptions = CLIUtils.loadStore(outputStore, configFile, params.getConsole());

    if ((kdeOptions.getOutputIndex() != null) && !kdeOptions.getOutputIndex().trim().isEmpty()) {
      final String outputIndex = kdeOptions.getOutputIndex();

      // Load the Indices
      final List<Index> outputIndices =
          DataStoreUtils.loadIndices(outputStoreOptions.createIndexStore(), outputIndex);

      for (final Index primaryIndex : outputIndices) {
        if (SpatialDimensionalityTypeProvider.isSpatial(primaryIndex)) {
          outputPrimaryIndex = primaryIndex;
        } else {
          LOGGER.error(
              "spatial temporal is not supported for output index. Only spatial index is supported.");
          throw new IOException(
              "spatial temporal is not supported for output index. Only spatial index is supported.");
        }
      }
    }

    final KDEJobRunner runner =
        new KDEJobRunner(
            kdeOptions,
            inputStoreOptions,
            outputStoreOptions,
            configFile,
            outputPrimaryIndex);
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

  public KDECommandLineOptions getKdeOptions() {
    return kdeOptions;
  }

  public void setKdeOptions(final KDECommandLineOptions kdeOptions) {
    this.kdeOptions = kdeOptions;
  }

  public DataStorePluginOptions getInputStoreOptions() {
    return inputStoreOptions;
  }

  public DataStorePluginOptions getOutputStoreOptions() {
    return outputStoreOptions;
  }

  @Override
  public Void computeResults(final OperationParams params) throws Exception {
    final KDEJobRunner runner = createRunner(params);
    final int status = runner.runJob();
    if (status != 0) {
      throw new RuntimeException("Failed to execute: " + status);
    }
    return null;
  }
}
