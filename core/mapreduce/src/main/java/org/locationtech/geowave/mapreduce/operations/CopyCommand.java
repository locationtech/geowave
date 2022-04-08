/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.store.StoreSection;
import org.locationtech.geowave.mapreduce.copy.StoreCopyJobRunner;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "copymr", parentOperation = StoreSection.class)
@Parameters(
    commandDescription = "Copy all data from one data store to another existing data store using MapReduce")
public class CopyCommand extends DefaultOperation implements Command {
  @Parameter(description = "<input store name> <output store name>")
  private List<String> parameters = new ArrayList<>();

  @ParametersDelegate
  private CopyCommandOptions options = new CopyCommandOptions();

  private DataStorePluginOptions inputStoreOptions = null;
  private DataStorePluginOptions outputStoreOptions = null;

  @Override
  public void execute(final OperationParams params) throws Exception {
    createRunner(params).runJob();
  }

  public StoreCopyJobRunner createRunner(final OperationParams params) {
    // Ensure we have all the required arguments
    if (parameters.size() != 2) {
      throw new ParameterException("Requires arguments: <input store name> <output store name>");
    }

    final String inputStoreName = parameters.get(0);
    final String outputStoreName = parameters.get(1);

    // Config file
    final File configFile = getGeoWaveConfigFile(params);

    if (options.getHdfsHostPort() == null) {
      final Properties configProperties = ConfigOptions.loadProperties(configFile);
      final String hdfsFSUrl = ConfigHDFSCommand.getHdfsUrl(configProperties);
      options.setHdfsHostPort(hdfsFSUrl);
    }

    // Attempt to load input store.
    inputStoreOptions = CLIUtils.loadStore(inputStoreName, configFile, params.getConsole());

    // Attempt to load output store.
    outputStoreOptions = CLIUtils.loadStore(outputStoreName, configFile, params.getConsole());

    final String jobName = "Copy " + inputStoreName + " to " + outputStoreName;

    final StoreCopyJobRunner runner =
        new StoreCopyJobRunner(inputStoreOptions, outputStoreOptions, options, jobName);

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

  public DataStorePluginOptions getInputStoreOptions() {
    return inputStoreOptions;
  }

  public DataStorePluginOptions getOutputStoreOptions() {
    return outputStoreOptions;
  }

  public CopyCommandOptions getOptions() {
    return options;
  }

  public void setOptions(final CopyCommandOptions options) {
    this.options = options;
  }
}
