/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.export;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.locationtech.geowave.adapter.vector.cli.VectorSection;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.mapreduce.operations.ConfigHDFSCommand;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "mrexport", parentOperation = VectorSection.class)
@Parameters(commandDescription = "Export data using MapReduce")
public class VectorMRExportCommand extends DefaultOperation implements Command {

  @Parameter(description = "<path to base directory to write to> <store name>")
  private List<String> parameters = new ArrayList<>();

  @ParametersDelegate
  private VectorMRExportOptions mrOptions = new VectorMRExportOptions();

  private DataStorePluginOptions storeOptions = null;

  @Override
  public void execute(final OperationParams params) throws Exception {
    createRunner(params).runJob();
  }

  public VectorMRExportJobRunner createRunner(final OperationParams params) {
    // Ensure we have all the required arguments
    if (parameters.size() != 2) {
      throw new ParameterException(
          "Requires arguments: <path to base directory to write to> <store name>");
    }

    final String hdfsPath = parameters.get(0);
    final String storeName = parameters.get(1);

    // Config file
    final File configFile = getGeoWaveConfigFile(params);
    final Properties configProperties = ConfigOptions.loadProperties(configFile);
    final String hdfsHostPort = ConfigHDFSCommand.getHdfsUrl(configProperties);

    // Attempt to load store.
    if (storeOptions == null) {
      storeOptions = CLIUtils.loadStore(storeName, configFile, params.getConsole());
    }

    final VectorMRExportJobRunner vectorRunner =
        new VectorMRExportJobRunner(
            storeOptions,
            mrOptions,
            hdfsHostPort,
            hdfsPath,
            params.getConsole());
    return vectorRunner;
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String hdfsPath, final String storeName) {
    parameters = new ArrayList<>();
    parameters.add(hdfsPath);
    parameters.add(storeName);
  }

  public VectorMRExportOptions getMrOptions() {
    return mrOptions;
  }

  public void setMrOptions(final VectorMRExportOptions mrOptions) {
    this.mrOptions = mrOptions;
  }

  public DataStorePluginOptions getStoreOptions() {
    return storeOptions;
  }

  public void setStoreOptions(final DataStorePluginOptions storeOptions) {
    this.storeOptions = storeOptions;
  }
}
