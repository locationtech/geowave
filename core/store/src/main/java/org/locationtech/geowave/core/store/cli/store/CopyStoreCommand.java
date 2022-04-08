/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.store;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "copy", parentOperation = StoreSection.class)
@Parameters(commandDescription = "Copy all data from one data store to another existing data store")
public class CopyStoreCommand extends DefaultOperation implements Command {
  @Parameter(description = "<input store name> <output store name>")
  private List<String> parameters = new ArrayList<>();

  private DataStorePluginOptions inputStoreOptions = null;
  private DataStorePluginOptions outputStoreOptions = null;

  @Override
  public void execute(final OperationParams params) throws Exception {
    // Ensure we have all the required arguments
    if (parameters.size() != 2) {
      throw new ParameterException("Requires arguments: <input store name> <output store name>");
    }
    // Config file
    final File configFile = getGeoWaveConfigFile(params);

    final String inputStoreName = parameters.get(0);
    final String outputStoreName = parameters.get(1);
    // Attempt to load input store.
    inputStoreOptions = CLIUtils.loadStore(inputStoreName, configFile, params.getConsole());
    // Attempt to load output store.
    outputStoreOptions = CLIUtils.loadStore(outputStoreName, configFile, params.getConsole());
    inputStoreOptions.createDataStore().copyTo(outputStoreOptions.createDataStore());
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
}
