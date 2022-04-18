/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.type;

import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "list", parentOperation = TypeSection.class)
@Parameters(commandDescription = "Display all type names in a data store")
public class ListTypesCommand extends ServiceEnabledCommand<String> {
  @Parameter(description = "<store name>")
  private List<String> parameters = new ArrayList<>();

  private DataStorePluginOptions inputStoreOptions = null;

  @Override
  public void execute(final OperationParams params) {
    params.getConsole().println("Available types: " + computeResults(params));
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String storeName) {
    parameters = new ArrayList<>();
    parameters.add(storeName);
  }

  public DataStorePluginOptions getInputStoreOptions() {
    return inputStoreOptions;
  }

  @Override
  public String computeResults(final OperationParams params) {
    if (parameters.size() < 1) {
      throw new ParameterException("Must specify store name");
    }

    final String inputStoreName = parameters.get(0);

    // Attempt to load store.
    inputStoreOptions =
        CLIUtils.loadStore(inputStoreName, getGeoWaveConfigFile(params), params.getConsole());

    final String[] typeNames = inputStoreOptions.createInternalAdapterStore().getTypeNames();
    final StringBuffer buffer = new StringBuffer();
    for (final String typeName : typeNames) {
      buffer.append(typeName).append(' ');
    }

    return buffer.toString();
  }

}
