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
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "rm", parentOperation = TypeSection.class)
@Parameters(commandDescription = "Remove a data type and all associated data from a data store")
public class RemoveTypeCommand extends ServiceEnabledCommand<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoveTypeCommand.class);

  @Parameter(description = "<store name> <datatype name>")
  private List<String> parameters = new ArrayList<>();

  private DataStorePluginOptions inputStoreOptions = null;

  /** Return "200 OK" for all removal commands. */
  @Override
  public Boolean successStatusIs200() {
    return true;
  }

  @Override
  public void execute(final OperationParams params) {
    computeResults(params);
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String storeName, final String adapterId) {
    parameters = new ArrayList<>();
    parameters.add(storeName);
    parameters.add(adapterId);
  }

  public DataStorePluginOptions getInputStoreOptions() {
    return inputStoreOptions;
  }

  @Override
  public Void computeResults(final OperationParams params) {
    // Ensure we have all the required arguments
    if (parameters.size() != 2) {
      throw new ParameterException("Requires arguments: <store name> <type name>");
    }

    final String inputStoreName = parameters.get(0);
    final String typeName = parameters.get(1);

    // Attempt to load store.
    inputStoreOptions =
        CLIUtils.loadStore(inputStoreName, getGeoWaveConfigFile(params), params.getConsole());

    LOGGER.info("Deleting everything in store: " + inputStoreName + " with type name: " + typeName);
    inputStoreOptions.createDataStore().delete(
        QueryBuilder.newBuilder().addTypeName(typeName).build());
    return null;
  }
}
