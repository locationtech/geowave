/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.store;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "copycfg", parentOperation = StoreSection.class)
@Parameters(commandDescription = "Copy and modify local data store configuration")
public class CopyConfigStoreCommand extends DefaultOperation implements Command {

  @Parameter(description = "<store name> <new name>")
  private List<String> parameters = new ArrayList<>();

  @Parameter(
      names = {"-d", "--default"},
      description = "Make this the default store in all operations")
  private Boolean makeDefault;

  @ParametersDelegate
  private DataStorePluginOptions newPluginOptions = new DataStorePluginOptions();

  @Override
  public boolean prepare(final OperationParams params) {
    super.prepare(params);

    final Properties existingProps = getGeoWaveConfigProperties(params);

    // Load the old store, so that we can override the values
    String oldStore = null;
    if (parameters.size() >= 1) {
      oldStore = parameters.get(0);
      if (!newPluginOptions.load(
          existingProps,
          DataStorePluginOptions.getStoreNamespace(oldStore))) {
        throw new ParameterException("Could not find store: " + oldStore);
      }
    }

    // Successfully prepared.
    return true;
  }

  public void setNewPluginOptions(final DataStorePluginOptions newPluginOptions) {
    this.newPluginOptions = newPluginOptions;
  }

  @Override
  public void execute(final OperationParams params) {

    final Properties existingProps = getGeoWaveConfigProperties(params);

    if (parameters.size() < 2) {
      throw new ParameterException("Must specify <existing store> <new store> names");
    }

    // This is the new store name.
    final String newStore = parameters.get(1);
    final String newStoreNamespace = DataStorePluginOptions.getStoreNamespace(newStore);

    // Make sure we're not already in the index.
    final DataStorePluginOptions existPlugin = new DataStorePluginOptions();
    if (existPlugin.load(existingProps, newStoreNamespace)) {
      throw new ParameterException("That store already exists: " + newStore);
    }

    // Save the options.
    newPluginOptions.save(existingProps, newStoreNamespace);

    // Make default?
    if (Boolean.TRUE.equals(makeDefault)) {
      existingProps.setProperty(DataStorePluginOptions.DEFAULT_PROPERTY_NAMESPACE, newStore);
    }

    // Write properties file
    ConfigOptions.writeProperties(getGeoWaveConfigFile(params), existingProps, params.getConsole());
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String existingStore, final String newStore) {
    parameters = new ArrayList<>();
    parameters.add(existingStore);
    parameters.add(newStore);
  }
}
