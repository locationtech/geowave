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
import org.locationtech.geowave.core.cli.api.DefaultPluginOptions;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "add", parentOperation = StoreSection.class)
@Parameters(commandDescription = "Add a data store to the GeoWave configuration")
public class AddStoreCommand extends ServiceEnabledCommand<String> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AddStoreCommand.class);

  public static final String PROPERTIES_CONTEXT = "properties";

  @Parameter(description = "<store name>")
  private List<String> parameters = new ArrayList<>();

  @Parameter(
      names = {"-d", "--default"},
      description = "Make this the default store in all operations")
  private Boolean makeDefault;

  @Parameter(
      names = {"-t", "--type"},
      required = true,
      description = "The type of store, such as accumulo, hbase, etc")
  private String storeType;

  private DataStorePluginOptions pluginOptions = new DataStorePluginOptions();

  @ParametersDelegate
  private StoreFactoryOptions requiredOptions;

  @Override
  public boolean prepare(final OperationParams params) {
    super.prepare(params);

    // Load SPI options for the given type into pluginOptions.
    if (storeType != null) {
      pluginOptions.selectPlugin(storeType);
      requiredOptions = pluginOptions.getFactoryOptions();
    } else {
      final Properties existingProps = getGeoWaveConfigProperties(params);

      // Try to load the 'default' options.
      final String defaultStore =
          existingProps.getProperty(DataStorePluginOptions.DEFAULT_PROPERTY_NAMESPACE);

      // Load the default index.
      if (defaultStore != null) {
        try {
          if (pluginOptions.load(
              existingProps,
              DataStorePluginOptions.getStoreNamespace(defaultStore))) {
            // Set the required type option.
            storeType = pluginOptions.getType();
            requiredOptions = pluginOptions.getFactoryOptions();
          }
        } catch (final ParameterException pe) {
          // HP Fortify "Improper Output Neutralization" false
          // positive
          // What Fortify considers "user input" comes only
          // from users with OS-level access anyway
          LOGGER.warn("Couldn't load default store: " + defaultStore, pe);
        }
      }
    }
    return true;
  }

  @Override
  public void execute(final OperationParams params) {
    computeResults(params);
  }

  @Override
  public String computeResults(final OperationParams params) {
    final Properties existingProps = getGeoWaveConfigProperties(params);

    // Ensure that a name is chosen.
    if (parameters.size() != 1) {
      throw new ParameterException("Must specify store name");
    }

    // Make sure we're not already in the index.
    final DataStorePluginOptions existingOptions = new DataStorePluginOptions();
    if (existingOptions.load(existingProps, getNamespace())) {
      throw new ParameterException("That store already exists: " + getPluginName());
    }

    if (pluginOptions.getFactoryOptions() != null) {
      pluginOptions.getFactoryOptions().validatePluginOptions(existingProps, params.getConsole());
    }

    // Save the store options.
    pluginOptions.save(existingProps, getNamespace());

    // Make default?
    if (Boolean.TRUE.equals(makeDefault)) {
      existingProps.setProperty(DataStorePluginOptions.DEFAULT_PROPERTY_NAMESPACE, getPluginName());
    }

    // Write properties file
    ConfigOptions.writeProperties(
        getGeoWaveConfigFile(),
        existingProps,
        pluginOptions.getFactoryOptions().getClass(),
        getNamespace() + "." + DefaultPluginOptions.OPTS,
        params.getConsole());

    final StringBuilder builder = new StringBuilder();
    for (final Object key : existingProps.keySet()) {
      final String[] split = key.toString().split("\\.");
      if (split.length > 1) {
        if (split[1].equals(parameters.get(0))) {
          builder.append(key.toString() + "=" + existingProps.getProperty(key.toString()) + "\n");
        }
      }
    }
    return builder.toString();
  }

  public DataStorePluginOptions getPluginOptions() {
    return pluginOptions;
  }

  public String getPluginName() {
    return parameters.get(0);
  }

  public String getNamespace() {
    return DataStorePluginOptions.getStoreNamespace(getPluginName());
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String storeName) {
    parameters = new ArrayList<>();
    parameters.add(storeName);
  }

  public Boolean getMakeDefault() {
    return makeDefault;
  }

  public void setMakeDefault(final Boolean makeDefault) {
    this.makeDefault = makeDefault;
  }

  public String getStoreType() {
    return storeType;
  }

  public void setStoreType(final String storeType) {
    this.storeType = storeType;
  }

  public void setPluginOptions(final DataStorePluginOptions pluginOptions) {
    this.pluginOptions = pluginOptions;
  }
}
