/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.exceptions.DuplicateEntryException;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.store.StoreSection;
import org.locationtech.geowave.datastore.bigtable.config.BigTableOptions;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@Parameters(commandDescription = "Create a BigTable store within Geowave")
public class AddBigTableStoreCommand extends ServiceEnabledCommand<String> {
  /** A REST Operation for the AddStoreCommand where --type=bigtable */
  public static final String PROPERTIES_CONTEXT = "properties";

  // Default AddStore Options
  @Parameter(description = "<name>")
  private List<String> parameters = new ArrayList<>();

  @Parameter(
      names = {"-d", "--default"},
      description = "Make this the default store in all operations")
  private Boolean makeDefault;

  private DataStorePluginOptions pluginOptions = new DataStorePluginOptions();

  @ParametersDelegate
  private BigTableOptions opts;

  @Override
  public boolean prepare(final OperationParams params) {

    pluginOptions.selectPlugin("bigtable");
    pluginOptions.setFactoryOptions(opts);

    return true;
  }

  @Override
  public void execute(final OperationParams params) throws Exception {
    computeResults(params);
  }

  @Override
  public String computeResults(final OperationParams params) throws Exception {

    final File propFile = getGeoWaveConfigFile(params);

    final Properties existingProps = ConfigOptions.loadProperties(propFile);

    // Ensure that a name is chosen.
    if (parameters.size() != 1) {
      throw new ParameterException("Must specify store name");
    }

    // Make sure we're not already in the index.
    final DataStorePluginOptions existingOptions = new DataStorePluginOptions();
    if (existingOptions.load(existingProps, getNamespace())) {
      throw new DuplicateEntryException("That store already exists: " + getPluginName());
    }

    // Save the store options.
    pluginOptions.save(existingProps, getNamespace());

    // Make default?
    if (Boolean.TRUE.equals(makeDefault)) {
      existingProps.setProperty(DataStorePluginOptions.DEFAULT_PROPERTY_NAMESPACE, getPluginName());
    }

    // Write properties file
    ConfigOptions.writeProperties(propFile, existingProps, params.getConsole());

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

  @Override
  public String getId() {
    return StoreSection.class.getName() + ".add/bigtable";
  }

  @Override
  public String getPath() {
    return "v0/store/add/bigtable";
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
    return "bigtable";
  }

  public void setPluginOptions(final DataStorePluginOptions pluginOptions) {
    this.pluginOptions = pluginOptions;
  }
}
