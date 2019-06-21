/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.operations.config.ConfigSection;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.store.cli.remote.options.IndexPluginOptions;
import org.locationtech.geowave.core.store.operations.remote.options.BasicIndexOptions;
import org.locationtech.geowave.core.store.spi.DimensionalityTypeOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "addindex", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Configure an index for usage in GeoWave")
public class AddIndexCommand extends ServiceEnabledCommand<String> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddIndexCommand.class);

  @Parameter(description = "<name>", required = true)
  private List<String> parameters = new ArrayList<>();

  @Parameter(
      names = {"-d", "--default"},
      description = "Make this the default index creating stores")
  private Boolean makeDefault;

  @Parameter(
      names = {"-t", "--type"},
      required = true,
      description = "The type of index, such as spatial, or spatial_temporal")
  private String type;

  private IndexPluginOptions pluginOptions = new IndexPluginOptions();

  @ParametersDelegate
  private BasicIndexOptions basicIndexOptions = new BasicIndexOptions();

  @ParametersDelegate
  DimensionalityTypeOptions opts;

  @Override
  public boolean prepare(final OperationParams params) {
    super.prepare(params);

    // Load SPI options for the given type into pluginOptions.
    if (type != null) {
      pluginOptions.selectPlugin(type);
      pluginOptions.setBasicIndexOptions(basicIndexOptions);
      opts = pluginOptions.getDimensionalityOptions();
    } else {
      final Properties existingProps = getGeoWaveConfigProperties(params);

      final String defaultIndex =
          existingProps.getProperty(IndexPluginOptions.DEFAULT_PROPERTY_NAMESPACE);

      // Load the default index.
      if (defaultIndex != null) {
        try {
          if (pluginOptions.load(
              existingProps,
              IndexPluginOptions.getIndexNamespace(defaultIndex))) {
            // Set the required type option.
            type = pluginOptions.getType();
            opts = pluginOptions.getDimensionalityOptions();
          }
        } catch (final ParameterException pe) {
          // HP Fortify "Improper Output Neutralization" false
          // positive
          // What Fortify considers "user input" comes only
          // from users with OS-level access anyway
          LOGGER.warn("Couldn't load default index: " + defaultIndex, pe);
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

    // Ensure that a name is chosen.
    if (parameters.size() != 1) {
      throw new ParameterException("Must specify index name");
    }

    final Properties existingProps = getGeoWaveConfigProperties(params);

    // Make sure we're not already in the index.
    final IndexPluginOptions existPlugin = new IndexPluginOptions();
    if (existPlugin.load(existingProps, getNamespace())) {
      throw new ParameterException("That index already exists: " + getPluginName());
    }

    // Save the options.
    pluginOptions.save(existingProps, getNamespace());

    // Make default?
    if (Boolean.TRUE.equals(makeDefault)) {
      existingProps.setProperty(IndexPluginOptions.DEFAULT_PROPERTY_NAMESPACE, getPluginName());
    }

    // Write properties file
    ConfigOptions.writeProperties(getGeoWaveConfigFile(params), existingProps);

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

  public IndexPluginOptions getPluginOptions() {
    return pluginOptions;
  }

  public String getPluginName() {
    return parameters.get(0);
  }

  public String getNamespace() {
    return IndexPluginOptions.getIndexNamespace(getPluginName());
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String indexName) {
    parameters = new ArrayList<>();
    parameters.add(indexName);
  }

  public Boolean getMakeDefault() {
    return makeDefault;
  }

  public void setMakeDefault(final Boolean makeDefault) {
    this.makeDefault = makeDefault;
  }

  public String getType() {
    return type;
  }

  public void setType(final String type) {
    this.type = type;
  }

  public void setPluginOptions(final IndexPluginOptions pluginOptions) {
    this.pluginOptions = pluginOptions;
  }
}
