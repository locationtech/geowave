/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.index;

import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexPluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.operations.remote.options.BasicIndexOptions;
import org.locationtech.geowave.core.store.spi.DimensionalityTypeOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "add", parentOperation = IndexSection.class)
@Parameters(commandDescription = "Add an index to a data store")
public class AddIndexCommand extends ServiceEnabledCommand<String> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddIndexCommand.class);

  @Parameter(description = "<store name> <index name>", required = true)
  private List<String> parameters = new ArrayList<>();

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

    pluginOptions.selectPlugin(type);
    pluginOptions.setBasicIndexOptions(basicIndexOptions);
    opts = pluginOptions.getDimensionalityOptions();

    return true;
  }

  public void setBasicIndexOptions(BasicIndexOptions basicIndexOptions) {
    this.basicIndexOptions = basicIndexOptions;
  }

  @Override
  public void execute(final OperationParams params) {
    // Ensure we have all the required arguments
    if (parameters.size() != 2) {
      throw new ParameterException("Requires arguments: <store name> <index name>");
    }

    computeResults(params);
  }

  @Override
  public String computeResults(final OperationParams params) {
    final String storeName = parameters.get(0);
    final String indexName = parameters.get(1);
    pluginOptions.setName(indexName);

    // Attempt to load store.
    final DataStorePluginOptions storeOptions =
        CLIUtils.loadStore(storeName, getGeoWaveConfigFile(params), params.getConsole());

    final Index newIndex = pluginOptions.createIndex(storeOptions.createDataStore());

    final IndexStore indexStore = storeOptions.createIndexStore();

    if (indexStore.indexExists(newIndex.getName())) {
      throw new ParameterException("That index already exists: " + newIndex.getName());
    }

    storeOptions.createDataStore().addIndex(newIndex);

    return newIndex.getName();
  }

  public IndexPluginOptions getPluginOptions() {
    return pluginOptions;
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String storeName) {
    parameters = new ArrayList<>();
    parameters.add(storeName);
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
