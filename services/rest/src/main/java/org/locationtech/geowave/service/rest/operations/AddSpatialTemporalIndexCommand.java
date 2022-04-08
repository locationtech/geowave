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
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalOptions;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.index.IndexSection;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.store.StoreLoader;
import org.locationtech.geowave.core.store.index.IndexPluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.operations.remote.options.BasicIndexOptions;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@Parameters(commandDescription = "Add a spatial temporal index to a GeoWave store")
public class AddSpatialTemporalIndexCommand extends ServiceEnabledCommand<String> {
  /** A REST Operation for the AddIndexCommand where --type=spatial_temporal */
  @Parameter(description = "<store name> <index name>", required = true)
  private List<String> parameters = new ArrayList<>();

  @ParametersDelegate
  private BasicIndexOptions basicIndexOptions = new BasicIndexOptions();

  private IndexPluginOptions pluginOptions = new IndexPluginOptions();

  @ParametersDelegate
  SpatialTemporalOptions opts = new SpatialTemporalOptions();

  @Override
  public boolean prepare(final OperationParams params) {

    pluginOptions.selectPlugin("spatial_temporal");
    pluginOptions.setBasicIndexOptions(basicIndexOptions);
    pluginOptions.setDimensionalityTypeOptions(opts);
    return true;
  }

  @Override
  public void execute(final OperationParams params) throws Exception {
    computeResults(params);
  }

  @Override
  public String getId() {
    return IndexSection.class.getName() + ".add/spatial_temporal";
  }

  @Override
  public String getPath() {
    return "v0/index/add/spatial_temporal";
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

  public void setParameters(final String storeName, final String indexName) {
    parameters = new ArrayList<>();
    parameters.add(storeName);
    parameters.add(indexName);
  }

  public String getType() {
    return "spatial_temporal";
  }

  public void setPluginOptions(final IndexPluginOptions pluginOptions) {
    this.pluginOptions = pluginOptions;
  }

  @Override
  public String computeResults(final OperationParams params) throws Exception {

    // Ensure that a name is chosen.
    if (getParameters().size() < 2) {
      System.out.println(getParameters());
      throw new ParameterException("Must specify store name and index name");
    }

    final String storeName = getParameters().get(0);
    final String indexName = getParameters().get(1);
    pluginOptions.setName(indexName);

    // Attempt to load store.
    final File configFile = getGeoWaveConfigFile(params);

    final StoreLoader inputStoreLoader = new StoreLoader(storeName);
    if (!inputStoreLoader.loadFromConfig(configFile)) {
      throw new ParameterException("Cannot find store name: " + inputStoreLoader.getStoreName());
    }
    DataStorePluginOptions storeOptions = inputStoreLoader.getDataStorePlugin();

    final Index newIndex = pluginOptions.createIndex(storeOptions.createDataStore());

    IndexStore indexStore = storeOptions.createIndexStore();

    Index existingIndex = indexStore.getIndex(newIndex.getName());
    if (existingIndex != null) {
      throw new ParameterException("That index already exists: " + newIndex.getName());
    }

    storeOptions.createDataStore().addIndex(newIndex);

    return newIndex.getName();
  }
}
