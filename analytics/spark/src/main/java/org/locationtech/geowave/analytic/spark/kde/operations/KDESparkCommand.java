/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.kde.operations;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.analytic.mapreduce.operations.AnalyticSection;
import org.locationtech.geowave.analytic.spark.kde.KDERunner;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.StoreLoader;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "sparkkde", parentOperation = AnalyticSection.class)
@Parameters(commandDescription = "KDE via Spark")
public class KDESparkCommand extends ServiceEnabledCommand<Void> implements Command {
  @Parameter(description = "<input storename> <output storename>")
  private List<String> parameters = new ArrayList<>();

  @ParametersDelegate
  private KDESparkOptions kdeSparkOptions = new KDESparkOptions();

  DataStorePluginOptions inputDataStore = null;
  DataStorePluginOptions outputDataStore = null;

  @Override
  public void execute(final OperationParams params) throws Exception {
    // Ensure we have all the required arguments
    if (parameters.size() != 2) {
      throw new ParameterException("Requires arguments: <input storename> <output storename>");
    }
    computeResults(params);
  }

  @Override
  public Void computeResults(final OperationParams params) throws Exception {
    final String inputStoreName = parameters.get(0);
    final String outputStoreName = parameters.get(1);

    // Config file
    final File configFile = getGeoWaveConfigFile(params);

    final StoreLoader inputStoreLoader = new StoreLoader(inputStoreName);
    if (!inputStoreLoader.loadFromConfig(configFile)) {
      throw new ParameterException("Cannot find input store: " + inputStoreLoader.getStoreName());
    }
    inputDataStore = inputStoreLoader.getDataStorePlugin();

    final StoreLoader outputStoreLoader = new StoreLoader(outputStoreName);
    if (!outputStoreLoader.loadFromConfig(configFile)) {
      throw new ParameterException("Cannot find output store: " + outputStoreLoader.getStoreName());
    }
    outputDataStore = outputStoreLoader.getDataStorePlugin();

    final KDERunner runner = new KDERunner();
    runner.setAppName(kdeSparkOptions.getAppName());
    runner.setMaster(kdeSparkOptions.getMaster());
    runner.setHost(kdeSparkOptions.getHost());
    runner.setSplits(kdeSparkOptions.getMinSplits(), kdeSparkOptions.getMaxSplits());
    runner.setInputDataStore(inputDataStore);
    runner.setTypeName(kdeSparkOptions.getTypeName());
    runner.setOutputDataStore(outputDataStore);
    runner.setCoverageName(kdeSparkOptions.getCoverageName());
    runner.setIndexName(kdeSparkOptions.getIndexName());
    runner.setMinLevel(kdeSparkOptions.getMinLevel());
    runner.setMaxLevel(kdeSparkOptions.getMaxLevel());
    runner.setTileSize(kdeSparkOptions.getTileSize());
    
    if (kdeSparkOptions.getCqlFilter() != null) {
      runner.setCqlFilter(kdeSparkOptions.getCqlFilter());
    }
    runner.setOutputDataStore(outputDataStore);
    try {
      runner.run();
    } catch (final IOException e) {
      throw new RuntimeException("Failed to execute: " + e.getMessage());
    } finally {
      runner.close();
    }

    return null;
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String storeName) {
    parameters = new ArrayList<>();
    parameters.add(storeName);
  }

  public DataStorePluginOptions getInputStoreOptions() {
    return inputDataStore;
  }

  public DataStorePluginOptions getOutputStoreOptions() {
    return outputDataStore;
  }

  public KDESparkOptions getKDESparkOptions() {
    return kdeSparkOptions;
  }

  public void setKDESparkOptions(final KDESparkOptions kdeSparkOptions) {
    this.kdeSparkOptions = kdeSparkOptions;
  }
}
