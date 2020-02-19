/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.kmeans.operations;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.mapreduce.operations.AnalyticSection;
import org.locationtech.geowave.analytic.mapreduce.operations.options.PropertyManagementConverter;
import org.locationtech.geowave.analytic.param.StoreParameters;
import org.locationtech.geowave.analytic.spark.kmeans.KMeansRunner;
import org.locationtech.geowave.analytic.store.PersistableStore;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.store.StoreLoader;
import org.locationtech.jts.util.Stopwatch;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "kmeansspark", parentOperation = AnalyticSection.class)
@Parameters(commandDescription = "KMeans clustering using Spark ML")
public class KmeansSparkCommand extends ServiceEnabledCommand<Void> implements Command {
  @Parameter(description = "<input store name> <output store name>")
  private List<String> parameters = new ArrayList<>();

  @ParametersDelegate
  private KMeansSparkOptions kMeansSparkOptions = new KMeansSparkOptions();

  DataStorePluginOptions inputDataStore = null;
  DataStorePluginOptions outputDataStore = null;

  // Log some timing
  Stopwatch stopwatch = new Stopwatch();

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

    // Save a reference to the store in the property management.
    final PersistableStore persistedStore = new PersistableStore(inputDataStore);
    final PropertyManagement properties = new PropertyManagement();
    properties.store(StoreParameters.StoreParam.INPUT_STORE, persistedStore);

    // Convert properties from DBScanOptions and CommonOptions
    final PropertyManagementConverter converter = new PropertyManagementConverter(properties);
    converter.readProperties(kMeansSparkOptions);

    final KMeansRunner runner = new KMeansRunner();
    runner.setAppName(kMeansSparkOptions.getAppName());
    runner.setMaster(kMeansSparkOptions.getMaster());
    runner.setHost(kMeansSparkOptions.getHost());
    runner.setSplits(kMeansSparkOptions.getMinSplits(), kMeansSparkOptions.getMaxSplits());
    runner.setInputDataStore(inputDataStore);
    runner.setNumClusters(kMeansSparkOptions.getNumClusters());
    runner.setNumIterations(kMeansSparkOptions.getNumIterations());
    runner.setUseTime(kMeansSparkOptions.isUseTime());
    runner.setTypeName(kMeansSparkOptions.getTypeName());

    if (kMeansSparkOptions.getEpsilon() != null) {
      runner.setEpsilon(kMeansSparkOptions.getEpsilon());
    }

    if (kMeansSparkOptions.getTypeName() != null) {
      runner.setTypeName(kMeansSparkOptions.getTypeName());
    }

    if (kMeansSparkOptions.getCqlFilter() != null) {
      runner.setCqlFilter(kMeansSparkOptions.getCqlFilter());
    }
    runner.setGenerateHulls(kMeansSparkOptions.isGenerateHulls());
    runner.setComputeHullData(kMeansSparkOptions.isComputeHullData());
    runner.setHullTypeName(kMeansSparkOptions.getHullTypeName());
    runner.setCentroidTypeName(kMeansSparkOptions.getCentroidTypeName());
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

  public KMeansSparkOptions getKMeansSparkOptions() {
    return kMeansSparkOptions;
  }

  public void setKMeansSparkOptions(final KMeansSparkOptions kMeansSparkOptions) {
    this.kMeansSparkOptions = kMeansSparkOptions;
  }
}
