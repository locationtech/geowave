/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.mapreduce.clustering.runner.MultiLevelJumpKMeansClusteringJobRunner;
import org.locationtech.geowave.analytic.mapreduce.operations.options.CommonOptions;
import org.locationtech.geowave.analytic.mapreduce.operations.options.KMeansCommonOptions;
import org.locationtech.geowave.analytic.mapreduce.operations.options.KMeansJumpOptions;
import org.locationtech.geowave.analytic.mapreduce.operations.options.PropertyManagementConverter;
import org.locationtech.geowave.analytic.param.ExtractParameters.Extract;
import org.locationtech.geowave.analytic.param.StoreParameters;
import org.locationtech.geowave.analytic.store.PersistableStore;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.store.StoreLoader;
import org.locationtech.geowave.mapreduce.operations.ConfigHDFSCommand;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "kmeansjump", parentOperation = AnalyticSection.class)
@Parameters(commandDescription = "KMeans clustering using jump method")
public class KmeansJumpCommand extends DefaultOperation implements Command {

  @Parameter(description = "<store name>")
  private List<String> parameters = new ArrayList<>();

  @ParametersDelegate
  private CommonOptions commonOptions = new CommonOptions();

  @ParametersDelegate
  private KMeansCommonOptions kmeansCommonOptions = new KMeansCommonOptions();

  @ParametersDelegate
  private KMeansJumpOptions kmeansJumpOptions = new KMeansJumpOptions();

  private DataStorePluginOptions inputStoreOptions = null;

  @Override
  public void execute(final OperationParams params) throws Exception {

    // Ensure we have all the required arguments
    if (parameters.size() != 1) {
      throw new ParameterException("Requires arguments: <storename>");
    }

    final String inputStoreName = parameters.get(0);

    // Config file
    final File configFile = getGeoWaveConfigFile(params);

    if (commonOptions.getMapReduceHdfsHostPort() == null) {

      final Properties configProperties = ConfigOptions.loadProperties(configFile);
      final String hdfsFSUrl = ConfigHDFSCommand.getHdfsUrl(configProperties);
      commonOptions.setMapReduceHdfsHostPort(hdfsFSUrl);
    }

    final StoreLoader inputStoreLoader = new StoreLoader(inputStoreName);
    if (!inputStoreLoader.loadFromConfig(configFile)) {
      throw new ParameterException("Cannot find store name: " + inputStoreLoader.getStoreName());
    }
    inputStoreOptions = inputStoreLoader.getDataStorePlugin();

    // Save a reference to the store in the property management.
    final PersistableStore persistedStore = new PersistableStore(inputStoreOptions);
    final PropertyManagement properties = new PropertyManagement();
    properties.store(StoreParameters.StoreParam.INPUT_STORE, persistedStore);

    // Convert properties from DBScanOptions and CommonOptions
    final PropertyManagementConverter converter = new PropertyManagementConverter(properties);
    converter.readProperties(commonOptions);
    converter.readProperties(kmeansCommonOptions);
    converter.readProperties(kmeansJumpOptions);
    properties.store(Extract.QUERY, commonOptions.buildQuery());

    final MultiLevelJumpKMeansClusteringJobRunner runner =
        new MultiLevelJumpKMeansClusteringJobRunner();
    final int status = runner.run(properties);
    if (status != 0) {
      throw new RuntimeException("Failed to execute: " + status);
    }
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String storeName) {
    parameters = new ArrayList<>();
    parameters.add(storeName);
  }

  public CommonOptions getCommonOptions() {
    return commonOptions;
  }

  public void setCommonOptions(final CommonOptions commonOptions) {
    this.commonOptions = commonOptions;
  }

  public KMeansCommonOptions getKmeansCommonOptions() {
    return kmeansCommonOptions;
  }

  public void setKmeansCommonOptions(final KMeansCommonOptions kmeansCommonOptions) {
    this.kmeansCommonOptions = kmeansCommonOptions;
  }

  public KMeansJumpOptions getKmeansJumpOptions() {
    return kmeansJumpOptions;
  }

  public void setKmeansJumpOptions(final KMeansJumpOptions kmeansJumpOptions) {
    this.kmeansJumpOptions = kmeansJumpOptions;
  }
}
