/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
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
import org.locationtech.geowave.analytic.mapreduce.nn.GeoWaveExtractNNJobRunner;
import org.locationtech.geowave.analytic.mapreduce.operations.options.CommonOptions;
import org.locationtech.geowave.analytic.mapreduce.operations.options.NearestNeighborOptions;
import org.locationtech.geowave.analytic.mapreduce.operations.options.PropertyManagementConverter;
import org.locationtech.geowave.analytic.param.ExtractParameters.Extract;
import org.locationtech.geowave.analytic.param.StoreParameters;
import org.locationtech.geowave.analytic.store.PersistableStore;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.mapreduce.operations.ConfigHDFSCommand;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "nn", parentOperation = AnalyticSection.class)
@Parameters(commandDescription = "Nearest neighbors")
public class NearestNeighborCommand extends ServiceEnabledCommand<Void> {

  @Parameter(description = "<store name>")
  private List<String> parameters = new ArrayList<>();

  @ParametersDelegate
  private CommonOptions commonOptions = new CommonOptions();

  @ParametersDelegate
  private NearestNeighborOptions nnOptions = new NearestNeighborOptions();

  private DataStorePluginOptions inputStoreOptions = null;

  @Override
  public void execute(final OperationParams params) throws Exception {

    computeResults(params);
  }

  @Override
  public boolean runAsync() {
    return true;
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

  public NearestNeighborOptions getNnOptions() {
    return nnOptions;
  }

  public void setNnOptions(final NearestNeighborOptions nnOptions) {
    this.nnOptions = nnOptions;
  }

  public DataStorePluginOptions getInputStoreOptions() {
    return inputStoreOptions;
  }

  @Override
  public Void computeResults(final OperationParams params) throws Exception {
    // Ensure we have all the required arguments
    if ((parameters.size() != 1) && (inputStoreOptions == null)) {
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

    // Attempt to load store.
    inputStoreOptions =
        CLIUtils.loadStore(inputStoreName, getGeoWaveConfigFile(params), params.getConsole());

    // Save a reference to the store in the property management.
    final PersistableStore persistedStore = new PersistableStore(inputStoreOptions);
    final PropertyManagement properties = new PropertyManagement();
    properties.store(StoreParameters.StoreParam.INPUT_STORE, persistedStore);

    // Convert properties from DBScanOptions and CommonOptions
    final PropertyManagementConverter converter = new PropertyManagementConverter(properties);
    converter.readProperties(commonOptions);
    converter.readProperties(nnOptions);
    properties.store(Extract.QUERY, commonOptions.buildQuery());

    final GeoWaveExtractNNJobRunner runner = new GeoWaveExtractNNJobRunner();
    final int status = runner.run(properties);
    if (status != 0) {
      throw new RuntimeException("Failed to execute: " + status);
    }
    return null;
  }
}
