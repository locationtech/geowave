/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.cli;

import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.datastore.accumulo.split.SplitCommandLineOptions;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.ParametersDelegate;

public abstract class AbstractSplitsCommand extends DefaultOperation {

  @Parameter(description = "<storename>")
  private List<String> parameters = new ArrayList<>();

  @ParametersDelegate
  protected SplitCommandLineOptions splitOptions = new SplitCommandLineOptions();

  protected DataStorePluginOptions inputStoreOptions = null;

  public AbstractSplitsCommand() {}

  public void execute(final OperationParams params) throws Exception {

    // Ensure we have all the required arguments
    if (parameters.size() != 1) {
      throw new ParameterException("Requires arguments: <storename>");
    }

    final String inputStoreName = parameters.get(0);

    inputStoreOptions =
        CLIUtils.loadStore(inputStoreName, getGeoWaveConfigFile(params), params.getConsole());

    doSplit();
  }

  public abstract void doSplit() throws Exception;

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String storeName) {
    parameters = new ArrayList<>();
    parameters.add(storeName);
  }

  public SplitCommandLineOptions getSplitOptions() {
    return splitOptions;
  }

  public void setSplitOptions(final SplitCommandLineOptions splitOptions) {
    this.splitOptions = splitOptions;
  }

  public DataStorePluginOptions getInputStoreOptions() {
    return inputStoreOptions;
  }
}
