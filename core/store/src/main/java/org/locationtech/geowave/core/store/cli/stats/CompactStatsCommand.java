/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.stats;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "compact", parentOperation = StatsSection.class)
@Parameters(commandDescription = "Compact all statistics in data store")
public class CompactStatsCommand extends DefaultOperation implements Command {

  @Parameter(description = "<store name>")
  private List<String> parameters = new ArrayList<>();

  private DataStorePluginOptions inputStoreOptions = null;

  /** Prep the driver & run the operation. */
  @Override
  public void execute(final OperationParams params) {

    // Ensure we have all the required arguments
    if (parameters.size() != 1) {
      throw new ParameterException("Requires arguments: <store name>");
    }

    final String inputStoreName = parameters.get(0);

    // Attempt to load input store.
    if (inputStoreOptions == null) {
      // Attempt to load store.
      inputStoreOptions =
          CLIUtils.loadStore(inputStoreName, getGeoWaveConfigFile(params), params.getConsole());
    }

    final DataStatisticsStore statsStore = inputStoreOptions.createDataStatisticsStore();
    final DataStoreOperations operations = inputStoreOptions.createDataStoreOperations();
    operations.mergeStats(statsStore);
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String storeName, final String adapterId) {
    parameters = Arrays.asList(storeName, adapterId);
  }

  public DataStorePluginOptions getInputStoreOptions() {
    return inputStoreOptions;
  }

  public void setInputStoreOptions(final DataStorePluginOptions inputStoreOptions) {
    this.inputStoreOptions = inputStoreOptions;
  }
}
