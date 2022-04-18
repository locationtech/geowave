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
import java.util.List;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticBinningStrategy;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.statistics.StatisticsRegistry;
import org.locationtech.geowave.core.store.statistics.binning.CompositeBinningStrategy;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "add", parentOperation = StatsSection.class)
@Parameters(commandDescription = "Add a statistic to a data store")
public class AddStatCommand extends ServiceEnabledCommand<Void> {

  @Parameter(description = "<store name>")
  private List<String> parameters = new ArrayList<>();

  @Parameter(
      names = {"-b", "--binningStrategy"},
      description = "If specified, statistics will be binned using the given strategy.")
  private String binningStrategyName = null;

  @Parameter(
      names = {"-skip", "--skipCalculation"},
      description = "If specified, the initial value of the statistic will not be calculated.")
  private boolean skipCalculation = false;

  @Parameter(names = {"-t", "--type"}, required = true, description = "The statistic type to add.")
  private String statType = null;

  @ParametersDelegate
  private Statistic<?> statOptions;

  @ParametersDelegate
  private StatisticBinningStrategy binningStrategy = null;

  @Override
  public boolean prepare(final OperationParams params) {
    if (!super.prepare(params)) {
      return false;
    }

    if (statType == null) {
      throw new ParameterException("Missing statistic type.");
    }
    statOptions = StatisticsRegistry.instance().getStatistic(statType);
    if (statOptions == null) {
      throw new ParameterException("Unrecognized statistic type: " + statType);
    }

    if (binningStrategyName != null) {
      binningStrategy = StatisticsRegistry.instance().getBinningStrategy(binningStrategyName);
      if (binningStrategy == null) {
        throw new ParameterException("Unrecognized binning strategy: " + binningStrategyName);
      }
      if (binningStrategy instanceof CompositeBinningStrategy) {
        throw new ParameterException(
            "Statistics with composite binning strategies are currently unable to be added through the CLI.");
      }
    }

    return true;
  }

  @Override
  public void execute(final OperationParams params) {
    if (parameters.size() != 1) {
      throw new ParameterException("Requires argument: <store name>");
    }
    computeResults(params);
  }

  @Override
  public Void computeResults(final OperationParams params) {
    final String storeName = parameters.get(0);

    // Attempt to load store.
    final DataStorePluginOptions storeOptions =
        CLIUtils.loadStore(storeName, getGeoWaveConfigFile(params), params.getConsole());

    final DataStore dataStore = storeOptions.createDataStore();

    if (binningStrategy != null) {
      statOptions.setBinningStrategy(binningStrategy);
    }
    if (skipCalculation) {
      dataStore.addEmptyStatistic(statOptions);
    } else {
      dataStore.addStatistic(statOptions);
    }
    return null;
  }

  void setBinningStrategyName(final String binningStrategyName) {
    this.binningStrategyName = binningStrategyName;
  }

  void setStatType(final String statType) {
    this.statType = statType;
  }

  void setSkipCalculation(final boolean skipCalculation) {
    this.skipCalculation = skipCalculation;
  }

  void setParameters(final List<String> parameters) {
    this.parameters = parameters;
  }

}
