/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.internal.Console;

@GeowaveOperation(name = "rm", parentOperation = StatsSection.class)
@Parameters(commandDescription = "Remove a statistic from a data store")
public class RemoveStatCommand extends AbstractStatsCommand<Void> {

  @Parameter(description = "<store name>")
  private List<String> parameters = new ArrayList<>();

  @Parameter(
      names = "--all",
      description = "If specified, all matching statistics will be removed.")
  private boolean all = false;

  @Parameter(
      names = "--force",
      description = "Force an internal statistic to be removed.  IMPORTANT: Removing statistics "
          + "that are marked as \"internal\" can have a detrimental impact on performance!")
  private boolean force = false;

  @Override
  public void execute(final OperationParams params) {
    computeResults(params);
  }

  @Override
  protected boolean performStatsCommand(
      final DataStorePluginOptions storeOptions,
      final StatsCommandLineOptions statsOptions,
      final Console console) throws IOException {

    final DataStore dataStore = storeOptions.createDataStore();
    final DataStatisticsStore statStore = storeOptions.createDataStatisticsStore();
    final IndexStore indexStore = storeOptions.createIndexStore();

    final List<Statistic<? extends StatisticValue<?>>> toRemove =
        statsOptions.resolveMatchingStatistics(dataStore, statStore, indexStore);

    if (!force) {
      for (Statistic<?> stat : toRemove) {
        if (stat.isInternal()) {
          throw new ParameterException(
              "Unable to remove an internal statistic without specifying the --force option. "
                  + "Removing an internal statistic can have a detrimental impact on performance.");
        }
      }
    }
    if (toRemove.isEmpty()) {
      throw new ParameterException("A matching statistic could not be found");
    } else if (toRemove.size() > 1 && !all) {
      throw new ParameterException(
          "Multiple statistics matched the given parameters.  If this is intentional, "
              + "supply the --all option, otherwise provide additional parameters to "
              + "specify which statistic to delete.");
    }

    if (!statStore.removeStatistics(toRemove.iterator())) {
      throw new RuntimeException("Unable to remove statistics");
    }

    console.println(
        toRemove.size()
            + " statistic"
            + (toRemove.size() == 1 ? " was" : "s were")
            + " successfully removed.");

    return true;
  }

  @Override
  public Void computeResults(final OperationParams params) {
    // Ensure we have all the required arguments
    if (parameters.size() != 1) {
      throw new ParameterException("Requires argument: <store name>");
    }

    super.run(params, parameters);
    return null;
  }
}
