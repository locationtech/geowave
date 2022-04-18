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
import org.locationtech.geowave.core.cli.VersionUtils;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.DefaultStatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.internal.Console;

@GeowaveOperation(name = "recalc", parentOperation = StatsSection.class)
@Parameters(commandDescription = "Recalculate statistics in a given data store")
public class RecalculateStatsCommand extends AbstractStatsCommand<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RecalculateStatsCommand.class);

  @Parameter(description = "<store name>")
  private List<String> parameters = new ArrayList<>();

  @Parameter(
      names = "--all",
      description = "If specified, all matching statistics will be recalculated.")
  private boolean all = false;

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

    if (all) {
      // check for legacy stats table and if it exists, delete it and add all default stats
      final DataStoreOperations ops = storeOptions.createDataStoreOperations();
      final MetadataReader reader = ops.createMetadataReader(MetadataType.LEGACY_STATISTICS);
      boolean legacyStatsExists;
      // rather than checking for table existence, its more thorough for each data store
      // implementation to check for at least one row
      try (CloseableIterator<GeoWaveMetadata> it = reader.query(new MetadataQuery(null, null))) {
        legacyStatsExists = it.hasNext();
      }
      if (legacyStatsExists) {
        console.println(
            "Found legacy stats prior to v1.3. Deleting and recalculating all default stats as a migration to v"
                + VersionUtils.getVersion()
                + ".");
        // first let's do the add just to make sure things are in working order prior to deleting
        // legacy stats
        console.println("Adding default statistics...");
        final List<Statistic<?>> defaultStatistics = new ArrayList<>();
        for (final Index index : dataStore.getIndices()) {
          if (index instanceof DefaultStatisticsProvider) {
            defaultStatistics.addAll(((DefaultStatisticsProvider) index).getDefaultStatistics());
          }
        }
        for (final DataTypeAdapter<?> adapter : dataStore.getTypes()) {
          final DefaultStatisticsProvider defaultStatProvider =
              BaseDataStoreUtils.getDefaultStatisticsProvider(adapter);
          if (defaultStatProvider != null) {
            defaultStatistics.addAll(defaultStatProvider.getDefaultStatistics());
          }
        }
        dataStore.addEmptyStatistic(
            defaultStatistics.toArray(new Statistic[defaultStatistics.size()]));
        console.println("Deleting legacy statistics...");
        try (MetadataDeleter deleter = ops.createMetadataDeleter(MetadataType.LEGACY_STATISTICS)) {
          deleter.delete(new MetadataQuery(null, null));
        } catch (final Exception e) {
          LOGGER.warn("Error deleting legacy statistics", e);
        }

        // Clear out all options so that all stats get recalculated.
        statsOptions.setIndexName(null);
        statsOptions.setTypeName(null);
        statsOptions.setFieldName(null);
        statsOptions.setStatType(null);
        statsOptions.setTag(null);
      }
    }
    final List<Statistic<? extends StatisticValue<?>>> toRecalculate =
        statsOptions.resolveMatchingStatistics(dataStore, statStore, indexStore);

    if (toRecalculate.isEmpty()) {
      throw new ParameterException("A matching statistic could not be found");
    } else if ((toRecalculate.size() > 1) && !all) {
      throw new ParameterException(
          "Multiple statistics matched the given parameters.  If this is intentional, "
              + "supply the --all option, otherwise provide additional parameters to "
              + "specify which statistic to recalculate.");
    }
    final Statistic<?>[] toRecalcArray =
        toRecalculate.toArray(new Statistic<?>[toRecalculate.size()]);
    dataStore.recalcStatistic(toRecalcArray);

    console.println(
        toRecalculate.size()
            + " statistic"
            + (toRecalculate.size() == 1 ? " was" : "s were")
            + " successfully recalculated.");
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

  public void setParameters(final List<String> parameters) {
    this.parameters = parameters;
  }

  public void setAll(final boolean all) {
    this.all = all;
  }
}
