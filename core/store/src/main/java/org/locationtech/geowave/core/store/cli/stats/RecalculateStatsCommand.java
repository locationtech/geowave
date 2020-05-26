/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
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
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.DataStoreStatisticsProvider;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.statistics.StatsCompositionTool;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.base.BaseDataStore;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.internal.Console;

@GeowaveOperation(name = "recalc", parentOperation = StatsSection.class)
@Parameters(commandDescription = "Recalculate the statistics of a data store")
public class RecalculateStatsCommand extends AbstractStatsCommand<Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecalculateStatsCommand.class);

  @Parameter(
      names = {"--typeName"},
      description = "Optionally recalculate a single datatype's stats")
  private String typeName = "";

  @Parameter(description = "<store name>")
  private List<String> parameters = new ArrayList<>();

  @Override
  public void execute(final OperationParams params) {
    computeResults(params);
  }

  @Override
  protected boolean performStatsCommand(
      final DataStorePluginOptions storeOptions,
      final InternalDataAdapter<?> adapter,
      final StatsCommandLineOptions statsOptions,
      final Console console) throws IOException {

    try {
      final DataStore dataStore = storeOptions.createDataStore();
      if (!(dataStore instanceof BaseDataStore)) {
        LOGGER.warn(
            "Datastore type '"
                + dataStore.getClass().getName()
                + "' must be instance of BaseDataStore to recalculate stats");
        return false;
      }

      final AdapterIndexMappingStore mappingStore = storeOptions.createAdapterIndexMappingStore();
      final IndexStore indexStore = storeOptions.createIndexStore();

      boolean isFirstTime = true;
      for (final Index index : mappingStore.getIndicesForAdapter(adapter.getAdapterId()).getIndices(
          indexStore)) {
        @SuppressWarnings({"rawtypes", "unchecked"})
        final DataStoreStatisticsProvider provider =
            new DataStoreStatisticsProvider(adapter, index, isFirstTime);
        final String[] authorizations = getAuthorizations(statsOptions.getAuthorizations());

        try (StatsCompositionTool<?> statsTool =
            new StatsCompositionTool(
                provider,
                storeOptions.createDataStatisticsStore(),
                index,
                adapter,
                true)) {
          try (CloseableIterator<?> entryIt =
              ((BaseDataStore) dataStore).query(
                  QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                      index.getName()).setAuthorizations(authorizations).build(),
                  (ScanCallback) statsTool)) {
            while (entryIt.hasNext()) {
              entryIt.next();
            }
          }
        }
        isFirstTime = false;
      }

    } catch (final Exception ex) {
      LOGGER.error("Error while writing statistics.", ex);
      return false;
    }

    return true;
  }

  public void setTypeName(String typeName) {
    this.typeName = typeName;
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String storeName, final String adapterName) {
    parameters = new ArrayList<>();
    parameters.add(storeName);
    if (adapterName != null) {
      parameters.add(adapterName);
    }
  }

  @Override
  public Void computeResults(final OperationParams params) {
    // Ensure we have all the required arguments
    if (parameters.size() < 1) {
      throw new ParameterException("Requires arguments: <store name>");
    }
    if ((typeName != null) && !typeName.trim().isEmpty()) {
      parameters.add(typeName);
    }
    super.run(params, parameters);
    return null;
  }
}
