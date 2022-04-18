/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.migration.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.util.UtilSection;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.DataStoreProperty;
import org.locationtech.geowave.core.store.PropertyStore;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.store.StoreLoader;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.BasicIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.statistics.DefaultStatisticsProvider;
import org.locationtech.geowave.migration.legacy.adapter.LegacyInternalDataAdapterWrapper;
import org.locationtech.geowave.migration.legacy.adapter.vector.LegacyFeatureDataAdapter;
import org.locationtech.geowave.migration.legacy.core.geotime.LegacySpatialField;
import org.locationtech.geowave.migration.legacy.core.store.LegacyAdapterIndexMappingStore;
import org.locationtech.geowave.migration.legacy.core.store.LegacyAdapterToIndexMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.internal.Console;
import com.google.common.collect.Lists;

@GeowaveOperation(name = "migrate", parentOperation = UtilSection.class)
@Parameters(
    commandDescription = "Migrates data in a GeoWave data store to be compatible with the CLI version")
public class MigrationCommand extends DefaultOperation implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(MigrationCommand.class);

  @Parameter(description = "<store name>", required = true)
  private final List<String> parameters = new ArrayList<>();

  /** Prep the driver & run the operation. */
  @Override
  public void execute(final OperationParams params) {

    if (parameters.size() != 1) {
      throw new ParameterException("Requires arguments: <store name>");
    }

    final String storeName = parameters.get(0);

    final StoreLoader inputStoreLoader = new StoreLoader(storeName);
    if (!inputStoreLoader.loadFromConfig(getGeoWaveConfigFile(), params.getConsole())) {
      throw new ParameterException("Cannot find store name: " + inputStoreLoader.getStoreName());
    }
    final DataStorePluginOptions storeOptions = inputStoreLoader.getDataStorePlugin();

    final DataStoreOperations operations = storeOptions.createDataStoreOperations();
    final PropertyStore propertyStore = storeOptions.createPropertyStore();

    try {
      if (!operations.metadataExists(MetadataType.ADAPTER)
          && !operations.metadataExists(MetadataType.INDEX)) {
        throw new ParameterException("There is no data in the data store to migrate.");
      }
    } catch (final IOException e) {
      throw new RuntimeException("Unable to determine if metadata tables exist for data store.", e);
    }

    final DataStoreProperty dataVersionProperty =
        propertyStore.getProperty(BaseDataStoreUtils.DATA_VERSION_PROPERTY);
    final int dataVersion = dataVersionProperty == null ? 0 : (int) dataVersionProperty.getValue();
    if (dataVersion == BaseDataStoreUtils.DATA_VERSION) {
      throw new ParameterException(
          "The data version matches the CLI version, there are no migrations to apply.");
    }
    if (dataVersion > BaseDataStoreUtils.DATA_VERSION) {
      throw new ParameterException(
          "The data store is using a newer serialization format.  Please update to a newer version "
              + "of the CLI that is compatible with the data store.");
    }

    // Do migration
    if (dataVersion < 1) {
      migrate0to1(storeOptions, operations, params.getConsole());
    }
    propertyStore.setProperty(
        new DataStoreProperty(
            BaseDataStoreUtils.DATA_VERSION_PROPERTY,
            BaseDataStoreUtils.DATA_VERSION));
    params.getConsole().println("Migration completed successfully!");
  }

  public void migrate0to1(
      final DataStorePluginOptions options,
      final DataStoreOperations operations,
      final Console console) {
    console.println("Migration 1.x -> 2.x");
    final DataStore dataStore = options.createDataStore();
    console.println("  Migrating data type adapters...");
    final PersistentAdapterStore adapterStore = options.createAdapterStore();
    final List<Short> adapterIDs = Lists.newArrayList();
    int migratedAdapters = 0;
    final InternalDataAdapter<?>[] adapters = adapterStore.getAdapters();
    for (final InternalDataAdapter<?> adapter : adapters) {
      adapterIDs.add(adapter.getAdapterId());
      if (adapter instanceof LegacyInternalDataAdapterWrapper) {
        adapterStore.removeAdapter(adapter.getAdapterId());
        // Write updated adapter
        adapterStore.addAdapter(
            ((LegacyInternalDataAdapterWrapper<?>) adapter).getUpdatedAdapter());
        migratedAdapters++;
      } else if (adapter.getAdapter() instanceof LegacyFeatureDataAdapter) {
        final FeatureDataAdapter updatedAdapter =
            ((LegacyFeatureDataAdapter) adapter.getAdapter()).getUpdatedAdapter();
        final VisibilityHandler visibilityHandler =
            ((LegacyFeatureDataAdapter) adapter.getAdapter()).getVisibilityHandler();
        // Write updated adapter
        adapterStore.removeAdapter(adapter.getAdapterId());
        adapterStore.addAdapter(
            updatedAdapter.asInternalAdapter(adapter.getAdapterId(), visibilityHandler));
        migratedAdapters++;
      }
    }
    if (migratedAdapters > 0) {
      console.println("    Migrated " + migratedAdapters + " data type adapters.");
    } else {
      console.println("    No data type adapters needed to be migrated.");
    }
    console.println("  Migrating indices...");
    final IndexStore indexStore = options.createIndexStore();
    int migratedIndices = 0;
    try (CloseableIterator<Index> indices = indexStore.getIndices()) {
      while (indices.hasNext()) {
        final Index index = indices.next();
        final CommonIndexModel indexModel = index.getIndexModel();
        // if the index model uses any spatial fields, update and re-write
        if ((indexModel != null) && (indexModel instanceof BasicIndexModel)) {
          final NumericDimensionField<?>[] oldFields = indexModel.getDimensions();
          final NumericDimensionField<?>[] updatedFields =
              new NumericDimensionField<?>[oldFields.length];
          boolean updated = false;
          for (int i = 0; i < oldFields.length; i++) {
            if (oldFields[i] instanceof LegacySpatialField) {
              updatedFields[i] = ((LegacySpatialField<?>) oldFields[i]).getUpdatedField(index);
              updated = true;
            } else {
              updatedFields[i] = oldFields[i];
            }
          }
          if (updated) {
            ((BasicIndexModel) indexModel).init(updatedFields);
            indexStore.removeIndex(index.getName());
            indexStore.addIndex(index);
            migratedIndices++;
          }
        }
      }
    }
    if (migratedIndices > 0) {
      console.println("    Migrated " + migratedIndices + " indices.");
    } else {
      console.println("    No indices needed to be migrated.");
    }

    console.println("  Migrating index mappings...");
    // Rewrite adapter to index mappings
    final LegacyAdapterIndexMappingStore legacyIndexMappings =
        new LegacyAdapterIndexMappingStore(
            operations,
            options.getFactoryOptions().getStoreOptions());
    final AdapterIndexMappingStore indexMappings = options.createAdapterIndexMappingStore();
    console.println("    Writing new mappings...");
    int indexMappingCount = 0;
    for (final Short adapterId : adapterIDs) {
      final LegacyAdapterToIndexMapping mapping =
          legacyIndexMappings.getIndicesForAdapter(adapterId);
      final InternalDataAdapter<?> adapter = adapterStore.getAdapter(adapterId);
      for (final String indexName : mapping.getIndexNames()) {
        indexMappings.addAdapterIndexMapping(
            BaseDataStoreUtils.mapAdapterToIndex(adapter, indexStore.getIndex(indexName)));
        indexMappingCount++;
      }
    }
    if (indexMappingCount > 0) {
      console.println("    Migrated " + indexMappingCount + " index mappings.");
      console.println("    Deleting legacy index mappings...");
      try (MetadataDeleter deleter =
          operations.createMetadataDeleter(MetadataType.LEGACY_INDEX_MAPPINGS)) {
        deleter.delete(new MetadataQuery(null));
      } catch (final Exception e) {
        LOGGER.warn("Error deleting legacy index mappings", e);
      }

    } else {
      console.println("    No index mappings to migrate.");
    }

    // Update statistics
    console.println("  Migrating statistics...");
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
    console.println("    Calculating updated statistics...");
    dataStore.addStatistic(defaultStatistics.toArray(new Statistic[defaultStatistics.size()]));
    console.println("    Deleting legacy statistics...");
    try (MetadataDeleter deleter =
        operations.createMetadataDeleter(MetadataType.LEGACY_STATISTICS)) {
      deleter.delete(new MetadataQuery(null));
    } catch (final Exception e) {
      LOGGER.warn("Error deleting legacy statistics", e);
    }
  }
}
