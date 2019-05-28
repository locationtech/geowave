/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.local;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.IndexPluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.VisibilityOptions;
import org.locationtech.geowave.core.store.ingest.AbstractLocalFileIngestDriver;
import org.locationtech.geowave.core.store.ingest.DataAdapterProvider;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.locationtech.geowave.core.store.ingest.LocalInputCommandLineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This extends the local file driver to directly ingest data into GeoWave utilizing the
 * LocalFileIngestPlugin's that are discovered by the system.
 */
public class LocalFileIngestCLIDriver extends AbstractLocalFileIngestDriver {
  public static final int INGEST_BATCH_SIZE = 50000;
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileIngestCLIDriver.class);
  protected DataStorePluginOptions storeOptions;
  protected List<IndexPluginOptions> indexOptions;
  protected VisibilityOptions ingestOptions;
  protected Map<String, LocalFileIngestPlugin<?>> ingestPlugins;
  protected int threads;
  protected ExecutorService ingestExecutor;

  public LocalFileIngestCLIDriver(
      final DataStorePluginOptions storeOptions,
      final List<IndexPluginOptions> indexOptions,
      final Map<String, LocalFileIngestPlugin<?>> ingestPlugins,
      final VisibilityOptions ingestOptions,
      final LocalInputCommandLineOptions inputOptions,
      final int threads) {
    super(inputOptions);
    this.storeOptions = storeOptions;
    this.indexOptions = indexOptions;
    this.ingestOptions = ingestOptions;
    this.ingestPlugins = ingestPlugins;
    this.threads = threads;
  }

  @Override
  protected Map<String, Index> getIndices() throws IOException {
    final Map<String, Index> specifiedPrimaryIndexes = new HashMap<>();
    for (final IndexPluginOptions dimensionType : indexOptions) {
      final Index primaryIndex = dimensionType.createIndex();
      if (primaryIndex == null) {
        LOGGER.error("Could not get index instance, getIndex() returned null;");
        throw new IOException("Could not get index instance, getIndex() returned null");
      }
      specifiedPrimaryIndexes.put(primaryIndex.getName(), primaryIndex);
    }
    return specifiedPrimaryIndexes;
  }

  @Override
  protected boolean isSupported(
      final String providerName,
      final DataAdapterProvider<?> adapterProvider) {
    return checkIndexesAgainstProvider(providerName, adapterProvider, indexOptions);
  }

  @Override
  protected int getNumThreads() {
    return threads;
  }

  @Override
  protected String getGlobalVisibility() {
    return ingestOptions.getVisibility();
  }

  @Override
  protected Map<String, LocalFileIngestPlugin<?>> getIngestPlugins() {
    return ingestPlugins;
  }

  @Override
  protected DataStore getDataStore() {
    return storeOptions.createDataStore();
  }
}
