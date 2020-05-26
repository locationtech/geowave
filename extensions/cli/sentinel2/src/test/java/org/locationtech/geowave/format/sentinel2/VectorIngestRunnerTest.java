/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.sentinel2;

import static org.junit.Assert.assertTrue;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalDimensionalityTypeProvider.Bias;
import org.locationtech.geowave.core.geotime.index.api.SpatialIndexBuilder;
import org.locationtech.geowave.core.geotime.index.api.SpatialTemporalIndexBuilder;
import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.store.StoreLoader;
import org.locationtech.geowave.core.store.index.IndexPluginOptions.PartitionStrategy;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;
import com.beust.jcommander.ParameterException;
import it.geosolutions.jaiext.JAIExt;

public class VectorIngestRunnerTest {
  @BeforeClass
  public static void setup() throws IOException {
    GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().put(
        "memory",
        new MemoryStoreFactoryFamily());
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
  }

  @Test
  public void testIngestProviders() throws Exception {
    for (final Sentinel2ImageryProvider provider : Sentinel2ImageryProvider.getProviders()) {
      testIngest(provider.providerName());
    }
  }

  public void testIngest(final String providerName) throws Exception {
    JAIExt.initJAIEXT();

    final Sentinel2ImageryProvider provider = Sentinel2ImageryProvider.getProvider(providerName);
    if (provider == null) {
      throw new RuntimeException("Unable to find '" + providerName + "' Sentinel2 provider");
    }

    final Date[] timePeriodSettings = Tests.timePeriodSettings(providerName);
    final Date startDate = timePeriodSettings[0];
    final Date endDate = timePeriodSettings[1];

    final Sentinel2BasicCommandLineOptions analyzeOptions = new Sentinel2BasicCommandLineOptions();
    analyzeOptions.setWorkspaceDir(Tests.WORKSPACE_DIR);
    analyzeOptions.setProviderName(providerName);
    analyzeOptions.setCollection(provider.collections()[0]);
    analyzeOptions.setLocation("T30TXN");
    analyzeOptions.setStartDate(startDate);
    analyzeOptions.setEndDate(endDate);
    analyzeOptions.setCqlFilter(
        "BBOX(shape,-1.8274,42.3253,-1.6256,42.4735) AND (band='B4' OR band='B8')");

    final VectorIngestRunner runner =
        new VectorIngestRunner(
            analyzeOptions,
            Arrays.asList("memorystore", "spatialindex,spatempindex"));

    final ManualOperationParams params = new ManualOperationParams();
    params.getContext().put(
        ConfigOptions.PROPERTIES_FILE_CONTEXT,
        new File(
            VectorIngestRunnerTest.class.getClassLoader().getResource(
                "geowave-config.properties").toURI()));

    createIndices(params);

    runner.runInternal(params);

    try (CloseableIterator<Object> results =
        getStorePluginOptions(params).createDataStore().query(QueryBuilder.newBuilder().build())) {
      assertTrue("Store is empty when it should have at least one result", results.hasNext());
    }

    // Not sure what assertions can be made about the indexes.
  }

  private DataStorePluginOptions getStorePluginOptions(final OperationParams params) {
    final File configFile = (File) params.getContext().get(ConfigOptions.PROPERTIES_FILE_CONTEXT);

    final StoreLoader inputStoreLoader = new StoreLoader("memorystore");
    if (!inputStoreLoader.loadFromConfig(configFile, params.getConsole())) {
      throw new ParameterException("Cannot find store name: " + inputStoreLoader.getStoreName());
    }

    return inputStoreLoader.getDataStorePlugin();
  }

  private void createIndices(final OperationParams params) {
    final IndexStore indexStore = getStorePluginOptions(params).createIndexStore();
    // Create the spatial index
    final SpatialIndexBuilder builder = new SpatialIndexBuilder();
    builder.setName("spatialindex");
    builder.setNumPartitions(1);
    builder.setIncludeTimeInCommonIndexModel(false);
    indexStore.addIndex(builder.createIndex());

    // Create the spatial temporal index
    final SpatialTemporalIndexBuilder st_builder = new SpatialTemporalIndexBuilder();
    st_builder.setName("spatempindex");
    st_builder.setBias(Bias.BALANCED);
    st_builder.setMaxDuplicates(-1);
    st_builder.setNumPartitions(1);
    st_builder.setPartitionStrategy(PartitionStrategy.ROUND_ROBIN);
    st_builder.setPeriodicity(Unit.DAY);
    indexStore.addIndex(st_builder.createIndex());
  }
}
