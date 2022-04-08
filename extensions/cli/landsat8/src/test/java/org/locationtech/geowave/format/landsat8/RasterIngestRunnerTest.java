/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.landsat8;

import static org.junit.Assert.assertTrue;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.lang.SystemUtils;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.locationtech.geowave.adapter.raster.plugin.gdal.InstallGdal;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.core.geotime.index.api.SpatialIndexBuilder;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;
import com.beust.jcommander.JCommander;
import it.geosolutions.jaiext.JAIExt;

@Ignore
public class RasterIngestRunnerTest {

  @BeforeClass
  public static void setup() throws IOException {

    // Skip this test if we're on a Mac
    org.junit.Assume.assumeTrue(isNotMac() && isGDALEnabled());

    GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().put(
        "memory",
        new MemoryStoreFactoryFamily());

    InstallGdal.main(new String[] {System.getenv("GDAL_DIR")});
  }

  private static boolean isGDALEnabled() {
    String enabled = System.getenv("GDAL_DISABLED");
    if (enabled != null && enabled.trim().equalsIgnoreCase("true")) {
      return false;
    }
    return true;
  }

  private static boolean isNotMac() {
    return !SystemUtils.IS_OS_MAC;
  }

  @Test
  public void testIngest() throws Exception {
    JAIExt.initJAIEXT();

    final Landsat8BasicCommandLineOptions analyzeOptions = new Landsat8BasicCommandLineOptions();
    analyzeOptions.setNBestScenes(1);
    analyzeOptions.setCqlFilter(
        "BBOX(shape,-76.6,42.34,-76.4,42.54) and band='BQA' and sizeMB < 1");
    analyzeOptions.setUseCachedScenes(true);
    analyzeOptions.setWorkspaceDir(Tests.WORKSPACE_DIR);

    final Landsat8DownloadCommandLineOptions downloadOptions =
        new Landsat8DownloadCommandLineOptions();
    downloadOptions.setOverwriteIfExists(false);

    final Landsat8RasterIngestCommandLineOptions ingestOptions =
        new Landsat8RasterIngestCommandLineOptions();
    ingestOptions.setRetainImages(true);
    ingestOptions.setCreatePyramid(false);
    ingestOptions.setScale(10);
    ingestOptions.setCreateHistogram(true);
    final RasterIngestRunner runner =
        new RasterIngestRunner(
            analyzeOptions,
            downloadOptions,
            ingestOptions,
            Arrays.asList("memorystore", "spatialindex"));
    final ManualOperationParams params = new ManualOperationParams();
    params.getContext().put(
        ConfigOptions.PROPERTIES_FILE_CONTEXT,
        new File(
            RasterIngestRunnerTest.class.getClassLoader().getResource(
                "geowave-config.properties").toURI()));
    createIndices(params);
    runner.runInternal(params);
    try (CloseableIterator<Object> results =
        getStorePluginOptions(params).createDataStore().query(QueryBuilder.newBuilder().build())) {
      assertTrue("Store is empty", results.hasNext());
    }

    // Not sure what assertions can be made about the index.
  }

  private DataStorePluginOptions getStorePluginOptions(final OperationParams params) {
    final File configFile = (File) params.getContext().get(ConfigOptions.PROPERTIES_FILE_CONTEXT);

    return CLIUtils.loadStore("memorystore", configFile, new JCommander().getConsole());
  }

  private void createIndices(final OperationParams params) {
    DataStore dataStore = getStorePluginOptions(params).createDataStore();

    // Create the spatial index
    final SpatialIndexBuilder builder = new SpatialIndexBuilder();
    builder.setName("spatialindex");
    builder.setNumPartitions(1);
    builder.setIncludeTimeInCommonIndexModel(false);
    dataStore.addIndex(builder.createIndex());
  }
}
