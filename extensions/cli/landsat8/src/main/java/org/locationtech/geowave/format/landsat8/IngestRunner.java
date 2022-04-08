/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.landsat8;

import java.io.File;
import java.util.List;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestRunner extends RasterIngestRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(IngestRunner.class);
  private Writer<SimpleFeature> bandWriter;
  private Writer<SimpleFeature> sceneWriter;
  private final VectorOverrideCommandLineOptions vectorOverrideOptions;
  private SimpleFeatureType sceneType;

  public IngestRunner(
      final Landsat8BasicCommandLineOptions analyzeOptions,
      final Landsat8DownloadCommandLineOptions downloadOptions,
      final Landsat8RasterIngestCommandLineOptions ingestOptions,
      final VectorOverrideCommandLineOptions vectorOverrideOptions,
      final List<String> parameters) {
    super(analyzeOptions, downloadOptions, ingestOptions, parameters);
    this.vectorOverrideOptions = vectorOverrideOptions;
  }

  @Override
  protected void processParameters(final OperationParams params) throws Exception { // Ensure we
    // have all the
    // required
    // arguments
    super.processParameters(params);

    final DataStore vectorStore;
    final Index[] vectorIndices;
    // Config file
    final File configFile = (File) params.getContext().get(ConfigOptions.PROPERTIES_FILE_CONTEXT);

    if ((vectorOverrideOptions.getVectorStore() != null)
        && !vectorOverrideOptions.getVectorStore().trim().isEmpty()) {
      final String vectorStoreName = vectorOverrideOptions.getVectorStore();
      final DataStorePluginOptions vectorStoreOptions =
          CLIUtils.loadStore(vectorStoreName, configFile, params.getConsole());
      vectorStore = vectorStoreOptions.createDataStore();
    } else {
      vectorStore = store;
    }
    if ((vectorOverrideOptions.getVectorIndex() != null)
        && !vectorOverrideOptions.getVectorIndex().trim().isEmpty()) {
      final String vectorIndexList = vectorOverrideOptions.getVectorIndex();

      // Load the Indices
      vectorIndices =
          DataStoreUtils.loadIndices(vectorStore, vectorIndexList).toArray(new Index[0]);
    } else {
      vectorIndices = indices;
    }
    sceneType = SceneFeatureIterator.createFeatureType();
    final FeatureDataAdapter sceneAdapter = new FeatureDataAdapter(sceneType);
    vectorStore.addType(sceneAdapter, vectorIndices);
    sceneWriter = vectorStore.createWriter(sceneAdapter.getTypeName());
    final SimpleFeatureType bandType = BandFeatureIterator.createFeatureType(sceneType);
    final FeatureDataAdapter bandAdapter = new FeatureDataAdapter(bandType);

    vectorStore.addType(bandAdapter, vectorIndices);
    bandWriter = vectorStore.createWriter(bandAdapter.getTypeName());
  }

  @Override
  protected void nextBand(final SimpleFeature band, final AnalysisInfo analysisInfo) {
    bandWriter.write(band);
    super.nextBand(band, analysisInfo);
  }

  @Override
  protected void nextScene(final SimpleFeature firstBandOfScene, final AnalysisInfo analysisInfo) {
    VectorIngestRunner.writeScene(sceneType, firstBandOfScene, sceneWriter);
    super.nextScene(firstBandOfScene, analysisInfo);
  }

  @Override
  protected void runInternal(final OperationParams params) throws Exception {
    try {
      super.runInternal(params);
    } finally {
      if (sceneWriter != null) {
        sceneWriter.close();
      }
      if (bandWriter != null) {
        bandWriter.close();
      }
    }
  }
}
