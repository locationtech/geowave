/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.sentinel2;

import java.io.File;
import java.util.List;
import org.geotools.feature.simple.SimpleFeatureBuilder;
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
import org.opengis.feature.type.AttributeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.ParameterException;

public class VectorIngestRunner extends AnalyzeRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(RasterIngestRunner.class);

  protected final List<String> parameters;
  private Writer<SimpleFeature> bandWriter;
  private Writer<SimpleFeature> sceneWriter;
  private SimpleFeatureType sceneType;

  public VectorIngestRunner(
      final Sentinel2BasicCommandLineOptions analyzeOptions,
      final List<String> parameters) {
    super(analyzeOptions);
    this.parameters = parameters;
  }

  @Override
  protected void runInternal(final OperationParams params) throws Exception {
    try {
      // Ensure we have all the required arguments
      if (parameters.size() != 2) {
        throw new ParameterException(
            "Requires arguments: <store name> <comma delimited index list>");
      }

      final String providerName = sentinel2Options.providerName();
      final String inputStoreName = parameters.get(0);
      final String indexList = parameters.get(1);

      // Get the Sentinel2 provider.
      final Sentinel2ImageryProvider provider = Sentinel2ImageryProvider.getProvider(providerName);
      if (provider == null) {
        throw new RuntimeException("Unable to find '" + providerName + "' Sentinel2 provider");
      }

      // Config file
      final File configFile = (File) params.getContext().get(ConfigOptions.PROPERTIES_FILE_CONTEXT);

      final DataStorePluginOptions storeOptions =
          CLIUtils.loadStore(inputStoreName, configFile, params.getConsole());

      final DataStore store = storeOptions.createDataStore();

      // Load the Indices
      final Index[] indices =
          DataStoreUtils.loadIndices(storeOptions.createIndexStore(), indexList).toArray(
              new Index[0]);

      sceneType = provider.sceneFeatureTypeBuilder().buildFeatureType();
      final FeatureDataAdapter sceneAdapter = new FeatureDataAdapter(sceneType);
      store.addType(sceneAdapter, indices);
      sceneWriter = store.createWriter(sceneAdapter.getTypeName());

      final SimpleFeatureType bandType = provider.bandFeatureTypeBuilder().buildFeatureType();
      final FeatureDataAdapter bandAdapter = new FeatureDataAdapter(bandType);
      store.addType(bandAdapter, indices);
      bandWriter = store.createWriter(bandAdapter.getTypeName());

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

  @Override
  protected void nextBand(final SimpleFeature band, final AnalysisInfo analysisInfo) {
    bandWriter.write(band);
    super.nextBand(band, analysisInfo);
  }

  @Override
  protected void nextScene(final SimpleFeature firstBandOfScene, final AnalysisInfo analysisInfo) {
    writeScene(sceneType, firstBandOfScene, sceneWriter);
    super.nextScene(firstBandOfScene, analysisInfo);
  }

  public static void writeScene(
      final SimpleFeatureType sceneType,
      final SimpleFeature firstBandOfScene,
      final Writer<SimpleFeature> sceneWriter) {
    final SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(sceneType);
    String fid = null;

    for (int i = 0; i < sceneType.getAttributeCount(); i++) {
      final AttributeDescriptor descriptor = sceneType.getDescriptor(i);

      final String name = descriptor.getLocalName();
      final Object value = firstBandOfScene.getAttribute(name);

      if (value != null) {
        featureBuilder.set(i, value);

        if (name.equals(SceneFeatureIterator.ENTITY_ID_ATTRIBUTE_NAME)) {
          fid = value.toString();
        }
      }
    }
    if (fid != null) {
      sceneWriter.write(featureBuilder.buildFeature(fid));
    }
  }
}
