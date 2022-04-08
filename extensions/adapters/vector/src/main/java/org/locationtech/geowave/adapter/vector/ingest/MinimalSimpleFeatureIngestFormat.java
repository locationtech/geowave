/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.ingest;

import org.locationtech.geowave.core.ingest.avro.AvroWholeFile;
import org.locationtech.geowave.core.ingest.avro.GeoWaveAvroFormatPlugin;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import org.locationtech.geowave.core.ingest.spi.IngestFormatPluginProviderSpi;
import org.locationtech.geowave.core.store.ingest.IngestFormatOptions;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.opengis.feature.simple.SimpleFeature;

public abstract class MinimalSimpleFeatureIngestFormat implements
    IngestFormatPluginProviderSpi<AvroWholeFile, SimpleFeature> {
  protected final SimpleFeatureIngestOptions myOptions = new SimpleFeatureIngestOptions();

  private MinimalSimpleFeatureIngestPlugin getInstance(final IngestFormatOptions options) {
    final MinimalSimpleFeatureIngestPlugin myInstance = newPluginInstance(options);
    myInstance.setFilterProvider(myOptions.getCqlFilterOptionProvider());
    myInstance.setTypeNameProvider(myOptions.getTypeNameOptionProvider());
    myInstance.setGeometrySimpOptionProvider(myOptions.getGeometrySimpOptionProvider());
    return myInstance;
  }

  protected abstract MinimalSimpleFeatureIngestPlugin newPluginInstance(
      IngestFormatOptions options);

  @Override
  public GeoWaveAvroFormatPlugin<AvroWholeFile, SimpleFeature> createAvroFormatPlugin(
      final IngestFormatOptions options) {
    throw new UnsupportedOperationException("Avro format is unsupported for this plugin.");
  }

  @Override
  public IngestFromHdfsPlugin<AvroWholeFile, SimpleFeature> createIngestFromHdfsPlugin(
      final IngestFormatOptions options) {
    throw new UnsupportedOperationException("Ingest from HDFS is unsupported for this plugin.");
  }

  @Override
  public LocalFileIngestPlugin<SimpleFeature> createLocalFileIngestPlugin(
      final IngestFormatOptions options) {
    return getInstance(options);
  }

  /**
   * Create an options instance. We may want to change this code from a singleton instance to
   * actually allow multiple instances per format.
   */
  @Override
  public IngestFormatOptions createOptionsInstances() {
    myOptions.setPluginOptions(internalGetIngestFormatOptionProviders());
    return myOptions;
  }

  protected Object internalGetIngestFormatOptionProviders() {
    return null;
  }
}
