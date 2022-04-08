/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.ingest;

import org.locationtech.geowave.core.ingest.avro.GeoWaveAvroFormatPlugin;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import org.locationtech.geowave.core.ingest.spi.IngestFormatPluginProviderSpi;
import org.locationtech.geowave.core.store.ingest.IngestFormatOptions;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.opengis.feature.simple.SimpleFeature;
import com.beust.jcommander.ParametersDelegate;

public abstract class AbstractSimpleFeatureIngestFormat<I> implements
    IngestFormatPluginProviderSpi<I, SimpleFeature> {
  protected final SerializableSimpleFeatureIngestOptions myOptions =
      new SerializableSimpleFeatureIngestOptions();

  private AbstractSimpleFeatureIngestPlugin<I> getInstance(final IngestFormatOptions options) {
    final AbstractSimpleFeatureIngestPlugin<I> myInstance = newPluginInstance(options);
    myInstance.setFilterProvider(myOptions.getCqlFilterOptionProvider());
    myInstance.setTypeNameProvider(myOptions.getTypeNameOptionProvider());
    myInstance.setSerializationFormatProvider(myOptions.getSerializationFormatOptionProvider());
    myInstance.setGeometrySimpOptionProvider(myOptions.getGeometrySimpOptionProvider());
    return myInstance;
  }

  protected abstract AbstractSimpleFeatureIngestPlugin<I> newPluginInstance(
      IngestFormatOptions options);

  @Override
  public GeoWaveAvroFormatPlugin<I, SimpleFeature> createAvroFormatPlugin(
      final IngestFormatOptions options) {
    return getInstance(options);
  }

  @Override
  public IngestFromHdfsPlugin<I, SimpleFeature> createIngestFromHdfsPlugin(
      final IngestFormatOptions options) {
    return getInstance(options);
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
