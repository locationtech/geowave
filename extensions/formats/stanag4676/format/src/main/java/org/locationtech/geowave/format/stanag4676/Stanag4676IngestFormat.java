/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.stanag4676;

import org.locationtech.geowave.core.ingest.avro.AvroWholeFile;
import org.locationtech.geowave.core.ingest.avro.GeoWaveAvroFormatPlugin;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import org.locationtech.geowave.core.ingest.spi.IngestFormatPluginProviderSpi;
import org.locationtech.geowave.core.store.ingest.IngestFormatOptions;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;

public class Stanag4676IngestFormat implements
    IngestFormatPluginProviderSpi<AvroWholeFile, Object> {
  private static Stanag4676IngestPlugin singletonInstance;

  private static synchronized Stanag4676IngestPlugin getSingletonInstance() {
    if (singletonInstance == null) {
      singletonInstance = new Stanag4676IngestPlugin();
    }
    return singletonInstance;
  }

  @Override
  public GeoWaveAvroFormatPlugin<AvroWholeFile, Object> createAvroFormatPlugin(
      final IngestFormatOptions options) throws UnsupportedOperationException {
    return getSingletonInstance();
  }

  @Override
  public IngestFromHdfsPlugin<AvroWholeFile, Object> createIngestFromHdfsPlugin(
      final IngestFormatOptions options) throws UnsupportedOperationException {
    return getSingletonInstance();
  }

  @Override
  public LocalFileIngestPlugin<Object> createLocalFileIngestPlugin(
      final IngestFormatOptions options) throws UnsupportedOperationException {
    return getSingletonInstance();
  }

  @Override
  public String getIngestFormatName() {
    return "stanag4676";
  }

  @Override
  public String getIngestFormatDescription() {
    return "xml files representing track data that adheres to the schema defined by STANAG-4676";
  }

  @Override
  public IngestFormatOptions createOptionsInstances() {
    // for now don't support filtering
    return null;
  }
}
