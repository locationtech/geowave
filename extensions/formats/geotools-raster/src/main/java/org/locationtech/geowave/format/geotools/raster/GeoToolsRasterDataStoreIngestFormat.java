/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.geotools.raster;

import org.locationtech.geowave.core.ingest.avro.GeoWaveAvroFormatPlugin;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import org.locationtech.geowave.core.ingest.spi.IngestFormatPluginProviderSpi;
import org.locationtech.geowave.core.store.ingest.IngestFormatOptions;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.opengis.coverage.grid.GridCoverage;

/**
 * This represents an ingest format plugin provider for GeoTools grid coverage (raster) formats. It
 * currently only supports ingesting data directly from a local file system into GeoWave.
 */
public class GeoToolsRasterDataStoreIngestFormat implements
    IngestFormatPluginProviderSpi<Object, GridCoverage> {
  private final RasterOptionProvider optionProvider = new RasterOptionProvider();

  @Override
  public GeoWaveAvroFormatPlugin<Object, GridCoverage> createAvroFormatPlugin(
      final IngestFormatOptions options) throws UnsupportedOperationException {
    // unsupported right now
    throw new UnsupportedOperationException(
        "GeoTools raster files cannot be ingested using intermediate avro files");
  }

  @Override
  public IngestFromHdfsPlugin<Object, GridCoverage> createIngestFromHdfsPlugin(
      final IngestFormatOptions options) throws UnsupportedOperationException {
    // unsupported right now
    throw new UnsupportedOperationException("GeoTools raster files cannot be ingested from HDFS");
  }

  @Override
  public LocalFileIngestPlugin<GridCoverage> createLocalFileIngestPlugin(
      final IngestFormatOptions options) throws UnsupportedOperationException {
    return new GeoToolsRasterDataStoreIngestPlugin(optionProvider);
  }

  @Override
  public String getIngestFormatName() {
    return "geotools-raster";
  }

  @Override
  public String getIngestFormatDescription() {
    return "all file-based raster formats supported within geotools";
  }

  @Override
  public IngestFormatOptions createOptionsInstances() {
    return optionProvider;
  }
}
