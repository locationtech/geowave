/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.hdfs.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.ingest.avro.AbstractStageWholeFileToAvro;
import org.locationtech.geowave.core.ingest.avro.AvroWholeFile;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class can be sub-classed as a general-purpose recipe for parallelizing ingestion of files
 * either locally or by directly staging the binary of the file to HDFS and then ingesting it within
 * the map phase of a map-reduce job.
 */
public abstract class AbstractLocalIngestWithMapper<T> extends AbstractStageWholeFileToAvro
    implements
    LocalFileIngestPlugin<T>,
    IngestFromHdfsPlugin<AvroWholeFile, T>,
    Persistable {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLocalIngestWithMapper.class);

  @Override
  public boolean isUseReducerPreferred() {
    return false;
  }

  @Override
  public IngestWithMapper<AvroWholeFile, T> ingestWithMapper() {
    return new InternalIngestWithMapper<>(this);
  }

  @Override
  public CloseableIterator<GeoWaveData<T>> toGeoWaveData(
      final URL input,
      final String[] indexNames) {
    try (final InputStream inputStream = input.openStream()) {
      return toGeoWaveDataInternal(inputStream, indexNames);
    } catch (final IOException e) {
      LOGGER.warn("Cannot open file, unable to ingest", e);
    }
    return new CloseableIterator.Wrapper(Collections.emptyIterator());
  }

  protected abstract CloseableIterator<GeoWaveData<T>> toGeoWaveDataInternal(
      final InputStream file,
      final String[] indexNames);

  @Override
  public IngestWithReducer<AvroWholeFile, ?, ?, T> ingestWithReducer() {
    return null;
  }

  protected static class InternalIngestWithMapper<T> implements IngestWithMapper<AvroWholeFile, T> {
    private AbstractLocalIngestWithMapper parentPlugin;

    public InternalIngestWithMapper() {}

    public InternalIngestWithMapper(final AbstractLocalIngestWithMapper parentPlugin) {
      this.parentPlugin = parentPlugin;
    }

    @Override
    public DataTypeAdapter<T>[] getDataAdapters() {
      return parentPlugin.getDataAdapters();
    }

    @Override
    public CloseableIterator<GeoWaveData<T>> toGeoWaveData(
        final AvroWholeFile input,
        final String[] indexNames) {
      final InputStream inputStream = new ByteBufferBackedInputStream(input.getOriginalFile());
      return parentPlugin.toGeoWaveDataInternal(inputStream, indexNames);
    }

    @Override
    public byte[] toBinary() {
      return PersistenceUtils.toClassId(parentPlugin);
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      parentPlugin = (AbstractLocalIngestWithMapper) PersistenceUtils.fromClassId(bytes);
    }

    @Override
    public String[] getSupportedIndexTypes() {
      return parentPlugin.getSupportedIndexTypes();
    }
  }
}
