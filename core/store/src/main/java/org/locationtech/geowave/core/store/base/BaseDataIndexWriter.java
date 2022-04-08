/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.base;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.api.WriteResults;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.callback.IngestCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

class BaseDataIndexWriter<T> implements Writer<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseIndexWriter.class);
  protected final DataStoreOperations operations;
  protected final DataStoreOptions options;
  protected final IngestCallback<T> callback;
  protected RowWriter writer;

  protected final InternalDataAdapter<T> adapter;
  protected final AdapterToIndexMapping indexMapping;
  protected final VisibilityHandler visibilityHandler;
  final Closeable closable;

  protected BaseDataIndexWriter(
      final InternalDataAdapter<T> adapter,
      final AdapterToIndexMapping indexMapping,
      final VisibilityHandler visibilityHandler,
      final DataStoreOperations operations,
      final DataStoreOptions options,
      final IngestCallback<T> callback,
      final Closeable closable) {
    this.operations = operations;
    this.options = options;
    this.callback = callback;
    this.adapter = adapter;
    this.closable = closable;
    this.indexMapping = indexMapping;
    this.visibilityHandler = visibilityHandler;
  }

  @Override
  public Index[] getIndices() {
    return new Index[] {DataIndexUtils.DATA_ID_INDEX};
  }

  @Override
  public WriteResults write(final T entry) {
    return write(entry, visibilityHandler);
  }

  @Override
  public WriteResults write(final T entry, final VisibilityHandler visibilityHandler) {
    IntermediaryWriteEntryInfo entryInfo;
    ensureOpen();

    if (writer == null) {
      LOGGER.error("Null writer - empty list returned");
      return new WriteResults();
    }
    entryInfo =
        BaseDataStoreUtils.getWriteInfo(
            entry,
            adapter,
            indexMapping,
            DataIndexUtils.DATA_ID_INDEX,
            visibilityHandler,
            options.isSecondaryIndexing(),
            true,
            options.isVisibilityEnabled());
    final GeoWaveRow[] rows = entryInfo.getRows();

    writer.write(rows);
    callback.entryIngested(entry, rows);
    return new WriteResults();
  }

  @Override
  public void close() {
    try {
      closable.close();
    } catch (final IOException e) {
      LOGGER.error("Cannot close callbacks", e);
    }
    // thread safe close
    closeInternal();
  }

  @Override
  public synchronized void flush() {
    // thread safe flush of the writers
    if (writer != null) {
      writer.flush();
    }
    if (this.callback instanceof Flushable) {
      try {
        ((Flushable) callback).flush();
      } catch (final IOException e) {
        LOGGER.error("Cannot flush callbacks", e);
      }
    }
  }

  protected synchronized void closeInternal() {
    if (writer != null) {
      try {
        writer.close();
        writer = null;
      } catch (final Exception e) {
        LOGGER.warn("Unable to close writer", e);
      }
    }
  }

  @SuppressFBWarnings(justification = "This is intentional to avoid unnecessary sync")
  protected void ensureOpen() {
    if (writer == null) {
      synchronized (this) {
        if (writer == null) {
          try {
            writer = operations.createDataIndexWriter(adapter);
          } catch (final Exception e) {
            LOGGER.error("Unable to open writer", e);
          }
        }
      }
    }
  }
}
