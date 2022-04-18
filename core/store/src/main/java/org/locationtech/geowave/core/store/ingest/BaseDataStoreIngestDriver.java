/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.ingest;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IngestOptions;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.api.IngestOptions.IngestCallback;
import org.locationtech.geowave.core.store.api.WriteResults;
import org.locationtech.geowave.core.store.api.Writer;

public class BaseDataStoreIngestDriver extends AbstractLocalFileIngestDriver {

  private final DataStore store;
  private final IngestOptions<?> ingestOptions;
  private final Index[] indices;

  public BaseDataStoreIngestDriver(
      final DataStore store,
      final IngestOptions<?> ingestOptions,
      final Index... indices) {
    super();
    this.store = store;
    this.indices = indices;
    this.ingestOptions = ingestOptions;
    configProperties = ingestOptions.getProperties();
  }

  @Override
  protected int getNumThreads() {
    return ingestOptions.getThreads();
  }

  @Override
  protected VisibilityHandler getVisibilityHandler() {
    return ingestOptions.getVisibilityHandler();
  }

  @Override
  protected Map<String, LocalFileIngestPlugin<?>> getIngestPlugins() {
    if (ingestOptions.getFormat() != null) {
      return Collections.singletonMap("provided", ingestOptions.getFormat());
    }
    return IngestUtils.getDefaultLocalIngestPlugins();
  }

  @Override
  protected DataStore getDataStore() {
    return store;
  }

  public boolean runIngest(final String inputPath) {
    return super.runOperation(inputPath, null);
  }

  @Override
  protected Map<String, Index> getIndices() throws IOException {
    final Map<String, Index> indexMap = new HashMap<>(indices.length);
    for (final Index i : indices) {
      indexMap.put(i.getName(), i);
    }
    return indexMap;
  }

  @Override
  protected String[] getExtensions() {
    return ingestOptions.getFileExtensions();
  }

  @Override
  protected boolean isSupported(
      final String providerName,
      final DataAdapterProvider<?> adapterProvider) {
    return true;
  }

  @Override
  protected void write(final Writer writer, final GeoWaveData<?> geowaveData) {
    Object obj = geowaveData.getValue();
    if (ingestOptions.getTransform() != null) {
      obj = ((Function) ingestOptions.getTransform()).apply(obj);
    }
    if ((ingestOptions.getFilter() != null)
        && !((Predicate) ingestOptions.getFilter()).test(geowaveData.getValue())) {
      return;
    }
    final WriteResults results = writer.write(obj);
    if (ingestOptions.getCallback() != null) {
      ((IngestCallback) ingestOptions.getCallback()).dataWritten(results, obj);
    }
  }
}
