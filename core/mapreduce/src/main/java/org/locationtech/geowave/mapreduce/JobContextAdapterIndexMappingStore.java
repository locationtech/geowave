/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;

/**
 * This class implements an adapter index mapping store by first checking the job context for an
 * adapter and keeping a local cache of adapters that have been discovered. It will check the
 * metadata store if it cannot find an adapter in the job context.
 */
public class JobContextAdapterIndexMappingStore implements AdapterIndexMappingStore {
  private static final Class<?> CLASS = JobContextAdapterIndexMappingStore.class;
  private final JobContext context;
  private final AdapterIndexMappingStore persistentAdapterIndexMappingStore;
  private final Map<Short, AdapterToIndexMapping> adapterCache = new HashMap<>();

  public JobContextAdapterIndexMappingStore(
      final JobContext context,
      final AdapterIndexMappingStore persistentAdapterIndexMappingStore) {
    this.context = context;
    this.persistentAdapterIndexMappingStore = persistentAdapterIndexMappingStore;
  }

  private AdapterToIndexMapping getIndicesForAdapterInternal(final short internalAdapterId) {
    // first try to get it from the job context
    AdapterToIndexMapping adapter = getAdapterToIndexMapping(context, internalAdapterId);
    if (adapter == null) {
      // then try to get it from the persistent store
      adapter = persistentAdapterIndexMappingStore.getIndicesForAdapter(internalAdapterId);
    }

    if (adapter != null) {
      adapterCache.put(internalAdapterId, adapter);
    }
    return adapter;
  }

  @Override
  public void removeAll() {
    adapterCache.clear();
  }

  protected static AdapterToIndexMapping getAdapterToIndexMapping(
      final JobContext context,
      final short internalAdapterId) {
    return GeoWaveConfiguratorBase.getAdapterToIndexMapping(CLASS, context, internalAdapterId);
  }

  public static void addAdapterToIndexMapping(
      final Configuration configuration,
      final AdapterToIndexMapping adapter) {
    GeoWaveConfiguratorBase.addAdapterToIndexMapping(CLASS, configuration, adapter);
  }

  @Override
  public AdapterToIndexMapping getIndicesForAdapter(final short adapterId) {
    AdapterToIndexMapping adapter = adapterCache.get(adapterId);
    if (adapter == null) {
      adapter = getIndicesForAdapterInternal(adapterId);
    }
    return adapter;
  }

  @Override
  public void addAdapterIndexMapping(final AdapterToIndexMapping mapping) {
    adapterCache.put(mapping.getAdapterId(), mapping);
  }

  @Override
  public void remove(final short internalAdapterId) {
    adapterCache.remove(internalAdapterId);
  }

  @Override
  public boolean remove(final short internalAdapterId, final String indexName) {

    if (!adapterCache.containsKey(internalAdapterId)) {
      return false;
    }

    final AdapterToIndexMapping mapping = adapterCache.get(internalAdapterId);
    final String[] indexNames = mapping.getIndexNames();
    boolean found = false;
    for (int i = 0; i < indexNames.length; i++) {
      if (indexNames[i].compareTo(indexName) == 0) {
        found = true;
        break;
      }
    }

    if (!found) {
      return false;
    }

    if (indexNames.length > 1) {
      // update the mapping and reset it
      final String[] newIndices = new String[indexNames.length - 1];
      int count = 0;
      for (int i = 0; i < indexNames.length; i++) {
        if (indexNames[i].compareTo(indexName) == 0) {
          continue;
        } else {
          newIndices[count] = indexNames[i];
          count++;
        }
      }
      adapterCache.remove(internalAdapterId);
      adapterCache.put(
          mapping.getAdapterId(),
          new AdapterToIndexMapping(internalAdapterId, newIndices));
    } else {
      // otherwise just remove the mapping
      adapterCache.remove(internalAdapterId);
    }

    return true;
  }
}
