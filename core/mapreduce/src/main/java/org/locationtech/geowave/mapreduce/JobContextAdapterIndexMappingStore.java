/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import com.google.common.collect.Lists;

/**
 * This class implements an adapter index mapping store by first checking the job context for an
 * adapter and keeping a local cache of adapters that have been discovered. It will check the
 * metadata store if it cannot find an adapter in the job context.
 */
public class JobContextAdapterIndexMappingStore implements AdapterIndexMappingStore {
  private static final Class<?> CLASS = JobContextAdapterIndexMappingStore.class;
  private final JobContext context;
  private final AdapterIndexMappingStore persistentAdapterIndexMappingStore;
  private final Map<Short, List<AdapterToIndexMapping>> adapterCache = new HashMap<>();

  public JobContextAdapterIndexMappingStore(
      final JobContext context,
      final AdapterIndexMappingStore persistentAdapterIndexMappingStore) {
    this.context = context;
    this.persistentAdapterIndexMappingStore = persistentAdapterIndexMappingStore;
  }

  private AdapterToIndexMapping[] getIndicesForAdapterInternal(final short internalAdapterId) {
    // first try to get it from the job context
    AdapterToIndexMapping[] adapter = getAdapterToIndexMapping(context, internalAdapterId);
    if (adapter == null) {
      // then try to get it from the persistent store
      adapter = persistentAdapterIndexMappingStore.getIndicesForAdapter(internalAdapterId);
    }

    if (adapter != null) {
      adapterCache.put(internalAdapterId, Lists.newArrayList(adapter));
    }
    return adapter;
  }

  @Override
  public void removeAll() {
    adapterCache.clear();
  }

  protected static AdapterToIndexMapping[] getAdapterToIndexMapping(
      final JobContext context,
      final short internalAdapterId) {
    return GeoWaveConfiguratorBase.getAdapterToIndexMappings(CLASS, context, internalAdapterId);
  }

  public static void addAdapterToIndexMapping(
      final Configuration configuration,
      final AdapterToIndexMapping[] adapter) {
    GeoWaveConfiguratorBase.addAdapterToIndexMappings(CLASS, configuration, adapter);
  }

  @Override
  public AdapterToIndexMapping[] getIndicesForAdapter(final short adapterId) {
    List<AdapterToIndexMapping> adapterList = adapterCache.get(adapterId);
    if (adapterList == null) {
      return getIndicesForAdapterInternal(adapterId);
    }
    return adapterList.toArray(new AdapterToIndexMapping[adapterList.size()]);
  }

  @Override
  public AdapterToIndexMapping getMapping(final short adapterId, final String indexName) {
    if (indexName.equals(DataIndexUtils.DATA_ID_INDEX.getName())) {
      return new AdapterToIndexMapping(adapterId, indexName, Lists.newArrayList());
    }
    final AdapterToIndexMapping[] adapterIndices = getIndicesForAdapter(adapterId);
    return Arrays.stream(adapterIndices).filter(
        mapping -> mapping.getIndexName().equals(indexName)).findFirst().orElse(null);
  }

  @Override
  public void addAdapterIndexMapping(final AdapterToIndexMapping mapping) {
    if (!adapterCache.containsKey(mapping.getAdapterId())) {
      adapterCache.put(mapping.getAdapterId(), Lists.newArrayList());
    }
    adapterCache.get(mapping.getAdapterId()).add(mapping);
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

    final List<AdapterToIndexMapping> mappings = adapterCache.get(internalAdapterId);
    AdapterToIndexMapping found = null;
    for (int i = 0; i < mappings.size(); i++) {
      if (mappings.get(i).getIndexName().compareTo(indexName) == 0) {
        found = mappings.get(i);
        break;
      }
    }

    if (found == null) {
      return false;
    }

    if (mappings.size() > 1) {
      mappings.remove(found);
    } else {
      // otherwise just remove the mapping
      adapterCache.remove(internalAdapterId);
    }

    return true;
  }
}
