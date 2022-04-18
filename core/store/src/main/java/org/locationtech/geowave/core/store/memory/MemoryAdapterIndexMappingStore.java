/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */

package org.locationtech.geowave.core.store.memory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;

public class MemoryAdapterIndexMappingStore implements AdapterIndexMappingStore {

  private Map<Short, Map<String, AdapterToIndexMapping>> indexMappings;

  public MemoryAdapterIndexMappingStore() {
    indexMappings =
        Collections.synchronizedMap(new HashMap<Short, Map<String, AdapterToIndexMapping>>());
  }

  @Override
  public AdapterToIndexMapping[] getIndicesForAdapter(short internalAdapterId) {
    if (indexMappings.containsKey(internalAdapterId)) {
      final Collection<AdapterToIndexMapping> mappings =
          indexMappings.get(internalAdapterId).values();
      return mappings.toArray(new AdapterToIndexMapping[mappings.size()]);
    }
    return null;
  }

  @Override
  public AdapterToIndexMapping getMapping(short adapterId, String indexName) {
    if (indexMappings.containsKey(adapterId)) {
      return indexMappings.get(adapterId).get(indexName);
    }
    return null;
  }

  @Override
  public void addAdapterIndexMapping(AdapterToIndexMapping mapping) {
    if (!indexMappings.containsKey(mapping.getAdapterId())) {
      indexMappings.put(
          mapping.getAdapterId(),
          Collections.synchronizedMap(new HashMap<String, AdapterToIndexMapping>()));
    }
    indexMappings.get(mapping.getAdapterId()).put(mapping.getIndexName(), mapping);
  }

  @Override
  public void remove(short adapterId) {
    indexMappings.remove(adapterId);
  }

  @Override
  public boolean remove(short adapterId, String indexName) {
    if (indexMappings.containsKey(adapterId)) {
      return indexMappings.get(adapterId).remove(indexName) != null;
    }
    return false;
  }

  @Override
  public void removeAll() {
    indexMappings.clear();
  }

}
