/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import org.locationtech.geowave.core.store.AdapterToIndexMapping;

/**
 * This is responsible for persisting adapter/index mappings (either in memory or to disk depending
 * on the implementation).
 */
public interface AdapterIndexMappingStore {
  /**
   * Returns the indices associated with the given adapter.
   * 
   * @param internalAdapterId the internal adapter ID of the adapter
   * @return the adapter to index mapping
   */
  public AdapterToIndexMapping[] getIndicesForAdapter(short internalAdapterId);

  public AdapterToIndexMapping getMapping(short adapterId, String indexName);

  /**
   * If an adapter is already associated with indices and the provided indices do not match, update
   * the mapping to include the combined set of indices.
   *
   * @param mapping the mapping to add
   */
  public void addAdapterIndexMapping(AdapterToIndexMapping mapping);

  /**
   * Remove the given adapter from the mapping store.
   *
   * @param adapterId the internal adapter ID of the adapter
   */
  public void remove(short adapterId);

  /**
   * Remove an index for the specified adapter mapping. The method should return false if the
   * adapter, or index for the adapter does not exist.
   *
   * @param adapterId the internal adapter ID of the adapter
   * @param indexName the name of the index
   */
  public boolean remove(short adapterId, String indexName);

  /**
   * Remove all mappings from the store.
   */
  public void removeAll();
}
