/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p>See the NOTICE file distributed with this work for additional information regarding copyright
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
  public AdapterToIndexMapping getIndicesForAdapter(short internalAdapterId);

  /**
   * If an adapter is already associated with indices and the provided indices do not match, update
   * the mapping to include the combined set of indices
   *
   * @param adapter
   * @param indices
   */
  public void addAdapterIndexMapping(AdapterToIndexMapping mapping);

  /**
   * Adapter to index mappings are maintain without regard to visibility constraints.
   *
   * @param adapterId
   */
  public void remove(short adapterId);

  /**
   * Remove an index for the specified adapter mapping. The method should return false if the
   * adapter, or index for the adapter does not exist.
   *
   * @param adapterId
   * @param indexName
   */
  public boolean remove(short adapterId, String indexName);

  public void removeAll();
}
