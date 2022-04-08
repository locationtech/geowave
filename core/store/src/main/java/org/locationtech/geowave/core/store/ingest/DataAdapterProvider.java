/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.ingest;

import org.locationtech.geowave.core.store.api.DataTypeAdapter;

/**
 * This interface is applicable for plugins that need to provide writable data adapters for ingest.
 *
 * @param <T> the java type for the data being ingested
 */
public interface DataAdapterProvider<T> {
  /**
   * Get all writable adapters used by this plugin
   * 
   * @return An array of adapters that may be used by this plugin
   */
  public DataTypeAdapter<T>[] getDataAdapters();

  /**
   * return a set of index types that can be indexed by this data adapter provider, used for
   * compatibility checking with an index provider
   *
   * @return the named dimensions that are indexable by this adapter provider
   */
  public String[] getSupportedIndexTypes();
}
