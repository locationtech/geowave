/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.ingest;

import java.net.URL;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

/**
 * An interface required for ingest plugins to implement a conversion from an expected input format
 * to GeoWave data which can in turn be ingested into the system.
 *
 * @param <I> The type for the input data
 * @param <O> The type that represents each data entry being ingested
 */
public interface IngestPluginBase<I, O> extends DataAdapterProvider<O> {

  /**
   * Get all writable adapters used by this plugin for the given URL
   *
   * @param url the URL of the data to ingest
   * @return An array of adapters that may be used by this plugin
   */
  default DataTypeAdapter<O>[] getDataAdapters(final URL url) {
    return getDataAdapters();
  }

  /**
   * Convert from an expected input format to a data format that can be directly ingested into
   * GeoWave
   *
   * @param input The expected input.
   * @param indexNames The set of index IDs specified via a commandline argument (this is typically
   *        either the default spatial index or default spatial-temporal index)
   * @return The objects that can be directly ingested into GeoWave
   */
  CloseableIterator<GeoWaveData<O>> toGeoWaveData(I input, String[] indexNames);
}
