/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.util;

import java.util.Iterator;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.dataidx.BatchDataIndexRetrieval;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexRetrieval;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class GeoWaveRowIteratorFactory {
  public static <T> Iterator<T> iterator(
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final Index index,
      final Iterator<GeoWaveRow> rowIter,
      final QueryFilter[] clientFilters,
      final ScanCallback<T, ? extends GeoWaveRow> scanCallback,
      final byte[] fieldSubsetBitmask,
      final double[] maxResolutionSubsamplingPerDimension,
      final boolean decodePersistenceEncoding,
      final DataIndexRetrieval dataIndexRetrieval) {
    if (dataIndexRetrieval instanceof BatchDataIndexRetrieval) {
      return new AsyncNativeEntryIteratorWrapper<>(
          adapterStore,
          mappingStore,
          index,
          rowIter,
          clientFilters,
          scanCallback,
          fieldSubsetBitmask,
          maxResolutionSubsamplingPerDimension,
          decodePersistenceEncoding,
          (BatchDataIndexRetrieval) dataIndexRetrieval);
    }
    return new NativeEntryIteratorWrapper<>(
        adapterStore,
        mappingStore,
        index,
        rowIter,
        clientFilters,
        scanCallback,
        fieldSubsetBitmask,
        maxResolutionSubsamplingPerDimension,
        decodePersistenceEncoding,
        dataIndexRetrieval);
  }
}
