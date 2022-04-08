/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.util;

import java.util.Iterator;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexRetrieval;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class NativeEntryTransformer<T> implements GeoWaveRowIteratorTransformer<T> {
  private final PersistentAdapterStore adapterStore;
  private final AdapterIndexMappingStore mappingStore;
  private final Index index;
  private final QueryFilter[] clientFilters;
  private final ScanCallback<T, ? extends GeoWaveRow> scanCallback;
  private final byte[] fieldSubsetBitmask;
  private final double[] maxResolutionSubsamplingPerDimension;
  private final boolean decodePersistenceEncoding;
  private final DataIndexRetrieval dataIndexRetrieval;

  public NativeEntryTransformer(
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final Index index,
      final QueryFilter[] clientFilters,
      final ScanCallback<T, ? extends GeoWaveRow> scanCallback,
      final byte[] fieldSubsetBitmask,
      final double[] maxResolutionSubsamplingPerDimension,
      final boolean decodePersistenceEncoding,
      final DataIndexRetrieval dataIndexRetrieval) {
    this.adapterStore = adapterStore;
    this.mappingStore = mappingStore;
    this.index = index;
    this.clientFilters = clientFilters;
    this.scanCallback = scanCallback;
    this.fieldSubsetBitmask = fieldSubsetBitmask;
    this.decodePersistenceEncoding = decodePersistenceEncoding;
    this.maxResolutionSubsamplingPerDimension = maxResolutionSubsamplingPerDimension;
    this.dataIndexRetrieval = dataIndexRetrieval;
  }

  @Override
  public Iterator<T> apply(final Iterator<GeoWaveRow> rowIter) {
    return GeoWaveRowIteratorFactory.iterator(
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
