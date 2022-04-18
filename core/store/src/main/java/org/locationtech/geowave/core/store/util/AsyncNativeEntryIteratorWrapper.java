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
import org.locationtech.geowave.core.store.base.dataidx.BatchDataIndexRetrieval;
import org.locationtech.geowave.core.store.base.dataidx.BatchDataIndexRetrievalIteratorHelper;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class AsyncNativeEntryIteratorWrapper<T> extends NativeEntryIteratorWrapper<T> {
  private final BatchDataIndexRetrievalIteratorHelper<T, T> batchHelper;

  public AsyncNativeEntryIteratorWrapper(
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final Index index,
      final Iterator<GeoWaveRow> scannerIt,
      final QueryFilter[] clientFilters,
      final ScanCallback<T, ? extends GeoWaveRow> scanCallback,
      final byte[] fieldSubsetBitmask,
      final double[] maxResolutionSubsamplingPerDimension,
      final boolean decodePersistenceEncoding,
      final BatchDataIndexRetrieval dataIndexRetrieval) {
    super(
        adapterStore,
        mappingStore,
        index,
        scannerIt,
        clientFilters,
        scanCallback,
        fieldSubsetBitmask,
        maxResolutionSubsamplingPerDimension,
        decodePersistenceEncoding,
        dataIndexRetrieval);
    batchHelper = new BatchDataIndexRetrievalIteratorHelper<>(dataIndexRetrieval);
  }

  @Override
  protected T decodeRow(
      final GeoWaveRow row,
      final QueryFilter[] clientFilters,
      final Index index) {
    final T retVal = super.decodeRow(row, clientFilters, index);
    return batchHelper.postDecodeRow(retVal);
  }

  @Override
  public boolean hasNext() {
    batchHelper.preHasNext();
    return super.hasNext();
  }

  @Override
  protected void findNext() {
    super.findNext();
    final boolean hasNextValue = (nextValue != null);
    final T batchNextValue = batchHelper.postFindNext(hasNextValue, hasNextScannedResult());
    if (!hasNextValue) {
      nextValue = batchNextValue;
    }
  }
}
