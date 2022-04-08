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
import java.util.NoSuchElementException;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.IndexUtils;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.exceptions.AdapterException;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexRetrieval;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class NativeEntryIteratorWrapper<T> implements Iterator<T> {
  private final byte[] fieldSubsetBitmask;
  private final boolean decodePersistenceEncoding;
  private Integer bitPosition = null;
  private ByteArray skipUntilRow;
  private boolean reachedEnd = false;
  private boolean adapterValid = true;
  protected final DataIndexRetrieval dataIndexRetrieval;
  protected final PersistentAdapterStore adapterStore;
  protected final AdapterIndexMappingStore mappingStore;
  protected final Index index;
  protected final Iterator<GeoWaveRow> scannerIt;
  protected final QueryFilter[] clientFilters;
  protected final ScanCallback<T, ? extends GeoWaveRow> scanCallback;

  protected T nextValue;

  public NativeEntryIteratorWrapper(
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final Index index,
      final Iterator<GeoWaveRow> scannerIt,
      final QueryFilter[] clientFilters,
      final ScanCallback<T, ? extends GeoWaveRow> scanCallback,
      final byte[] fieldSubsetBitmask,
      final double[] maxResolutionSubsamplingPerDimension,
      final boolean decodePersistenceEncoding,
      final DataIndexRetrieval dataIndexRetrieval) {
    this.adapterStore = adapterStore;
    this.mappingStore = mappingStore;
    this.index = index;
    this.scannerIt = scannerIt;
    this.clientFilters = clientFilters;
    this.scanCallback = scanCallback;
    this.fieldSubsetBitmask = fieldSubsetBitmask;
    this.decodePersistenceEncoding = decodePersistenceEncoding;
    this.dataIndexRetrieval = dataIndexRetrieval;
    initializeBitPosition(maxResolutionSubsamplingPerDimension);
  }


  protected void findNext() {
    while ((nextValue == null) && hasNextScannedResult()) {
      final GeoWaveRow row = getNextEncodedResult();
      final T decodedValue = decodeRow(row, clientFilters, index);
      if (decodedValue != null) {
        nextValue = decodedValue;
        return;
      }
    }
  }

  protected boolean hasNextScannedResult() {
    return scannerIt.hasNext();
  }

  protected GeoWaveRow getNextEncodedResult() {
    return scannerIt.next();
  }

  @Override
  public boolean hasNext() {
    findNext();
    return nextValue != null;
  }

  @Override
  public T next() throws NoSuchElementException {
    if (nextValue == null) {
      findNext();
    }
    final T previousNext = nextValue;
    if (nextValue == null) {
      throw new NoSuchElementException();
    }
    nextValue = null;
    return previousNext;
  }

  @Override
  public void remove() {
    scannerIt.remove();
  }

  @SuppressWarnings("unchecked")
  protected T decodeRow(
      final GeoWaveRow row,
      final QueryFilter[] clientFilters,
      final Index index) {
    Object decodedRow = null;
    if (adapterValid && ((bitPosition == null) || passesSkipFilter(row))) {
      try {
        decodedRow =
            BaseDataStoreUtils.decodeRow(
                row,
                clientFilters,
                null,
                null,
                adapterStore,
                mappingStore,
                index,
                scanCallback,
                fieldSubsetBitmask,
                decodePersistenceEncoding,
                dataIndexRetrieval);

        if (decodedRow != null) {
          incrementSkipRow(row);
        }
      } catch (final AdapterException e) {
        adapterValid = false;
        // Attempting to decode future rows with the same adapter is
        // pointless.
      }
    }
    return (T) decodedRow;
  }

  boolean first = false;

  private boolean passesSkipFilter(final GeoWaveRow row) {
    if ((reachedEnd == true)
        || ((skipUntilRow != null)
            && ((skipUntilRow.compareTo(new ByteArray(row.getSortKey()))) > 0))) {
      return false;
    }

    return true;
  }

  private void incrementSkipRow(final GeoWaveRow row) {
    if (bitPosition != null) {
      final byte[] nextRow = IndexUtils.getNextRowForSkip(row.getSortKey(), bitPosition);
      if (nextRow == null) {
        reachedEnd = true;
      } else {
        skipUntilRow = new ByteArray(nextRow);
      }
    }
  }

  private void initializeBitPosition(final double[] maxResolutionSubsamplingPerDimension) {
    if ((maxResolutionSubsamplingPerDimension != null)
        && (maxResolutionSubsamplingPerDimension.length > 0)) {
      bitPosition =
          IndexUtils.getBitPositionOnSortKeyFromSubsamplingArray(
              index.getIndexStrategy(),
              maxResolutionSubsamplingPerDimension);
    }
  }
}
