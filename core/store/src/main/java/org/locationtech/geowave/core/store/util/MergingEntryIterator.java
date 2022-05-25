/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexRetrieval;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergingEntryIterator<T> extends NativeEntryIteratorWrapper<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(NativeEntryIteratorWrapper.class);

  private final Map<Short, RowMergingDataAdapter> mergingAdapters;
  private final Map<Short, RowTransform> transforms;

  public MergingEntryIterator(
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final Index index,
      final Iterator<GeoWaveRow> scannerIt,
      final QueryFilter[] clientFilters,
      final ScanCallback<T, GeoWaveRow> scanCallback,
      final Map<Short, RowMergingDataAdapter> mergingAdapters,
      final double[] maxResolutionSubsamplingPerDimension,
      final DataIndexRetrieval dataIndexRetrieval) {
    super(
        adapterStore,
        mappingStore,
        index,
        scannerIt,
        clientFilters,
        scanCallback,
        null,
        maxResolutionSubsamplingPerDimension,
        true,
        dataIndexRetrieval);
    this.mergingAdapters = mergingAdapters;
    transforms = new HashMap<>();
  }

  @Override
  protected GeoWaveRow getNextEncodedResult() {
    GeoWaveRow nextResult = scannerIt.next();

    final short internalAdapterId = nextResult.getAdapterId();

    final RowMergingDataAdapter mergingAdapter = mergingAdapters.get(internalAdapterId);

    if ((mergingAdapter != null) && (mergingAdapter.getTransform() != null)) {
      final RowTransform rowTransform = getRowTransform(internalAdapterId, mergingAdapter);

      // This iterator expects a single GeoWaveRow w/ multiple fieldValues
      nextResult = mergeSingleRowValues(nextResult, rowTransform);
    }

    return nextResult;
  }

  private RowTransform getRowTransform(
      final short internalAdapterId,
      final RowMergingDataAdapter mergingAdapter) {
    RowTransform transform = transforms.get(internalAdapterId);
    if (transform == null) {
      transform = mergingAdapter.getTransform();
      // set strategy
      try {
        transform.initOptions(mergingAdapter.getOptions(internalAdapterId, null));
      } catch (final IOException e) {
        LOGGER.error(
            "Unable to initialize merge strategy for adapter: " + mergingAdapter.getTypeName(),
            e);
      }
      transforms.put(internalAdapterId, transform);
    }

    return transform;
  }

  protected GeoWaveRow mergeSingleRowValues(
      final GeoWaveRow singleRow,
      final RowTransform rowTransform) {
    return DataStoreUtils.mergeSingleRowValues(singleRow, rowTransform);
  }

  @Override
  protected boolean hasNextScannedResult() {
    return scannerIt.hasNext();
  }

  @Override
  public void remove() {
    throw new NotImplementedException("Transforming iterator cannot use remove()");
  }
}
