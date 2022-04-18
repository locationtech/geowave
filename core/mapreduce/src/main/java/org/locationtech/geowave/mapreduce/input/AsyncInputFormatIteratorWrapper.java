/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce.input;

import java.util.Iterator;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.dataidx.BatchDataIndexRetrieval;
import org.locationtech.geowave.core.store.base.dataidx.BatchDataIndexRetrievalIteratorHelper;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class AsyncInputFormatIteratorWrapper<T> extends InputFormatIteratorWrapper<T> {
  private final BatchDataIndexRetrievalIteratorHelper<T, Pair<GeoWaveInputKey, T>> batchHelper;

  public AsyncInputFormatIteratorWrapper(
      final Iterator<GeoWaveRow> reader,
      final QueryFilter[] queryFilters,
      final TransientAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final AdapterIndexMappingStore mappingStore,
      final Index index,
      final boolean isOutputWritable,
      final BatchDataIndexRetrieval dataIndexRetrieval) {
    super(
        reader,
        queryFilters,
        adapterStore,
        internalAdapterStore,
        mappingStore,
        index,
        isOutputWritable,
        dataIndexRetrieval);
    batchHelper = new BatchDataIndexRetrievalIteratorHelper<>(dataIndexRetrieval);
  }

  @Override
  protected void findNext() {
    super.findNext();

    final boolean hasNextValue = (nextEntry != null);
    final Pair<GeoWaveInputKey, T> batchNextValue =
        batchHelper.postFindNext(hasNextValue, reader.hasNext());
    if (!hasNextValue) {
      nextEntry = batchNextValue;
    }
  }


  @Override
  public boolean hasNext() {
    batchHelper.preHasNext();
    return super.hasNext();
  }

  @Override
  protected Pair<GeoWaveInputKey, T> decodeRowToEntry(
      final GeoWaveRow row,
      final QueryFilter[] clientFilters,
      final InternalDataAdapter<T> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    Object value = decodeRowToValue(row, clientFilters, adapter, indexMapping, index);
    if (value == null) {
      return null;
    }
    value = batchHelper.postDecodeRow((T) value, v -> valueToEntry(row, v));
    if (value == null) {
      return null;
    }
    return valueToEntry(row, value);
  }
}
