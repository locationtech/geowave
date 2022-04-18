/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce.input;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.adapter.exceptions.AdapterException;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexRetrieval;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.locationtech.geowave.mapreduce.HadoopWritableSerializationTool;
import com.beust.jcommander.internal.Maps;

/**
 * This is used internally to translate GeoWave rows into native objects (using the appropriate data
 * adapter). It also performs any client-side filtering. It will peek at the next entry in the
 * underlying datastore iterator to always maintain a reference to the next value.
 *
 * @param <T> The type for the entry
 */
public class InputFormatIteratorWrapper<T> implements Iterator<Pair<GeoWaveInputKey, T>> {
  protected final Iterator<GeoWaveRow> reader;
  private final QueryFilter[] queryFilters;
  private final HadoopWritableSerializationTool serializationTool;
  private final boolean isOutputWritable;
  protected Pair<GeoWaveInputKey, T> nextEntry;
  private final Index index;
  private final DataIndexRetrieval dataIndexRetrieval;
  private final AdapterIndexMappingStore mappingStore;
  private final Map<Short, AdapterToIndexMapping> indexMappings;

  public InputFormatIteratorWrapper(
      final Iterator<GeoWaveRow> reader,
      final QueryFilter[] queryFilters,
      final TransientAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final AdapterIndexMappingStore mappingStore,
      final Index index,
      final boolean isOutputWritable,
      final DataIndexRetrieval dataIndexRetrieval) {
    this.reader = reader;
    this.queryFilters = queryFilters;
    this.index = index;
    this.serializationTool =
        new HadoopWritableSerializationTool(adapterStore, internalAdapterStore);
    this.isOutputWritable = isOutputWritable;
    this.dataIndexRetrieval = dataIndexRetrieval;
    this.mappingStore = mappingStore;
    this.indexMappings = Maps.newHashMap();
  }

  protected void findNext() {
    while ((this.nextEntry == null) && reader.hasNext()) {
      final GeoWaveRow nextRow = reader.next();
      if (nextRow != null) {
        if (!indexMappings.containsKey(nextRow.getAdapterId())) {
          indexMappings.put(
              nextRow.getAdapterId(),
              mappingStore.getMapping(nextRow.getAdapterId(), index.getName()));
        }
        final Pair<GeoWaveInputKey, T> decodedValue =
            decodeRowToEntry(
                nextRow,
                queryFilters,
                (InternalDataAdapter<T>) serializationTool.getInternalAdapter(
                    nextRow.getAdapterId()),
                indexMappings.get(nextRow.getAdapterId()),
                index);
        if (decodedValue != null) {
          nextEntry = decodedValue;
          return;
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  protected Object decodeRowToValue(
      final GeoWaveRow row,
      final QueryFilter[] clientFilters,
      final InternalDataAdapter<T> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    Object value = null;
    try {
      value =
          BaseDataStoreUtils.decodeRow(
              row,
              clientFilters,
              adapter,
              indexMapping,
              null,
              null,
              index,
              null,
              null,
              true,
              dataIndexRetrieval);
    } catch (final AdapterException e) {
      return null;
    }
    if (value == null) {
      return null;
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  protected Pair<GeoWaveInputKey, T> decodeRowToEntry(
      final GeoWaveRow row,
      final QueryFilter[] clientFilters,
      final InternalDataAdapter<T> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    final Object value = decodeRowToValue(row, clientFilters, adapter, indexMapping, index);
    if (value == null) {
      return null;
    }
    return valueToEntry(row, value);
  }

  protected Pair<GeoWaveInputKey, T> valueToEntry(final GeoWaveRow row, final Object value) {
    final short adapterId = row.getAdapterId();
    final T result =
        (T) (isOutputWritable
            ? serializationTool.getHadoopWritableSerializerForAdapter(adapterId).toWritable(value)
            : value);
    final GeoWaveInputKey key = new GeoWaveInputKey(row, index.getName());
    return Pair.of(key, result);
  }

  @Override
  public boolean hasNext() {
    findNext();
    return nextEntry != null;
  }

  @Override
  public Pair<GeoWaveInputKey, T> next() throws NoSuchElementException {
    final Pair<GeoWaveInputKey, T> previousNext = nextEntry;
    if (nextEntry == null) {
      throw new NoSuchElementException();
    }
    nextEntry = null;
    return previousNext;
  }

  @Override
  public void remove() {
    reader.remove();
  }
}
