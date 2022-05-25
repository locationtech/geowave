/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.base;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexRetrieval;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.locationtech.geowave.core.store.statistics.index.DifferingVisibilityCountStatistic.DifferingVisibilityCountValue;
import org.locationtech.geowave.core.store.statistics.index.FieldVisibilityCountStatistic.FieldVisibilityCountValue;
import org.locationtech.geowave.core.store.util.GeoWaveRowIteratorFactory;
import org.locationtech.geowave.core.store.util.MergingEntryIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Iterators;

abstract class BaseFilteredIndexQuery extends BaseQuery {
  protected List<QueryFilter> clientFilters;
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseFilteredIndexQuery.class);

  public BaseFilteredIndexQuery(
      final short[] adapterIds,
      final Index index,
      final ScanCallback<?, ?> scanCallback,
      final Pair<String[], InternalDataAdapter<?>> fieldIdsAdapterPair,
      final DifferingVisibilityCountValue differingVisibilityCounts,
      final FieldVisibilityCountValue visibilityCounts,
      final DataIndexRetrieval dataIndexRetrieval,
      final String... authorizations) {
    super(
        adapterIds,
        index,
        fieldIdsAdapterPair,
        scanCallback,
        differingVisibilityCounts,
        visibilityCounts,
        dataIndexRetrieval,
        authorizations);
  }

  protected List<QueryFilter> getClientFilters() {
    return clientFilters;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public CloseableIterator<Object> query(
      final DataStoreOperations datastoreOperations,
      final DataStoreOptions options,
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final InternalAdapterStore internalAdapterStore,
      final double[] maxResolutionSubsamplingPerDimension,
      final double[] targetResolutionPerDimensionForHierarchicalIndex,
      final Integer limit,
      final Integer queryMaxRangeDecomposition,
      final boolean delete) {
    final RowReader<?> reader =
        getReader(
            datastoreOperations,
            options,
            adapterStore,
            mappingStore,
            internalAdapterStore,
            maxResolutionSubsamplingPerDimension,
            targetResolutionPerDimensionForHierarchicalIndex,
            limit,
            queryMaxRangeDecomposition,
            getRowTransformer(
                options,
                adapterStore,
                mappingStore,
                maxResolutionSubsamplingPerDimension,
                !isCommonIndexAggregation()),
            delete);
    if (reader == null) {
      return new CloseableIterator.Empty();
    }
    Iterator it = reader;
    if ((limit != null) && (limit > 0)) {
      it = Iterators.limit(it, limit);
    }
    return new CloseableIteratorWrapper(reader, it);
  }

  @Override
  protected <C> RowReader<C> getReader(
      final DataStoreOperations datastoreOperations,
      final DataStoreOptions options,
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final InternalAdapterStore internalAdapterStore,
      final double[] maxResolutionSubsamplingPerDimension,
      final double[] targetResolutionPerDimensionForHierarchicalIndex,
      final Integer limit,
      final Integer queryMaxRangeDecomposition,
      final GeoWaveRowIteratorTransformer<C> rowTransformer,
      final boolean delete) {
    boolean exists = false;
    try {
      exists = datastoreOperations.indexExists(index.getName());
    } catch (final IOException e) {
      LOGGER.error("Table does not exist", e);
    }
    if (!exists) {
      LOGGER.warn("Table does not exist " + index.getName());
      return null;
    }

    return super.getReader(
        datastoreOperations,
        options,
        adapterStore,
        mappingStore,
        internalAdapterStore,
        maxResolutionSubsamplingPerDimension,
        targetResolutionPerDimensionForHierarchicalIndex,
        limit,
        queryMaxRangeDecomposition,
        rowTransformer,
        delete);
  }

  protected Map<Short, RowMergingDataAdapter> getMergingAdapters(
      final PersistentAdapterStore adapterStore) {
    final Map<Short, RowMergingDataAdapter> mergingAdapters = new HashMap<>();
    for (final Short adapterId : adapterIds) {
      final DataTypeAdapter<?> adapter = adapterStore.getAdapter(adapterId).getAdapter();
      if ((adapter instanceof RowMergingDataAdapter)
          && (((RowMergingDataAdapter) adapter).getTransform() != null)) {
        mergingAdapters.put(adapterId, (RowMergingDataAdapter) adapter);
      }
    }

    return mergingAdapters;
  }

  private <T> GeoWaveRowIteratorTransformer<T> getRowTransformer(
      final DataStoreOptions options,
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final double[] maxResolutionSubsamplingPerDimension,
      final boolean decodePersistenceEncoding) {
    final @Nullable QueryFilter[] clientFilters = getClientFilters(options);
    final DataIndexRetrieval dataIndexRetrieval = getDataIndexRetrieval();
    if ((options == null) || options.requiresClientSideMerging()) {
      final Map<Short, RowMergingDataAdapter> mergingAdapters = getMergingAdapters(adapterStore);

      if (!mergingAdapters.isEmpty()) {
        return new GeoWaveRowIteratorTransformer<T>() {

          @SuppressWarnings({"rawtypes", "unchecked"})
          @Override
          public Iterator<T> apply(final Iterator<GeoWaveRow> input) {
            return new MergingEntryIterator(
                adapterStore,
                mappingStore,
                index,
                input,
                clientFilters,
                scanCallback,
                mergingAdapters,
                maxResolutionSubsamplingPerDimension,
                dataIndexRetrieval);
          }
        };
      }
    }

    return new GeoWaveRowIteratorTransformer<T>() {

      @SuppressWarnings({"rawtypes", "unchecked"})
      @Override
      public Iterator<T> apply(final Iterator<GeoWaveRow> input) {
        return (Iterator<T>) GeoWaveRowIteratorFactory.iterator(
            adapterStore,
            mappingStore,
            index,
            input,
            clientFilters,
            scanCallback,
            getFieldBitmask(),
            // Don't do client side subsampling if server side is
            // enabled.
            ((options != null) && options.isServerSideLibraryEnabled()) ? null
                : maxResolutionSubsamplingPerDimension,
            decodePersistenceEncoding,
            dataIndexRetrieval);
      }
    };
  }

  @Override
  protected QueryFilter[] getClientFilters(final DataStoreOptions options) {
    final List<QueryFilter> internalClientFilters = getClientFiltersList(options);
    return internalClientFilters.isEmpty() ? null
        : internalClientFilters.toArray(new QueryFilter[0]);
  }

  protected List<QueryFilter> getClientFiltersList(final DataStoreOptions options) {
    return clientFilters;
  }
}
