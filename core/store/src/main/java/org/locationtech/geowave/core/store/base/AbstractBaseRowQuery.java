/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.base;

import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexRetrieval;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.statistics.index.DifferingVisibilityCountStatistic.DifferingVisibilityCountValue;
import org.locationtech.geowave.core.store.statistics.index.FieldVisibilityCountStatistic.FieldVisibilityCountValue;
import org.locationtech.geowave.core.store.util.NativeEntryTransformer;

/**
 * Represents a query operation by an Accumulo row. This abstraction is re-usable for both exact row
 * ID queries and row prefix queries.
 */
abstract class AbstractBaseRowQuery<T> extends BaseQuery {

  public AbstractBaseRowQuery(
      final Index index,
      final String[] authorizations,
      final ScanCallback<T, ?> scanCallback,
      final DifferingVisibilityCountValue differingVisibilityCounts,
      final FieldVisibilityCountValue visibilityCounts,
      final DataIndexRetrieval dataIndexRetrieval) {
    super(
        index,
        scanCallback,
        differingVisibilityCounts,
        visibilityCounts,
        dataIndexRetrieval,
        authorizations);
  }

  public CloseableIterator<T> query(
      final DataStoreOperations operations,
      final DataStoreOptions options,
      final double[] maxResolutionSubsamplingPerDimension,
      final double[] targetResolutionPerDimensionForHierarchicalIndex,
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final InternalAdapterStore internalAdapterStore,
      final Integer limit,
      final Integer queryMaxRangeDecomposition,
      final boolean delete) {
    final RowReader<T> reader =
        getReader(
            operations,
            options,
            adapterStore,
            mappingStore,
            internalAdapterStore,
            maxResolutionSubsamplingPerDimension,
            targetResolutionPerDimensionForHierarchicalIndex,
            limit,
            queryMaxRangeDecomposition,
            new NativeEntryTransformer<>(
                adapterStore,
                mappingStore,
                index,
                getClientFilters(options),
                (ScanCallback<T, ?>) scanCallback,
                getFieldBitmask(),
                maxResolutionSubsamplingPerDimension,
                !isCommonIndexAggregation(),
                getDataIndexRetrieval()),
            delete);
    return reader;
  }
}
