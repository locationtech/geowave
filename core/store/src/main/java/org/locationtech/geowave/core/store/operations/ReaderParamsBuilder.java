/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.operations;

import java.util.List;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class ReaderParamsBuilder<T> extends RangeReaderParamsBuilder<T, ReaderParamsBuilder<T>> {

  protected boolean isServersideAggregation = false;
  protected QueryRanges queryRanges = null;
  protected QueryFilter filter = null;
  protected List<MultiDimensionalCoordinateRangesArray> coordinateRanges = null;
  protected List<MultiDimensionalNumericData> constraints = null;
  protected GeoWaveRowIteratorTransformer<T> rowTransformer;

  public ReaderParamsBuilder(
      final Index index,
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final InternalAdapterStore internalAdapterStore,
      final GeoWaveRowIteratorTransformer<T> rowTransformer) {
    super(index, adapterStore, mappingStore, internalAdapterStore);
    this.rowTransformer = rowTransformer;
  }

  @Override
  protected ReaderParamsBuilder<T> builder() {
    return this;
  }

  public ReaderParamsBuilder<T> isServersideAggregation(final boolean isServersideAggregation) {
    this.isServersideAggregation = isServersideAggregation;
    return builder();
  }

  public ReaderParamsBuilder<T> queryRanges(final QueryRanges queryRanges) {
    this.queryRanges = queryRanges;
    return builder();
  }

  public ReaderParamsBuilder<T> filter(final QueryFilter filter) {
    this.filter = filter;
    return builder();
  }

  public ReaderParamsBuilder<T> coordinateRanges(
      final List<MultiDimensionalCoordinateRangesArray> coordinateRanges) {
    this.coordinateRanges = coordinateRanges;
    return builder();
  }

  public ReaderParamsBuilder<T> constraints(final List<MultiDimensionalNumericData> constraints) {
    this.constraints = constraints;
    return builder();
  }

  public GeoWaveRowIteratorTransformer<T> getRowTransformer() {
    return rowTransformer;
  }

  public ReaderParams<T> build() {
    if (queryRanges == null) {
      queryRanges = new QueryRanges();
    }
    if (additionalAuthorizations == null) {
      additionalAuthorizations = new String[0];
    }
    return new ReaderParams<>(
        index,
        adapterStore,
        mappingStore,
        internalAdapterStore,
        adapterIds,
        maxResolutionSubsamplingPerDimension,
        aggregation,
        fieldSubsets,
        isMixedVisibility,
        isAuthorizationsLimiting,
        isServersideAggregation,
        isClientsideRowMerging,
        queryRanges,
        filter,
        limit,
        maxRangeDecomposition,
        coordinateRanges,
        constraints,
        rowTransformer,
        additionalAuthorizations);
  }
}
