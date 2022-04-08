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
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class ReaderParams<T> extends RangeReaderParams<T> {
  private final boolean isServersideAggregation;
  private final QueryRanges queryRanges;
  private final QueryFilter filter;
  private final List<MultiDimensionalCoordinateRangesArray> coordinateRanges;
  private final List<MultiDimensionalNumericData> constraints;
  private final GeoWaveRowIteratorTransformer<T> rowTransformer;

  public ReaderParams(
      final Index index,
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final InternalAdapterStore internalAdapterStore,
      final short[] adapterIds,
      final double[] maxResolutionSubsamplingPerDimension,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
      final boolean isMixedVisibility,
      final boolean isAuthorizationsLimiting,
      final boolean isServersideAggregation,
      final boolean isClientsideRowMerging,
      final QueryRanges queryRanges,
      final QueryFilter filter,
      final Integer limit,
      final Integer maxRangeDecomposition,
      final List<MultiDimensionalCoordinateRangesArray> coordinateRanges,
      final List<MultiDimensionalNumericData> constraints,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final String[] additionalAuthorizations) {
    super(
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
        isClientsideRowMerging,
        limit,
        maxRangeDecomposition,
        additionalAuthorizations);
    this.isServersideAggregation = isServersideAggregation;
    this.queryRanges = queryRanges;
    this.filter = filter;
    this.coordinateRanges = coordinateRanges;
    this.constraints = constraints;
    this.rowTransformer = rowTransformer;
  }

  public List<MultiDimensionalCoordinateRangesArray> getCoordinateRanges() {
    return coordinateRanges;
  }

  public List<MultiDimensionalNumericData> getConstraints() {
    return constraints;
  }

  public boolean isServersideAggregation() {
    return isServersideAggregation;
  }

  public QueryRanges getQueryRanges() {
    return queryRanges;
  }

  public QueryFilter getFilter() {
    return filter;
  }

  public GeoWaveRowIteratorTransformer<T> getRowTransformer() {
    return rowTransformer;
  }
}
