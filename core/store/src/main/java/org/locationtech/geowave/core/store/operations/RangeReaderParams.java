/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.operations;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.Index;

public abstract class RangeReaderParams<T> extends BaseReaderParams<T> {
  private final Index index;
  private final short[] adapterIds;
  private final double[] maxResolutionSubsamplingPerDimension;
  private final boolean isMixedVisibility;
  private final boolean isClientsideRowMerging;
  private final Integer limit;
  private final Integer maxRangeDecomposition;

  public RangeReaderParams(
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
      final boolean isClientsideRowMerging,
      final Integer limit,
      final Integer maxRangeDecomposition,
      final String[] additionalAuthorizations) {
    super(
        adapterStore,
        mappingStore,
        internalAdapterStore,
        aggregation,
        fieldSubsets,
        isAuthorizationsLimiting,
        additionalAuthorizations);
    this.index = index;
    this.adapterIds = adapterIds;
    this.maxResolutionSubsamplingPerDimension = maxResolutionSubsamplingPerDimension;
    this.isMixedVisibility = isMixedVisibility;
    this.isClientsideRowMerging = isClientsideRowMerging;
    this.limit = limit;
    this.maxRangeDecomposition = maxRangeDecomposition;
  }

  public Index getIndex() {
    return index;
  }

  public short[] getAdapterIds() {
    return adapterIds;
  }

  public double[] getMaxResolutionSubsamplingPerDimension() {
    return maxResolutionSubsamplingPerDimension;
  }

  public boolean isMixedVisibility() {
    return isMixedVisibility;
  }

  public Integer getLimit() {
    return limit;
  }

  public Integer getMaxRangeDecomposition() {
    return maxRangeDecomposition;
  }

  public boolean isClientsideRowMerging() {
    return isClientsideRowMerging;
  }
}
