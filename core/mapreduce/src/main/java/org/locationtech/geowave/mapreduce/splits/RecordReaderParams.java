/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce.splits;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.RangeReaderParams;

public class RecordReaderParams extends RangeReaderParams<GeoWaveRow> {
  private final GeoWaveRowRange rowRange;

  public RecordReaderParams(
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
      final GeoWaveRowRange rowRange,
      final Integer limit,
      final Integer maxRangeDecomposition,
      final String... additionalAuthorizations) {
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
    this.rowRange = rowRange;
  }

  public GeoWaveRowRange getRowRange() {
    return rowRange;
  }
}
