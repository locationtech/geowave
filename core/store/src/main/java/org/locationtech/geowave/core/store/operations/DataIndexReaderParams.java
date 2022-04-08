/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
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
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

public class DataIndexReaderParams extends BaseReaderParams<GeoWaveRow> {
  private byte[][] dataIds;
  private byte[] startInclusiveDataId;
  private byte[] endInclusiveDataId;
  private boolean reverse = false;
  private final short adapterId;

  public DataIndexReaderParams(
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final InternalAdapterStore internalAdapterStore,
      final short adapterId,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
      final byte[][] dataIds,
      final boolean isAuthorizationsLimiting,
      final String[] additionalAuthorizations) {
    super(
        adapterStore,
        mappingStore,
        internalAdapterStore,
        aggregation,
        fieldSubsets,
        isAuthorizationsLimiting,
        additionalAuthorizations);
    this.dataIds = dataIds;
    this.adapterId = adapterId;
  }

  public DataIndexReaderParams(
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final InternalAdapterStore internalAdapterStore,
      final short adapterId,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
      final byte[] startInclusiveDataId,
      final byte[] endInclusiveDataId,
      final boolean reverse,
      final boolean isAuthorizationsLimiting,
      final String[] additionalAuthorizations) {
    super(
        adapterStore,
        mappingStore,
        internalAdapterStore,
        aggregation,
        fieldSubsets,
        isAuthorizationsLimiting,
        additionalAuthorizations);
    this.startInclusiveDataId = startInclusiveDataId;
    this.endInclusiveDataId = endInclusiveDataId;

    this.reverse = reverse;
    this.adapterId = adapterId;
  }

  public byte[][] getDataIds() {
    return dataIds;
  }

  public short getAdapterId() {
    return adapterId;
  }

  public byte[] getStartInclusiveDataId() {
    return startInclusiveDataId;
  }

  public byte[] getEndInclusiveDataId() {
    return endInclusiveDataId;
  }

  public boolean isReverse() {
    return reverse;
  }
}
