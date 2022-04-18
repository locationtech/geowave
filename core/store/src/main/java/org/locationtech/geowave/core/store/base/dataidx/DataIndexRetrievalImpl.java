/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.base.dataidx;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;

public class DataIndexRetrievalImpl implements DataIndexRetrieval {

  private final DataStoreOperations operations;
  private final PersistentAdapterStore adapterStore;
  private final AdapterIndexMappingStore mappingStore;
  private final InternalAdapterStore internalAdapterStore;
  private final Pair<String[], InternalDataAdapter<?>> fieldSubsets;
  private final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation;
  private final String[] additionalAuthorizations;


  public DataIndexRetrievalImpl(
      final DataStoreOperations operations,
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final InternalAdapterStore internalAdapterStore,
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
      final String[] additionalAuthorizations) {
    this.operations = operations;
    this.adapterStore = adapterStore;
    this.mappingStore = mappingStore;
    this.internalAdapterStore = internalAdapterStore;
    this.fieldSubsets = fieldSubsets;
    this.aggregation = aggregation;
    this.additionalAuthorizations = additionalAuthorizations;
  }

  @Override
  public GeoWaveValue[] getData(final short adapterId, final byte[] dataId) {
    return DataIndexUtils.getFieldValuesFromDataIdIndex(
        operations,
        adapterStore,
        mappingStore,
        internalAdapterStore,
        fieldSubsets,
        aggregation,
        additionalAuthorizations,
        adapterId,
        dataId);
  }
}
