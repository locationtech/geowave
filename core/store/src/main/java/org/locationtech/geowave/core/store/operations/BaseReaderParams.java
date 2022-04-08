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

public abstract class BaseReaderParams<T> {
  private final PersistentAdapterStore adapterStore;
  private final AdapterIndexMappingStore mappingStore;
  private final InternalAdapterStore internalAdapterStore;
  private final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation;
  private final Pair<String[], InternalDataAdapter<?>> fieldSubsets;
  private final boolean isAuthorizationsLimiting;
  private final String[] additionalAuthorizations;

  public BaseReaderParams(
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final InternalAdapterStore internalAdapterStore,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
      final boolean isAuthorizationsLimiting,
      final String[] additionalAuthorizations) {
    this.adapterStore = adapterStore;
    this.mappingStore = mappingStore;
    this.internalAdapterStore = internalAdapterStore;
    this.aggregation = aggregation;
    this.fieldSubsets = fieldSubsets;
    this.isAuthorizationsLimiting = isAuthorizationsLimiting;
    this.additionalAuthorizations = additionalAuthorizations;
  }


  public PersistentAdapterStore getAdapterStore() {
    return adapterStore;
  }

  public AdapterIndexMappingStore getAdapterIndexMappingStore() {
    return mappingStore;
  }

  public InternalAdapterStore getInternalAdapterStore() {
    return internalAdapterStore;
  }

  public Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> getAggregation() {
    return aggregation;
  }

  public Pair<String[], InternalDataAdapter<?>> getFieldSubsets() {
    return fieldSubsets;
  }

  public boolean isAggregation() {
    return ((aggregation != null) && (aggregation.getRight() != null));
  }

  public boolean isAuthorizationsLimiting() {
    return isAuthorizationsLimiting;
  }

  public String[] getAdditionalAuthorizations() {
    return additionalAuthorizations;
  }
}
