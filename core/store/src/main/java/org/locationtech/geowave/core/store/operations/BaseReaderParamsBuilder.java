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

public abstract class BaseReaderParamsBuilder<T, R extends BaseReaderParamsBuilder<T, R>> {
  protected final PersistentAdapterStore adapterStore;
  protected final AdapterIndexMappingStore mappingStore;
  protected final InternalAdapterStore internalAdapterStore;
  protected Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation = null;
  protected Pair<String[], InternalDataAdapter<?>> fieldSubsets = null;
  protected boolean isAuthorizationsLimiting = true;
  protected String[] additionalAuthorizations;

  public BaseReaderParamsBuilder(
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final InternalAdapterStore internalAdapterStore) {
    this.adapterStore = adapterStore;
    this.mappingStore = mappingStore;
    this.internalAdapterStore = internalAdapterStore;
  }

  protected abstract R builder();

  public R aggregation(final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation) {
    this.aggregation = aggregation;
    return builder();
  }

  public R fieldSubsets(final Pair<String[], InternalDataAdapter<?>> fieldSubsets) {
    this.fieldSubsets = fieldSubsets;
    return builder();
  }

  public R additionalAuthorizations(final String... authorizations) {
    this.additionalAuthorizations = authorizations;
    return builder();
  }

  public R isAuthorizationsLimiting(final boolean isAuthorizationsLimiting) {
    this.isAuthorizationsLimiting = isAuthorizationsLimiting;
    return builder();
  }
}
