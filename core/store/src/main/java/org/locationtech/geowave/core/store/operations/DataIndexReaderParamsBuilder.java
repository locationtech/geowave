/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.operations;

import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;

public class DataIndexReaderParamsBuilder<T> extends
    BaseReaderParamsBuilder<T, DataIndexReaderParamsBuilder<T>> {

  protected byte[][] dataIds = null;
  protected short adapterId;

  public DataIndexReaderParamsBuilder(
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore) {
    super(adapterStore, internalAdapterStore);
  }

  @Override
  protected DataIndexReaderParamsBuilder<T> builder() {
    return this;
  }

  public DataIndexReaderParamsBuilder<T> dataIds(final byte[]... dataIds) {
    this.dataIds = dataIds;
    return builder();
  }

  public DataIndexReaderParamsBuilder<T> adapterId(final short adapterId) {
    this.adapterId = adapterId;
    return builder();
  }

  public DataIndexReaderParams build() {
    if (dataIds == null) {
      dataIds = new byte[0][];
    }
    return new DataIndexReaderParams(
        adapterStore,
        internalAdapterStore,
        adapterId,
        aggregation,
        fieldSubsets,
        dataIds,
        isAuthorizationsLimiting,
        additionalAuthorizations);
  }
}
