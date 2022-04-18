/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.base.dataidx.BatchDataIndexRetrieval;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

/**
 * /** This is an implementation of persistence encoding that retrieves all of the extended data
 * values asynchronously but is supplied the common index values
 */
public class PartialAsyncPersistenceEncoding extends LazyReadPersistenceEncoding implements
    AsyncPersistenceEncoding {
  private final BatchDataIndexRetrieval asyncRetrieval;
  private CompletableFuture<GeoWaveValue[]> fieldValuesFuture = null;

  public PartialAsyncPersistenceEncoding(
      final short adapterId,
      final byte[] dataId,
      final byte[] partitionKey,
      final byte[] sortKey,
      final int duplicateCount,
      final BatchDataIndexRetrieval asyncRetrieval,
      final InternalDataAdapter<?> dataAdapter,
      final CommonIndexModel indexModel,
      final AdapterToIndexMapping indexMapping,
      final byte[] fieldSubsetBitmask,
      final Supplier<GeoWaveValue[]> fieldValues) {
    super(
        adapterId,
        dataId,
        partitionKey,
        sortKey,
        duplicateCount,
        dataAdapter,
        indexModel,
        indexMapping,
        fieldSubsetBitmask,
        fieldValues);
    this.asyncRetrieval = asyncRetrieval;
  }

  @Override
  public CompletableFuture<GeoWaveValue[]> getFieldValuesFuture() {
    return fieldValuesFuture;
  }

  @Override
  public boolean isAsync() {
    return fieldValuesFuture != null;
  }

  @Override
  public PersistentDataset<Object> getAdapterExtendedData() {
    // defer any reading of fieldValues until necessary
    deferredReadFields();
    return super.getAdapterExtendedData();
  }

  @Override
  public PersistentDataset<byte[]> getUnknownData() {
    // defer any reading of fieldValues until necessary
    deferredReadFields();
    return super.getUnknownData();
  }

  private void deferredReadFields() {
    fieldValuesFuture = asyncRetrieval.getDataAsync(getInternalAdapterId(), getDataId());
  }
}
