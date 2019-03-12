/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import java.util.concurrent.CompletableFuture;
import org.locationtech.geowave.core.store.base.dataidx.BatchDataIndexRetrieval;
import org.locationtech.geowave.core.store.data.MultiFieldPersistentDataset;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.index.CommonIndexValue;

/**
 * This is an implements of persistence encoding that also contains all of the extended data values
 * used to form the native type supported by this adapter. It also contains information about the
 * persisted object within a particular index such as the insertion ID in the index and the number
 * of duplicates for this entry in the index, and is used when reading data from the index.
 */
public class AsyncPersistenceEncoding extends IndexedAdapterPersistenceEncoding {
  private final BatchDataIndexRetrieval asyncRetrieval;
  private CompletableFuture<GeoWaveValue[]> fieldValuesFuture = null;

  public AsyncPersistenceEncoding(
      final short adapterId,
      final byte[] dataId,
      final byte[] partitionKey,
      final byte[] sortKey,
      final int duplicateCount,
      final BatchDataIndexRetrieval asyncRetrieval) {
    super(
        adapterId,
        dataId,
        partitionKey,
        sortKey,
        duplicateCount,
        new MultiFieldPersistentDataset<CommonIndexValue>(),
        new MultiFieldPersistentDataset<byte[]>(),
        new MultiFieldPersistentDataset<>());
    this.asyncRetrieval = asyncRetrieval;
  }

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

  @Override
  public PersistentDataset<CommonIndexValue> getCommonData() {
    // defer any reading of fieldValues until necessary
    deferredReadFields();
    return super.getCommonData();
  }

  private void deferredReadFields() {
    fieldValuesFuture = asyncRetrieval.getDataAsync(getInternalAdapterId(), getDataId());
  }
}
