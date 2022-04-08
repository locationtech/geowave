/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.base;

import java.util.Arrays;
import java.util.function.IntFunction;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;

/**
 * There is a single intermediate row per original entry passed into a write operation. This offers
 * a higher level abstraction from the raw key-value pairs in geowave (can be multiple per original
 * entry). A datastore is responsible for translating from this intermediary representation of rows
 * to key-value rows.
 */
class IntermediaryWriteEntryInfo {
  public static class FieldInfo<T> {
    private final String fieldName;
    private final String visibility;
    private final byte[] writtenValue;

    public FieldInfo(final String fieldName, final byte[] writtenValue, final String visibility) {
      this.fieldName = fieldName;
      this.writtenValue = writtenValue;
      this.visibility = visibility == null ? "" : visibility;
    }

    public String getFieldId() {
      return fieldName;
    }

    public byte[] getWrittenValue() {
      return writtenValue;
    }

    public String getVisibility() {
      return visibility;
    }
  }

  private final byte[] dataId;
  private final short internalAdapterId;
  private final InsertionIds insertionIds;
  private final GeoWaveValue[] entryValues;

  public IntermediaryWriteEntryInfo(
      final byte[] dataId,
      final short internalAdapterId,
      final InsertionIds insertionIds,
      final GeoWaveValue[] entryValues) {
    this.dataId = dataId;
    this.internalAdapterId = internalAdapterId;
    this.insertionIds = insertionIds;
    this.entryValues = entryValues;
  }

  @Override
  public String toString() {
    return new ByteArray(dataId).getString();
  }

  public short getInternalAdapterId() {
    return internalAdapterId;
  }

  public InsertionIds getInsertionIds() {
    return insertionIds;
  }

  public boolean isDataIdIndex() {
    return insertionIds == null;
  }

  public byte[] getDataId() {
    return dataId;
  }

  public GeoWaveValue[] getValues() {
    return entryValues;
  }

  public GeoWaveRow[] getRows() {
    if (isDataIdIndex()) {
      return new GeoWaveRow[] {
          // intentionally make the data ID as the sort Key and the data ID empty
          new GeoWaveRowImpl(
              new GeoWaveKeyImpl(dataId, internalAdapterId, new byte[0], new byte[0], 0),
              entryValues)};
    }
    final GeoWaveKey[] keys = GeoWaveKeyImpl.createKeys(insertionIds, dataId, internalAdapterId);
    return Arrays.stream(keys).map(k -> new GeoWaveRowImpl(k, entryValues)).toArray(
        new ArrayGenerator());
  }

  private static class ArrayGenerator implements IntFunction<GeoWaveRow[]> {
    @Override
    public GeoWaveRow[] apply(final int value) {
      return new GeoWaveRow[value];
    }
  }
}
