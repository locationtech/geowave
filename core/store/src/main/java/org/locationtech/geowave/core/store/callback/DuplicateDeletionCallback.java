/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.callback;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.SinglePartitionInsertionIds;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.base.BaseDataStore;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.query.constraints.InsertionIdQuery;
import org.locationtech.geowave.core.store.util.DataStoreUtils;

/** This callback finds the duplicates for each scanned entry, and deletes them by insertion ID */
public class DuplicateDeletionCallback<T> implements DeleteCallback<T, GeoWaveRow>, Closeable {
  private final BaseDataStore dataStore;
  private final InternalDataAdapter<?> adapter;
  private final Index index;
  private final AdapterToIndexMapping indexMapping;
  private final Map<ByteArray, Set<InsertionIdData>> insertionIdsNotYetDeletedByDataId;

  private boolean closed = false;

  public DuplicateDeletionCallback(
      final BaseDataStore store,
      final InternalDataAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    this.adapter = adapter;
    this.index = index;
    this.indexMapping = indexMapping;
    dataStore = store;
    insertionIdsNotYetDeletedByDataId = new HashMap<>();
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    } else {
      closed = true;
    }

    for (final Map.Entry<ByteArray, Set<InsertionIdData>> entry : insertionIdsNotYetDeletedByDataId.entrySet()) {
      for (final InsertionIdData insertionId : entry.getValue()) {
        final InsertionIdQuery constraint =
            new InsertionIdQuery(
                insertionId.partitionKey,
                insertionId.sortKey,
                entry.getKey().getBytes());
        final Query<T> query =
            (Query) QueryBuilder.newBuilder().indexName(index.getName()).addTypeName(
                adapter.getTypeName()).constraints(constraint).build();

        // we don't want the duplicates to try to delete one another
        // recursively over and over so we pass false for this deletion
        dataStore.delete(query, false);
      }
    }
  }

  @Override
  public synchronized void entryDeleted(final T entry, final GeoWaveRow... rows) {
    closed = false;
    if (rows.length > 0) {
      if ((rows[0].getNumberOfDuplicates() > 0)
          && (rows.length <= rows[0].getNumberOfDuplicates())) {
        final ByteArray dataId = new ByteArray(rows[0].getDataId());
        Set<InsertionIdData> insertionIds = insertionIdsNotYetDeletedByDataId.get(dataId);
        if (insertionIds == null) {
          insertionIds = new HashSet<>();
          insertionIdsNotYetDeletedByDataId.put(dataId, insertionIds);
          // we haven't visited this data ID yet so we need to start tracking it
          final InsertionIds ids =
              DataStoreUtils.getInsertionIdsForEntry(entry, adapter, indexMapping, index);
          for (final SinglePartitionInsertionIds insertId : ids.getPartitionKeys()) {
            for (final byte[] sortKey : insertId.getSortKeys()) {
              byte[] partitionKey = insertId.getPartitionKey();
              insertionIds.add(
                  new InsertionIdData(
                      partitionKey == null ? new byte[0] : partitionKey,
                      sortKey == null ? new byte[0] : sortKey));
            }
          }
        }
        final Set<InsertionIdData> i = insertionIds;
        // we need to do is remove the rows in this callback. marking them as deleted
        Arrays.stream(rows).forEach(row -> {
          byte[] partitionKey = row.getPartitionKey();
          byte[] sortKey = row.getSortKey();
          i.remove(
              new InsertionIdData(
                  partitionKey == null ? new byte[0] : partitionKey,
                  sortKey == null ? new byte[0] : sortKey));
        });
      }
    }
  }

  private static class InsertionIdData {
    public final byte[] partitionKey;
    public final byte[] sortKey;

    public InsertionIdData(final byte[] partitionKey, final byte[] sortKey) {
      this.partitionKey = partitionKey;
      this.sortKey = sortKey;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + Arrays.hashCode(partitionKey);
      result = (prime * result) + Arrays.hashCode(sortKey);
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final InsertionIdData other = (InsertionIdData) obj;
      if (!Arrays.equals(partitionKey, other.partitionKey)) {
        return false;
      }
      if (!Arrays.equals(sortKey, other.sortKey)) {
        return false;
      }
      return true;
    }
  }
}
