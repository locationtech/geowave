/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.operations;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBClient;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBIndexTable;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBUtils;

public class RocksDBWriter implements RowWriter {
  private static ByteArray EMPTY_PARTITION_KEY = new ByteArray();
  private final RocksDBClient client;
  private final String indexNamePrefix;

  private final short adapterId;
  private final LoadingCache<ByteArray, RocksDBIndexTable> tableCache =
      Caffeine.newBuilder().build(partitionKey -> getTable(partitionKey.getBytes()));
  boolean isTimestampRequired;

  public RocksDBWriter(
      final RocksDBClient client,
      final short adapterId,
      final String typeName,
      final String indexName,
      final boolean isTimestampRequired) {
    this.client = client;
    this.adapterId = adapterId;
    indexNamePrefix = RocksDBUtils.getTablePrefix(typeName, indexName);
    this.isTimestampRequired = isTimestampRequired;
  }

  private RocksDBIndexTable getTable(final byte[] partitionKey) {
    return RocksDBUtils.getIndexTableFromPrefix(
        client,
        indexNamePrefix,
        adapterId,
        partitionKey,
        isTimestampRequired);
  }

  @Override
  public void write(final GeoWaveRow[] rows) {
    for (final GeoWaveRow row : rows) {
      write(row);
    }
  }

  @Override
  public void write(final GeoWaveRow row) {
    ByteArray partitionKey;
    if ((row.getPartitionKey() == null) || (row.getPartitionKey().length == 0)) {
      partitionKey = EMPTY_PARTITION_KEY;
    } else {
      partitionKey = new ByteArray(row.getPartitionKey());
    }
    for (final GeoWaveValue value : row.getFieldValues()) {
      tableCache.get(partitionKey).add(
          row.getSortKey(),
          row.getDataId(),
          (short) row.getNumberOfDuplicates(),
          value);
    }
  }

  @Override
  public void flush() {
    tableCache.asMap().forEach((k, v) -> v.flush());
  }

  @Override
  public void close() {
    flush();
    tableCache.invalidateAll();
  }
}
