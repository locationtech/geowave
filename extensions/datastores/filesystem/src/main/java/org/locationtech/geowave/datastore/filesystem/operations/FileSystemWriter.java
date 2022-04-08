/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.operations;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemClient;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemIndexTable;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemUtils;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class FileSystemWriter implements RowWriter {
  private static ByteArray EMPTY_PARTITION_KEY = new ByteArray();
  private final FileSystemClient client;

  private final short adapterId;
  private final String typeName;
  private final String indexName;
  private final LoadingCache<ByteArray, FileSystemIndexTable> tableCache =
      Caffeine.newBuilder().build(partitionKey -> getTable(partitionKey.getBytes()));
  private final boolean isTimestampRequired;

  public FileSystemWriter(
      final FileSystemClient client,
      final short adapterId,
      final String typeName,
      final String indexName,
      final boolean isTimestampRequired) {
    this.client = client;
    this.adapterId = adapterId;
    this.typeName = typeName;
    this.indexName = indexName;
    this.isTimestampRequired = isTimestampRequired;
  }

  private FileSystemIndexTable getTable(final byte[] partitionKey) {
    return FileSystemUtils.getIndexTable(
        client,
        adapterId,
        typeName,
        indexName,
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
  public void flush() {}

  @Override
  public void close() {
    flush();
    tableCache.invalidateAll();
  }
}
