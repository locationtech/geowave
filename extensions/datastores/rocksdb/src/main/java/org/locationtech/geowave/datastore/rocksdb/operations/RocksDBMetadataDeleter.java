/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.operations;

import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBGeoWaveMetadata;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBMetadataTable;

public class RocksDBMetadataDeleter implements MetadataDeleter {
  private final RocksDBMetadataTable table;
  private final MetadataType metadataType;
  private boolean closed = false;

  public RocksDBMetadataDeleter(final RocksDBMetadataTable table, final MetadataType metadataType) {
    this.table = table;
    this.metadataType = metadataType;
  }

  @Override
  public boolean delete(final MetadataQuery query) {
    boolean atLeastOneDeletion = false;

    try (CloseableIterator<GeoWaveMetadata> it =
        new RocksDBMetadataReader(table, metadataType).query(query, false)) {
      while (it.hasNext()) {
        table.remove(((RocksDBGeoWaveMetadata) it.next()).getKey());
        atLeastOneDeletion = true;
      }
    }
    return atLeastOneDeletion;
  }

  @Override
  public void flush() {
    table.flush();
  }

  @Override
  public void close() throws Exception {
    // guard against repeated calls to close
    if (!closed) {
      flush();
      closed = true;
    }
  }
}
