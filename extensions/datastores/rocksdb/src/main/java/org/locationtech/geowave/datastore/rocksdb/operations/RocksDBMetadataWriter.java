/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.operations;

import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBMetadataTable;

public class RocksDBMetadataWriter implements MetadataWriter {
  private final RocksDBMetadataTable table;
  private boolean closed = false;

  public RocksDBMetadataWriter(final RocksDBMetadataTable table) {
    this.table = table;
  }

  @Override
  public void write(final GeoWaveMetadata metadata) {
    table.add(metadata);
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
