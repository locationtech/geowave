/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.operations;

import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBClient;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBDataIndexTable;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBUtils;

public class RockDBDataIndexWriter implements RowWriter {
  private final RocksDBDataIndexTable table;

  public RockDBDataIndexWriter(
      final RocksDBClient client,
      final short adapterId,
      final String typeName) {
    table = RocksDBUtils.getDataIndexTable(client, typeName, adapterId);
  }

  @Override
  public void write(final GeoWaveRow[] rows) {
    for (final GeoWaveRow row : rows) {
      write(row);
    }
  }

  @Override
  public void write(final GeoWaveRow row) {
    for (final GeoWaveValue value : row.getFieldValues()) {
      table.add(row.getDataId(), value);
    }
  }

  @Override
  public void flush() {
    table.flush();
  }

  @Override
  public void close() {
    flush();
  }
}
