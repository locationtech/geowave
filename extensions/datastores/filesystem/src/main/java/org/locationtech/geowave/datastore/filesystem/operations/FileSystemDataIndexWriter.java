/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.operations;

import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemClient;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemDataIndexTable;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemUtils;

public class FileSystemDataIndexWriter implements RowWriter {
  private final FileSystemDataIndexTable table;

  public FileSystemDataIndexWriter(
      final FileSystemClient client,
      final short adapterId,
      final String typeName) {
    table = FileSystemUtils.getDataIndexTable(client, adapterId, typeName);
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
  public void flush() {}

  @Override
  public void close() {}
}
