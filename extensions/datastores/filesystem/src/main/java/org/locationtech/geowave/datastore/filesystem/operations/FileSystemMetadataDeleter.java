/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.operations;

import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemGeoWaveMetadata;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemMetadataTable;

public class FileSystemMetadataDeleter implements MetadataDeleter {
  private final FileSystemMetadataTable table;
  private final MetadataType metadataType;

  public FileSystemMetadataDeleter(
      final FileSystemMetadataTable table,
      final MetadataType metadataType) {
    this.table = table;
    this.metadataType = metadataType;
  }

  @Override
  public boolean delete(final MetadataQuery query) {
    boolean atLeastOneDeletion = false;

    try (CloseableIterator<GeoWaveMetadata> it =
        new FileSystemMetadataReader(table, metadataType).query(query)) {
      while (it.hasNext()) {
        table.remove(((FileSystemGeoWaveMetadata) it.next()).getKey());
        atLeastOneDeletion = true;
      }
    }
    return atLeastOneDeletion;
  }

  @Override
  public void flush() {}

  @Override
  public void close() throws Exception {}
}
