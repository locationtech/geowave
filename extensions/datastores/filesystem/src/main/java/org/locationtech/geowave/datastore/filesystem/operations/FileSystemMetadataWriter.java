/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.operations;

import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.datastore.filesystem.util.FileSystemMetadataTable;

public class FileSystemMetadataWriter implements MetadataWriter {
  private final FileSystemMetadataTable table;

  public FileSystemMetadataWriter(final FileSystemMetadataTable table) {
    this.table = table;
  }

  @Override
  public void write(final GeoWaveMetadata metadata) {
    table.add(metadata);
  }

  @Override
  public void flush() {}

  @Override
  public void close() throws Exception {}
}
