/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import java.nio.file.Path;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatter.DataIndexFormatter;

public class DataIndexRowIterator extends AbstractFileSystemIterator<GeoWaveRow> {
  private final short adapterId;
  private final String typeName;
  private final DataIndexFormatter formatter;

  public DataIndexRowIterator(
      final Path subDirectory,
      final byte[] startKey,
      final byte[] endKey,
      final short adapterId,
      final String typeName,
      final DataIndexFormatter formatter) {
    super(
        subDirectory,
        startKey,
        endKey,
        true,
        fileName -> new BasicFileSystemKey(formatter.getDataId(fileName, typeName), fileName));
    this.adapterId = adapterId;
    this.typeName = typeName;
    this.formatter = formatter;
  }

  @Override
  protected GeoWaveRow readRow(final FileSystemKey key, final byte[] value) {
    return new GeoWaveRowImpl(
        new GeoWaveKeyImpl(key.getSortOrderKey(), adapterId, new byte[0], new byte[0], 0),
        new GeoWaveValue[] {
            formatter.getValue(key.getFileName(), typeName, key.getSortOrderKey(), value)});
  }
}
