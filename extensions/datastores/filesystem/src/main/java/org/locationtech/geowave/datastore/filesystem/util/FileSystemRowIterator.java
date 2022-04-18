/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import java.nio.file.Path;
import java.util.Collection;
import java.util.function.Function;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatter.FileSystemIndexKey;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatter.FormattedFileInfo;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatter.IndexFormatter;

public class FileSystemRowIterator extends AbstractFileSystemIterator<GeoWaveRow> {
  private final short adapterId;
  private final byte[] partition;
  private final IndexFormatter formatter;
  private final String typeName;
  private final String indexName;

  public FileSystemRowIterator(
      final Path subDirectory,
      final byte[] startKey,
      final byte[] endKey,
      final short adapterId,
      final String typeName,
      final String indexName,
      final byte[] partition,
      final IndexFormatter formatter,
      final Function<String, FileSystemKey> fileNameToKey) {
    super(subDirectory, startKey, endKey, fileNameToKey);
    this.adapterId = adapterId;
    this.typeName = typeName;
    this.indexName = indexName;
    this.partition = partition;
    this.formatter = formatter;
  }

  public FileSystemRowIterator(
      final Path subDirectory,
      final Collection<ByteArrayRange> ranges,
      final short adapterId,
      final String typeName,
      final String indexName,
      final byte[] partition,
      final IndexFormatter formatter,
      final Function<String, FileSystemKey> fileNameToKey) {
    super(subDirectory, ranges, fileNameToKey);
    this.adapterId = adapterId;
    this.typeName = typeName;
    this.indexName = indexName;
    this.partition = partition;
    this.formatter = formatter;
  }

  @Override
  protected GeoWaveRow readRow(final FileSystemKey key, final byte[] value) {
    final FileSystemIndexKey indexKey = ((FileSystemIndexKeyWrapper) key).getOriginalKey();
    return new FileSystemRow(
        key.getFileName(),
        adapterId,
        partition,
        indexKey,
        formatter.getValue(
            indexKey,
            typeName,
            indexName,
            new FormattedFileInfo(key.getFileName(), value)));
  }
}
