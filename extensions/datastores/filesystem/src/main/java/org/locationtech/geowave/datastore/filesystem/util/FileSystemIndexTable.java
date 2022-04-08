/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatter.FileSystemIndexKey;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatter.FormattedFileInfo;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatter.IndexFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.primitives.Bytes;

public class FileSystemIndexTable extends AbstractFileSystemTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemIndexTable.class);
  private final boolean requiresTimestamp;
  private final String indexName;
  private final byte[] partitionKey;

  public FileSystemIndexTable(
      final String subDirectory,
      final short adapterId,
      final String typeName,
      final String indexName,
      final byte[] partitionKey,
      final String format,
      final boolean requiresTimestamp,
      final boolean visibilityEnabled) throws IOException {
    super(adapterId, typeName, format, visibilityEnabled);
    this.requiresTimestamp = requiresTimestamp;
    this.indexName = indexName;
    this.partitionKey = partitionKey;
    final IndexFormatter indexFormatter = formatter.getIndexFormatter();

    setTableDirectory(
        FileSystemUtils.getSubdirectory(
            subDirectory,
            indexFormatter.getDirectoryName(indexName, typeName),
            indexFormatter.getPartitionDirectoryName(indexName, typeName, partitionKey)));
  }

  public void delete(final byte[] sortKey, final byte[] dataId) {
    final byte[] prefix = Bytes.concat(sortKey, dataId);
    FileSystemUtils.visit(tableDirectory, prefix, ByteArrayUtils.getNextPrefix(prefix), p -> {
      try {
        Files.delete(p);
      } catch (final IOException e) {
        LOGGER.warn("Unable to delete file", e);
      }
    }, fileNameToKey());
  }

  protected Function<String, FileSystemKey> fileNameToKey() {
    return fileName -> new FileSystemIndexKeyWrapper(
        formatter.getIndexFormatter().getKey(fileName, typeName, indexName, requiresTimestamp),
        fileName);
  }

  public synchronized void add(
      final byte[] sortKey,
      final byte[] dataId,
      final short numDuplicates,
      final GeoWaveValue value) {
    final FormattedFileInfo fileInfo =
        formatter.getIndexFormatter().format(
            typeName,
            indexName,
            new FileSystemIndexKey(
                sortKey,
                dataId,
                requiresTimestamp ? Optional.of(System.currentTimeMillis()) : Optional.empty(),
                numDuplicates),
            value);
    writeFile(fileInfo.getFileName(), fileInfo.getFileContents());
  }


  public CloseableIterator<GeoWaveRow> iterator() {
    return new FileSystemRowIterator(
        tableDirectory,
        null,
        null,
        adapterId,
        typeName,
        indexName,
        partitionKey,
        formatter.getIndexFormatter(),
        fileNameToKey());
  }

  public CloseableIterator<GeoWaveRow> iterator(final Collection<ByteArrayRange> ranges) {
    return new FileSystemRowIterator(
        tableDirectory,
        ranges,
        adapterId,
        typeName,
        indexName,
        partitionKey,
        formatter.getIndexFormatter(),
        fileNameToKey());
  }
}
