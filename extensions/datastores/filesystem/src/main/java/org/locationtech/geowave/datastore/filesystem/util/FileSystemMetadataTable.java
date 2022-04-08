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
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;

public class FileSystemMetadataTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemMetadataTable.class);
  private final Path subDirectory;
  private final boolean requiresTimestamp;
  private final boolean visibilityEnabled;
  private long prevTime = Long.MAX_VALUE;

  public FileSystemMetadataTable(
      final Path subDirectory,
      final boolean requiresTimestamp,
      final boolean visibilityEnabled) throws IOException {
    super();
    this.subDirectory = subDirectory;
    Files.createDirectories(subDirectory);
    this.requiresTimestamp = requiresTimestamp;
    this.visibilityEnabled = visibilityEnabled;
  }

  public void remove(final byte[] key) {
    try {
      Files.delete(subDirectory.resolve(FileSystemUtils.keyToFileName(key)));
    } catch (final Exception e) {
      LOGGER.warn("Unable to delete metadata", e);
    }
  }

  public void add(final GeoWaveMetadata value) {
    byte[] key;
    final byte[] secondaryId =
        value.getSecondaryId() == null ? new byte[0] : value.getSecondaryId();
    byte[] endBytes;
    if (visibilityEnabled) {
      final byte[] visibility = value.getVisibility() == null ? new byte[0] : value.getVisibility();

      endBytes =
          Bytes.concat(
              visibility,
              new byte[] {(byte) visibility.length, (byte) value.getPrimaryId().length});
    } else {
      endBytes = new byte[] {(byte) value.getPrimaryId().length};
    }
    if (requiresTimestamp) {
      // sometimes rows can be written so quickly that they are the exact
      // same millisecond - while Java does offer nanosecond precision,
      // support is OS-dependent. Instead this check is done to ensure
      // subsequent millis are written at least within this ingest
      // process.
      long time = Long.MAX_VALUE - System.currentTimeMillis();
      if (time >= prevTime) {
        time = prevTime - 1;
      }
      prevTime = time;
      key = Bytes.concat(value.getPrimaryId(), secondaryId, Longs.toByteArray(time), endBytes);
    } else {
      key = Bytes.concat(value.getPrimaryId(), secondaryId, endBytes);
    }
    put(key, value.getValue());
  }

  public CloseableIterator<GeoWaveMetadata> iterator(final ByteArrayRange range) {
    return new FileSystemMetadataIterator(
        subDirectory,
        range.getStart(),
        range.getEndAsNextPrefix(),
        requiresTimestamp,
        visibilityEnabled);

  }

  public CloseableIterator<GeoWaveMetadata> iterator(final byte[] primaryId) {
    return prefixIterator(primaryId);
  }

  public CloseableIterator<GeoWaveMetadata> iterator(
      final byte[] primaryId,
      final byte[] secondaryId) {
    return prefixIterator(Bytes.concat(primaryId, secondaryId));
  }

  private CloseableIterator<GeoWaveMetadata> prefixIterator(final byte[] prefix) {
    return new FileSystemMetadataIterator(
        subDirectory,
        prefix,
        ByteArrayUtils.getNextPrefix(prefix),
        requiresTimestamp,
        visibilityEnabled);
  }

  public CloseableIterator<GeoWaveMetadata> iterator() {
    return new FileSystemMetadataIterator(
        subDirectory,
        null,
        null,
        requiresTimestamp,
        visibilityEnabled);
  }

  public void put(final byte[] key, final byte[] value) {
    try {
      Files.write(
          subDirectory.resolve(FileSystemUtils.keyToFileName(key)),
          value,
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING,
          StandardOpenOption.SYNC);
    } catch (final IOException e) {
      LOGGER.warn("Unable to write file", e);
    }
  }
}
