/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.util;

import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;

public class RocksDBMetadataTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBMetadataTable.class);
  private final ManagedRocksDB db;
  private final boolean requiresTimestamp;
  private final boolean visibilityEnabled;
  private final boolean compactOnWrite;
  private long prevTime = Long.MAX_VALUE;

  /** The table owns the database, and cannot be used again once closed. */
  public RocksDBMetadataTable(
      final RocksDB db,
      final boolean requiresTimestamp,
      final boolean visibilityEnabled,
      final boolean compactOnWrite) {
    this(new ManagedRocksDB(null, null, db), requiresTimestamp, visibilityEnabled, compactOnWrite);
  }

  /** The database is open already, and reopens if the table is used after close(). */
  RocksDBMetadataTable(
      final String directory,
      final RocksDB db,
      final boolean requiresTimestamp,
      final boolean visibilityEnabled,
      final boolean compactOnWrite) {
    this(
        new ManagedRocksDB(directory, RocksDBClient::openMetadataDb, db),
        requiresTimestamp,
        visibilityEnabled,
        compactOnWrite);
  }

  private RocksDBMetadataTable(
      final ManagedRocksDB db,
      final boolean requiresTimestamp,
      final boolean visibilityEnabled,
      final boolean compactOnWrite) {
    super();
    this.db = db;
    this.requiresTimestamp = requiresTimestamp;
    this.visibilityEnabled = visibilityEnabled;
    this.compactOnWrite = compactOnWrite;
  }

  public void remove(final byte[] key) {
    try {
      db.write(rocks -> {
        rocks.singleDelete(key);
        return null;
      });
    } catch (final RocksDBException e) {
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

  public void compact() {
    try {
      compactRange();
    } catch (final RocksDBException e) {
      LOGGER.warn("Unable to force compacting metadata", e);
    }
  }

  public CloseableIterator<GeoWaveMetadata> iterator(final ByteArrayRange range) {
    return db.iterator(
        () -> range.getEnd() == null ? null : new UpperBoundReadOptions(range.getEndAsNextPrefix()),
        (options, it) -> {
          if (range.getStart() == null) {
            it.seekToFirst();
          } else {
            it.seek(range.getStart());
          }
          return new RocksDBMetadataIterator(options, it, requiresTimestamp, visibilityEnabled);
        });
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
    return db.iterator(() -> new ReadOptions().setPrefixSameAsStart(true), (options, it) -> {
      it.seek(prefix);
      return new RocksDBMetadataIterator(options, it, requiresTimestamp, visibilityEnabled);
    });
  }

  public CloseableIterator<GeoWaveMetadata> iterator() {
    return db.iterator(() -> null, (options, it) -> {
      it.seekToFirst();
      return new RocksDBMetadataIterator(it, requiresTimestamp, visibilityEnabled);
    });
  }

  public void put(final byte[] key, final byte[] value) {
    try {
      db.write(rocks -> {
        rocks.put(key, value);
        return null;
      });
    } catch (final RocksDBException e) {
      LOGGER.warn("Unable to add metadata", e);
    }
  }

  public void flush() {
    if (compactOnWrite) {
      try {
        compactRange();
      } catch (final RocksDBException e) {
        LOGGER.warn("Unable to compact metadata", e);
      }
    }
  }

  private void compactRange() throws RocksDBException {
    db.write(rocks -> {
      rocks.compactRange();
      return null;
    });
  }

  boolean isOpen() {
    return db.isOpen();
  }

  /** Closes the database and any iterators still open on it. */
  public void close() {
    db.close();
  }
}
