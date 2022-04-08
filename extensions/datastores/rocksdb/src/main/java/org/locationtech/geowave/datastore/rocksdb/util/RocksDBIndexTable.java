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
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;

public class RocksDBIndexTable extends AbstractRocksDBTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBIndexTable.class);
  private long prevTime = Long.MAX_VALUE;
  private final boolean requiresTimestamp;
  private final byte[] partition;

  public RocksDBIndexTable(
      final Options writeOptions,
      final WriteOptions batchWriteOptions,
      final String subDirectory,
      final short adapterId,
      final byte[] partition,
      final boolean requiresTimestamp,
      final boolean visibilityEnabled,
      final boolean compactOnWrite,
      final int batchSize) {
    super(
        writeOptions,
        batchWriteOptions,
        subDirectory,
        adapterId,
        visibilityEnabled,
        compactOnWrite,
        batchSize);
    this.requiresTimestamp = requiresTimestamp;
    this.partition = partition;
  }

  public void delete(final byte[] sortKey, final byte[] dataId) {
    final RocksDB db = getDb(false);
    if (db == null) {
      LOGGER.warn("Unable to delete key because directory '" + subDirectory + "' doesn't exist");
      return;
    }
    try {
      final byte[] prefix = Bytes.concat(sortKey, dataId);
      db.deleteRange(prefix, ByteArrayUtils.getNextPrefix(prefix));
    } catch (final RocksDBException e) {
      LOGGER.warn("Unable to delete by sort key and data ID", e);
    }
  }

  public synchronized void add(
      final byte[] sortKey,
      final byte[] dataId,
      final short numDuplicates,
      final GeoWaveValue value) {
    byte[] key;
    byte[] endBytes;
    if (visibilityEnabled) {
      endBytes =
          Bytes.concat(
              value.getVisibility(),
              ByteArrayUtils.shortToByteArray(numDuplicates),
              new byte[] {
                  (byte) value.getVisibility().length,
                  (byte) sortKey.length,
                  (byte) value.getFieldMask().length});
    } else {
      endBytes =
          Bytes.concat(
              ByteArrayUtils.shortToByteArray(numDuplicates),
              new byte[] {(byte) sortKey.length, (byte) value.getFieldMask().length});
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
      key = Bytes.concat(sortKey, dataId, Longs.toByteArray(time), value.getFieldMask(), endBytes);
    } else {
      key = Bytes.concat(sortKey, dataId, value.getFieldMask(), endBytes);
    }
    put(key, value.getValue());
  }


  public CloseableIterator<GeoWaveRow> iterator() {
    final RocksDB readDb = getDb(true);
    if (readDb == null) {
      return new CloseableIterator.Empty<>();
    }
    final ReadOptions options = new ReadOptions().setFillCache(false);
    final RocksIterator it = readDb.newIterator(options);
    it.seekToFirst();
    return new RocksDBRowIterator(
        options,
        it,
        adapterId,
        partition,
        requiresTimestamp,
        visibilityEnabled);
  }

  public CloseableIterator<GeoWaveRow> iterator(final ByteArrayRange range) {
    final RocksDB readDb = getDb(true);
    if (readDb == null) {
      return new CloseableIterator.Empty<>();
    }
    final ReadOptions options;
    final RocksIterator it;
    if (range.getEnd() == null) {
      options = null;
      it = readDb.newIterator();
    } else {
      options = new ReadOptions().setIterateUpperBound(new Slice(range.getEndAsNextPrefix()));
      it = readDb.newIterator(options);
    }
    if (range.getStart() == null) {
      it.seekToFirst();
    } else {
      it.seek(range.getStart());
    }

    return new RocksDBRowIterator(
        options,
        it,
        adapterId,
        partition,
        requiresTimestamp,
        visibilityEnabled);
  }
}
