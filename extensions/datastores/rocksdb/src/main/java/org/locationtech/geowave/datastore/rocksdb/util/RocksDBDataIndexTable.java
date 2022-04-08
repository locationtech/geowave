/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.util;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
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
import com.google.common.collect.Streams;

public class RocksDBDataIndexTable extends AbstractRocksDBTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBDataIndexTable.class);

  public RocksDBDataIndexTable(
      final Options writeOptions,
      final WriteOptions batchWriteOptions,
      final String subDirectory,
      final short adapterId,
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
  }

  public synchronized void add(final byte[] dataId, final GeoWaveValue value) {
    put(dataId, DataIndexUtils.serializeDataIndexValue(value, visibilityEnabled));
  }

  public CloseableIterator<GeoWaveRow> dataIndexIterator(final byte[][] dataIds) {
    if ((dataIds == null) || (dataIds.length == 0)) {
      return new CloseableIterator.Empty<>();
    }
    final RocksDB readDb = getDb(true);
    if (readDb == null) {
      return new CloseableIterator.Empty<>();
    }

    try {
      final List<byte[]> dataIdsList = Arrays.asList(dataIds);
      final List<byte[]> dataIdxResults = readDb.multiGetAsList(dataIdsList);
      if (dataIdsList.size() != dataIdxResults.size()) {
        LOGGER.warn("Result size differs from original keys");
      } else {
        return new CloseableIterator.Wrapper(
            Streams.zip(
                dataIdsList.stream(),
                dataIdxResults.stream(),
                (key, value) -> value == null ? null
                    : DataIndexUtils.deserializeDataIndexRow(
                        key,
                        adapterId,
                        value,
                        visibilityEnabled)).filter(Objects::nonNull).iterator());
      }
    } catch (final RocksDBException e) {
      LOGGER.error("Unable to get values by data ID", e);
    }
    return new CloseableIterator.Empty<>();
  }

  public CloseableIterator<GeoWaveRow> dataIndexIterator(
      final byte[] startDataId,
      final byte[] endDataId,
      final boolean reverse) {
    final RocksDB readDb = getDb(true);
    if (readDb == null) {
      return new CloseableIterator.Empty<>();
    }
    final RocksIterator it;
    if (reverse) {
      it = readDb.newIterator();
      if (endDataId == null) {
        it.seekToLast();
      } else {
        it.seekForPrev(ByteArrayUtils.getNextPrefix(endDataId));
      }
      if (startDataId == null) {
        return new DataIndexReverseRowIterator(it, adapterId, visibilityEnabled);
      }
      return new DataIndexBoundedReverseRowIterator(startDataId, it, adapterId, visibilityEnabled);
    } else {
      final ReadOptions options;
      if (endDataId == null) {
        options = null;
        it = readDb.newIterator();
      } else {
        options =
            new ReadOptions().setIterateUpperBound(
                new Slice(ByteArrayUtils.getNextPrefix(endDataId)));
        it = readDb.newIterator(options);
      }
      if (startDataId == null) {
        it.seekToFirst();
      } else {
        it.seek(startDataId);
      }
      return new DataIndexForwardRowIterator(options, it, adapterId, visibilityEnabled);
    }
  }
}
