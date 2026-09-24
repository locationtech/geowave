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
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Streams;

public class RocksDBDataIndexTable extends AbstractRocksDBTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBDataIndexTable.class);

  public RocksDBDataIndexTable(
      final String subDirectory,
      final short adapterId,
      final boolean visibilityEnabled,
      final boolean compactOnWrite,
      final int batchSize,
      final boolean walOnBatchWrite) {
    super(subDirectory, adapterId, visibilityEnabled, compactOnWrite, batchSize, walOnBatchWrite);
  }

  public synchronized void add(final byte[] dataId, final GeoWaveValue value) {
    put(dataId, DataIndexUtils.serializeDataIndexValue(value, visibilityEnabled));
  }

  public CloseableIterator<GeoWaveRow> dataIndexIterator(final byte[][] dataIds) {
    if ((dataIds == null) || (dataIds.length == 0)) {
      return new CloseableIterator.Empty<>();
    }
    final List<byte[]> dataIdsList = Arrays.asList(dataIds);
    try {
      final List<byte[]> dataIdxResults =
          getManagedDb().read(db -> db.multiGetAsList(dataIdsList), () -> null);
      if (dataIdxResults == null) {
        return new CloseableIterator.Empty<>();
      }
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
    if (reverse) {
      final CloseableIterator<GeoWaveRow> it = iterator(() -> null, (options, rocksIt) -> {
        if (endDataId == null) {
          rocksIt.seekToLast();
        } else {
          rocksIt.seekForPrev(ByteArrayUtils.getNextPrefix(endDataId));
        }
        return new DataIndexReverseRowIterator(rocksIt, adapterId, visibilityEnabled);
      });
      if (startDataId == null) {
        return it;
      }
      return new DataIndexBoundedReverseRowIterator(startDataId, it);
    }
    return iterator(
        () -> endDataId == null ? null
            : new ReadOptions().setIterateUpperBound(
                new Slice(ByteArrayUtils.getNextPrefix(endDataId))),
        (options, rocksIt) -> {
          if (startDataId == null) {
            rocksIt.seekToFirst();
          } else {
            rocksIt.seek(startDataId);
          }
          return new DataIndexForwardRowIterator(options, rocksIt, adapterId, visibilityEnabled);
        });
  }
}
