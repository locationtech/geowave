/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.core.store.entities.MergeableGeoWaveRow;

public class RocksDBRow extends MergeableGeoWaveRow implements GeoWaveRow {
  List<byte[]> mergedKeys;
  private final byte[] key;
  private final short adapterId;
  private final byte[] partition;
  private final byte[] sortKey;
  private final byte[] dataId;
  private final short duplicates;

  public RocksDBRow(
      final short adapterId,
      final byte[] partition,
      final byte[] key,
      final byte[] value,
      final boolean containsTimestamp,
      final boolean visibilityEnabled) {
    super();
    int otherBytes = 4;
    this.adapterId = adapterId;
    this.partition = partition;
    this.key = key;
    final ByteBuffer buf = ByteBuffer.wrap(key);
    sortKey = new byte[key[key.length - 2]];
    buf.get(sortKey);
    final byte[] fieldMask = new byte[key[key.length - 1]];
    final byte[] visibility;
    if (visibilityEnabled) {
      visibility = new byte[key[key.length - 3]];
      otherBytes++;
    } else {
      visibility = new byte[0];
    }
    if (containsTimestamp) {
      otherBytes += 8;
    }
    dataId =
        new byte[key.length - otherBytes - sortKey.length - fieldMask.length - visibility.length];
    buf.get(dataId);
    if (containsTimestamp) {
      // just skip 8 bytes - we don't care to parse out the timestamp but
      // its there for key uniqueness and to maintain expected sort order
      buf.position(buf.position() + 8);
    }
    buf.get(fieldMask);
    if (visibilityEnabled) {
      buf.get(visibility);
    }
    final byte[] duplicatesBytes = new byte[2];
    buf.get(duplicatesBytes);
    duplicates = ByteArrayUtils.byteArrayToShort(duplicatesBytes);
    attributeValues = new GeoWaveValue[] {new GeoWaveValueImpl(fieldMask, visibility, value)};
  }

  @Override
  public byte[] getDataId() {
    return dataId;
  }

  @Override
  public short getAdapterId() {
    return adapterId;
  }

  @Override
  public byte[] getSortKey() {
    return sortKey;
  }

  @Override
  public byte[] getPartitionKey() {
    return partition;
  }

  @Override
  public int getNumberOfDuplicates() {
    return duplicates;
  }

  public byte[][] getKeys() {
    // this is intentionally not threadsafe because it isn't required
    if (mergedKeys == null) {
      return new byte[][] {key};
    } else {
      return ArrayUtils.add(mergedKeys.toArray(new byte[0][]), key);
    }
  }

  @Override
  public void mergeRow(final MergeableGeoWaveRow row) {
    super.mergeRow(row);
    if (row instanceof RocksDBRow) {
      // this is intentionally not threadsafe because it isn't required
      if (mergedKeys == null) {
        mergedKeys = new ArrayList<>();
      }
      Arrays.stream(((RocksDBRow) row).getKeys()).forEach(r -> mergedKeys.add(r));
    }
  }
}
