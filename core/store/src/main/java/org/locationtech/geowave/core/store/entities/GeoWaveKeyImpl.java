/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.entities;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.SinglePartitionInsertionIds;
import org.locationtech.geowave.core.index.VarintUtils;

public class GeoWaveKeyImpl implements GeoWaveKey {
  protected byte[] dataId = null;
  protected short internalAdapterId = 0;
  protected byte[] partitionKey = null;
  protected byte[] sortKey = null;
  protected int numberOfDuplicates = 0;
  private byte[] compositeInsertionId = null;

  protected GeoWaveKeyImpl() {}

  public GeoWaveKeyImpl(final byte[] compositeInsertionId, final int partitionKeyLength) {
    this(compositeInsertionId, partitionKeyLength, compositeInsertionId.length);
  }

  public GeoWaveKeyImpl(
      final byte[] compositeInsertionId,
      final int partitionKeyLength,
      final int length) {
    this(compositeInsertionId, partitionKeyLength, 0, length);
  }

  public GeoWaveKeyImpl(
      final byte[] compositeInsertionId,
      final int partitionKeyLength,
      final int offset,
      final int length) {
    this.compositeInsertionId = compositeInsertionId;
    final ByteBuffer buf = ByteBuffer.wrap(compositeInsertionId, offset, length);
    buf.position(buf.limit() - 1);
    final int numberOfDuplicates = VarintUtils.readUnsignedIntReversed(buf);
    final int dataIdLength = VarintUtils.readUnsignedIntReversed(buf);
    final byte[] dataId = new byte[dataIdLength];
    buf.position(buf.position() - dataIdLength + 1);
    buf.get(dataId);
    buf.position(buf.position() - dataIdLength - 1);
    this.internalAdapterId = (short) VarintUtils.readUnsignedIntReversed(buf);
    int readLength = buf.limit() - 1 - buf.position();

    buf.position(offset);
    final byte[] sortKey = new byte[length - readLength - partitionKeyLength];
    final byte[] partitionKey = new byte[partitionKeyLength];
    buf.get(partitionKey);
    buf.get(sortKey);

    this.dataId = dataId;
    this.partitionKey = partitionKey;
    this.sortKey = sortKey;
    this.numberOfDuplicates = numberOfDuplicates;
  }

  public GeoWaveKeyImpl(
      final byte[] dataId,
      final short internalAdapterId,
      final byte[] partitionKey,
      final byte[] sortKey,
      final int numberOfDuplicates) {
    this.dataId = dataId;
    this.internalAdapterId = internalAdapterId;
    this.partitionKey = partitionKey;
    this.sortKey = sortKey;
    this.numberOfDuplicates = numberOfDuplicates;
  }

  @Override
  public byte[] getDataId() {
    return dataId;
  }

  @Override
  public short getAdapterId() {
    return internalAdapterId;
  }

  @Override
  public byte[] getPartitionKey() {
    return partitionKey;
  }

  @Override
  public byte[] getSortKey() {
    return sortKey;
  }

  public byte[] getCompositeInsertionId() {
    if (compositeInsertionId != null) {
      return compositeInsertionId;
    }
    compositeInsertionId = GeoWaveKey.getCompositeId(this);
    return compositeInsertionId;
  }

  @Override
  public int getNumberOfDuplicates() {
    return numberOfDuplicates;
  }

  public boolean isDeduplicationEnabled() {
    return numberOfDuplicates >= 0;
  }

  public static GeoWaveKey[] createKeys(
      final InsertionIds insertionIds,
      final byte[] dataId,
      final short internalAdapterId) {
    final GeoWaveKey[] keys = new GeoWaveKey[insertionIds.getSize()];
    final Collection<SinglePartitionInsertionIds> partitionKeys = insertionIds.getPartitionKeys();
    final Iterator<SinglePartitionInsertionIds> it = partitionKeys.iterator();
    final int numDuplicates = keys.length - 1;
    int i = 0;
    while (it.hasNext()) {
      final SinglePartitionInsertionIds partitionKey = it.next();
      if ((partitionKey.getSortKeys() == null) || partitionKey.getSortKeys().isEmpty()) {
        keys[i++] =
            new GeoWaveKeyImpl(
                dataId,
                internalAdapterId,
                partitionKey.getPartitionKey().getBytes(),
                new byte[] {},
                numDuplicates);
      } else {
        byte[] partitionKeyBytes;
        if (partitionKey.getPartitionKey() == null) {
          partitionKeyBytes = new byte[] {};
        } else {
          partitionKeyBytes = partitionKey.getPartitionKey().getBytes();
        }
        final List<ByteArray> sortKeys = partitionKey.getSortKeys();
        for (final ByteArray sortKey : sortKeys) {
          keys[i++] =
              new GeoWaveKeyImpl(
                  dataId,
                  internalAdapterId,
                  partitionKeyBytes,
                  sortKey.getBytes(),
                  numDuplicates);
        }
      }
    }
    return keys;
  }
}
