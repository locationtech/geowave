/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.locationtech.geowave.core.index.persist.Persistable;

public class SinglePartitionInsertionIds implements Persistable {
  private List<byte[]> compositeInsertionIds;
  private byte[] partitionKey;
  private List<byte[]> sortKeys;

  public SinglePartitionInsertionIds() {}

  public SinglePartitionInsertionIds(final byte[] partitionKey) {
    this(partitionKey, (byte[]) null);
  }

  public SinglePartitionInsertionIds(final byte[] partitionKey, final byte[] sortKey) {
    this.partitionKey = partitionKey;
    sortKeys = sortKey == null ? null : new ArrayList<>(Collections.singletonList(sortKey));
  }

  public SinglePartitionInsertionIds(
      final byte[] partitionKey,
      final SinglePartitionInsertionIds insertionId2) {
    this(new SinglePartitionInsertionIds(partitionKey, (List<byte[]>) null), insertionId2);
  }

  public SinglePartitionInsertionIds(
      final SinglePartitionInsertionIds insertionId1,
      final SinglePartitionInsertionIds insertionId2) {
    partitionKey =
        ByteArrayUtils.combineArrays(insertionId1.partitionKey, insertionId2.partitionKey);
    if ((insertionId1.sortKeys == null) || insertionId1.sortKeys.isEmpty()) {
      sortKeys = insertionId2.sortKeys;
    } else if ((insertionId2.sortKeys == null) || insertionId2.sortKeys.isEmpty()) {
      sortKeys = insertionId1.sortKeys;
    } else {
      // use all permutations of range keys
      sortKeys = new ArrayList<>(insertionId1.sortKeys.size() * insertionId2.sortKeys.size());
      for (final byte[] sortKey1 : insertionId1.sortKeys) {
        for (final byte[] sortKey2 : insertionId2.sortKeys) {
          sortKeys.add(ByteArrayUtils.combineArrays(sortKey1, sortKey2));
        }
      }
    }
  }

  public SinglePartitionInsertionIds(final byte[] partitionKey, final List<byte[]> sortKeys) {
    this.partitionKey = partitionKey;
    this.sortKeys = sortKeys;
  }

  public List<byte[]> getCompositeInsertionIds() {
    if (compositeInsertionIds != null) {
      return compositeInsertionIds;
    }

    if ((sortKeys == null) || sortKeys.isEmpty()) {
      compositeInsertionIds = Arrays.asList(partitionKey);
      return compositeInsertionIds;
    }

    if (partitionKey == null) {
      compositeInsertionIds = sortKeys;
      return compositeInsertionIds;
    }

    final List<byte[]> internalInsertionIds = new ArrayList<>(sortKeys.size());
    for (final byte[] sortKey : sortKeys) {
      internalInsertionIds.add(ByteArrayUtils.combineArrays(partitionKey, sortKey));
    }
    compositeInsertionIds = internalInsertionIds;
    return compositeInsertionIds;
  }

  public byte[] getPartitionKey() {
    return partitionKey;
  }

  public List<byte[]> getSortKeys() {
    return sortKeys;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + ((partitionKey == null) ? 0 : Arrays.hashCode(partitionKey));
    if (sortKeys != null) {
      for (final byte[] sortKey : sortKeys) {
        result = (prime * result) + (sortKey == null ? 0 : Arrays.hashCode(sortKey));
      }
    }
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final SinglePartitionInsertionIds other = (SinglePartitionInsertionIds) obj;
    if (partitionKey == null) {
      if (other.partitionKey != null) {
        return false;
      }
    } else if (!Arrays.equals(partitionKey, other.partitionKey)) {
      return false;
    }
    if (sortKeys == null) {
      if (other.sortKeys != null) {
        return false;
      }
    } else if (sortKeys.size() != other.sortKeys.size()) {
      return false;
    } else {
      final Iterator<byte[]> it1 = sortKeys.iterator();
      final Iterator<byte[]> it2 = other.sortKeys.iterator();
      while (it1.hasNext() && it2.hasNext()) {
        if ((!Arrays.equals(it1.next(), it2.next()))) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public byte[] toBinary() {
    int pLength;
    if (partitionKey == null) {
      pLength = 0;
    } else {
      pLength = partitionKey.length;
    }
    int sSize;
    int byteBufferSize = VarintUtils.unsignedIntByteLength(pLength) + pLength;
    if (sortKeys == null) {
      sSize = 0;
    } else {
      sSize = sortKeys.size();
      for (final byte[] sKey : sortKeys) {
        byteBufferSize += VarintUtils.unsignedIntByteLength(sKey.length) + sKey.length;
      }
    }
    byteBufferSize += VarintUtils.unsignedIntByteLength(sSize);
    final ByteBuffer buf = ByteBuffer.allocate(byteBufferSize);
    VarintUtils.writeUnsignedInt(pLength, buf);
    if (pLength > 0) {
      buf.put(partitionKey);
    }
    VarintUtils.writeUnsignedInt(sSize, buf);

    if (sSize > 0) {
      for (final byte[] sKey : sortKeys) {
        VarintUtils.writeUnsignedInt(sKey.length, buf);
        buf.put(sKey);
      }
    }
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int pLength = VarintUtils.readUnsignedInt(buf);
    if (pLength > 0) {
      final byte[] pBytes = ByteArrayUtils.safeRead(buf, pLength);
      partitionKey = pBytes;
    } else {
      partitionKey = null;
    }
    final int sSize = VarintUtils.readUnsignedInt(buf);
    if (sSize > 0) {
      sortKeys = new ArrayList<>(sSize);
      for (int i = 0; i < sSize; i++) {
        final int keyLength = VarintUtils.readUnsignedInt(buf);
        final byte[] sortKey = ByteArrayUtils.safeRead(buf, keyLength);
        sortKeys.add(sortKey);
      }
    } else {
      sortKeys = null;
    }
  }
}
