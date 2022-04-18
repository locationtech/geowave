/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

public class PrefixIdQueryFilter implements QueryFilter {
  private byte[] partitionKey;
  private byte[] sortKeyPrefix;

  public PrefixIdQueryFilter() {}

  public PrefixIdQueryFilter(final byte[] partitionKey, final byte[] sortKeyPrefix) {
    this.partitionKey = (partitionKey != null) ? partitionKey : new byte[0];
    this.sortKeyPrefix = sortKeyPrefix;
  }

  @Override
  public boolean accept(
      final CommonIndexModel indexModel,
      final IndexedPersistenceEncoding persistenceEncoding) {
    final byte[] otherPartitionKey = persistenceEncoding.getInsertionPartitionKey();
    final byte[] otherPartitionKeyBytes =
        (otherPartitionKey != null) ? otherPartitionKey : new byte[0];
    final byte[] sortKey = persistenceEncoding.getInsertionSortKey();
    return (Arrays.equals(sortKeyPrefix, Arrays.copyOf(sortKey, sortKeyPrefix.length))
        && Arrays.equals(partitionKey, otherPartitionKeyBytes));
  }

  @Override
  public byte[] toBinary() {
    final ByteBuffer buf =
        ByteBuffer.allocate(
            partitionKey.length
                + sortKeyPrefix.length
                + VarintUtils.unsignedIntByteLength(partitionKey.length));
    VarintUtils.writeUnsignedInt(partitionKey.length, buf);
    buf.put(partitionKey);
    buf.put(sortKeyPrefix);

    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    partitionKey = ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
    sortKeyPrefix = new byte[buf.remaining()];
    buf.get(sortKeyPrefix);
  }
}
