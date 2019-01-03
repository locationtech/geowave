/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p>See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter;

import java.nio.ByteBuffer;
import java.util.Objects;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

public class InsertionIdQueryFilter implements QueryFilter {
  private byte[] partitionKey;
  private byte[] sortKey;
  private byte[] dataId;

  public InsertionIdQueryFilter() {}

  public InsertionIdQueryFilter(
      final ByteArray partitionKey, final ByteArray sortKey, final ByteArray dataId) {
    this.partitionKey = partitionKey != null ? partitionKey.getBytes() : new byte[] {};
    this.sortKey = sortKey != null ? sortKey.getBytes() : new byte[] {};
    this.dataId = dataId != null ? dataId.getBytes() : new byte[] {};
  }

  @Override
  public boolean accept(
      final CommonIndexModel indexModel, final IndexedPersistenceEncoding persistenceEncoding) {
    return Objects.deepEquals(
            partitionKey,
            persistenceEncoding.getInsertionPartitionKey() != null
                ? persistenceEncoding.getInsertionPartitionKey().getBytes()
                : new byte[] {})
        && Objects.deepEquals(
            sortKey,
            persistenceEncoding.getInsertionSortKey() != null
                ? persistenceEncoding.getInsertionSortKey().getBytes()
                : new byte[] {})
        && Objects.deepEquals(
            dataId,
            persistenceEncoding.getDataId() != null
                ? persistenceEncoding.getDataId().getBytes()
                : new byte[] {});
  }

  @Override
  public byte[] toBinary() {
    final ByteBuffer buf =
        ByteBuffer.allocate(
            partitionKey.length
                + sortKey.length
                + dataId.length
                + VarintUtils.unsignedIntByteLength(partitionKey.length)
                + VarintUtils.unsignedIntByteLength(sortKey.length));
    VarintUtils.writeUnsignedInt(partitionKey.length, buf);
    buf.put(partitionKey);
    VarintUtils.writeUnsignedInt(sortKey.length, buf);
    buf.put(sortKey);
    buf.put(dataId);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    partitionKey = new byte[VarintUtils.readUnsignedInt(buf)];
    buf.get(partitionKey);
    sortKey = new byte[VarintUtils.readUnsignedInt(buf)];
    buf.get(sortKey);
    dataId = new byte[buf.remaining()];
    buf.get(dataId);
  }
}
