/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.constraints;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.filter.InsertionIdQueryFilter;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class InsertionIdQuery implements QueryConstraints {
  private byte[] partitionKey;
  private byte[] sortKey;
  private byte[] dataId;

  public InsertionIdQuery() {}

  public InsertionIdQuery(final byte[] partitionKey, final byte[] sortKey, final byte[] dataId) {
    this.partitionKey = partitionKey == null ? new byte[0] : partitionKey;
    this.sortKey = sortKey == null ? new byte[0] : sortKey;
    this.dataId = dataId == null ? new byte[0] : dataId;
  }

  public byte[] getPartitionKey() {
    return partitionKey;
  }

  public byte[] getSortKey() {
    return sortKey;
  }

  public byte[] getDataId() {
    return dataId;
  }

  @Override
  public List<QueryFilter> createFilters(final Index index) {
    final List<QueryFilter> filters = new ArrayList<>();
    filters.add(new InsertionIdQueryFilter(partitionKey, sortKey, dataId));
    return filters;
  }

  @Override
  public List<MultiDimensionalNumericData> getIndexConstraints(final Index index) {
    return Collections.emptyList();
  }

  @Override
  public byte[] toBinary() {
    byte[] sortKeyBinary, partitionKeyBinary, dataIdBinary;
    if (partitionKey != null) {
      partitionKeyBinary = partitionKey;
    } else {
      partitionKeyBinary = new byte[0];
    }
    if (sortKey != null) {
      sortKeyBinary = sortKey;
    } else {
      sortKeyBinary = new byte[0];
    }
    if (dataId != null) {
      dataIdBinary = dataId;
    } else {
      dataIdBinary = new byte[0];
    }
    final ByteBuffer buf =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(partitionKeyBinary.length)
                + VarintUtils.unsignedIntByteLength(sortKeyBinary.length)
                + sortKeyBinary.length
                + partitionKeyBinary.length);
    VarintUtils.writeUnsignedInt(partitionKeyBinary.length, buf);
    buf.put(partitionKeyBinary);
    VarintUtils.writeUnsignedInt(sortKeyBinary.length, buf);
    buf.put(sortKeyBinary);
    buf.put(dataIdBinary);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int partitionKeyBinaryLength = VarintUtils.readUnsignedInt(buf);
    if (partitionKeyBinaryLength == 0) {
      partitionKey = null;
    } else {
      partitionKey = ByteArrayUtils.safeRead(buf, partitionKeyBinaryLength);
    }
    final int sortKeyBinaryLength = VarintUtils.readUnsignedInt(buf);
    if (sortKeyBinaryLength == 0) {
      sortKey = null;
    } else {
      sortKey = ByteArrayUtils.safeRead(buf, sortKeyBinaryLength);
    }
    final byte[] dataIdBinary = new byte[buf.remaining()];
    if (dataIdBinary.length == 0) {
      dataId = null;
    } else {
      buf.get(dataIdBinary);
      dataId = dataIdBinary;
    }
  }
}
