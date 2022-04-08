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
import org.locationtech.geowave.core.store.query.filter.PrefixIdQueryFilter;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class PrefixIdQuery implements QueryConstraints {
  private byte[] sortKeyPrefix;
  private byte[] partitionKey;

  public PrefixIdQuery() {}

  public PrefixIdQuery(final byte[] partitionKey, final byte[] sortKeyPrefix) {
    this.partitionKey = partitionKey;
    this.sortKeyPrefix = sortKeyPrefix;
  }

  public byte[] getPartitionKey() {
    return partitionKey;
  }

  public byte[] getSortKeyPrefix() {
    return sortKeyPrefix;
  }

  @Override
  public List<QueryFilter> createFilters(final Index index) {
    final List<QueryFilter> filters = new ArrayList<>();
    filters.add(new PrefixIdQueryFilter(partitionKey, sortKeyPrefix));
    return filters;
  }

  @Override
  public List<MultiDimensionalNumericData> getIndexConstraints(final Index index) {
    return Collections.emptyList();
  }

  @Override
  public byte[] toBinary() {
    byte[] sortKeyPrefixBinary, partitionKeyBinary;
    if (partitionKey != null) {
      partitionKeyBinary = partitionKey;
    } else {
      partitionKeyBinary = new byte[0];
    }
    if (sortKeyPrefix != null) {
      sortKeyPrefixBinary = sortKeyPrefix;
    } else {
      sortKeyPrefixBinary = new byte[0];
    }
    final ByteBuffer buf =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(partitionKeyBinary.length)
                + sortKeyPrefixBinary.length
                + partitionKeyBinary.length);
    VarintUtils.writeUnsignedInt(partitionKeyBinary.length, buf);
    buf.put(partitionKeyBinary);
    buf.put(sortKeyPrefixBinary);
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
    final byte[] sortKeyPrefixBinary = new byte[buf.remaining()];
    if (sortKeyPrefixBinary.length == 0) {
      sortKeyPrefix = null;
    } else {
      buf.get(sortKeyPrefixBinary);
      sortKeyPrefix = sortKeyPrefixBinary;
    }
  }
}
