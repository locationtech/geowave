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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.filter.DataIdQueryFilter;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class DataIdQuery implements QueryConstraints {
  private byte[][] dataIds;

  public DataIdQuery() {}

  public DataIdQuery(final byte[] dataId) {
    dataIds = new byte[][] {dataId};
  }

  public DataIdQuery(final byte[][] dataIds) {
    this.dataIds = dataIds;
  }

  public byte[][] getDataIds() {
    return dataIds;
  }

  @Override
  public List<QueryFilter> createFilters(final Index index) {
    final List<QueryFilter> filters = new ArrayList<>();
    filters.add(new DataIdQueryFilter(dataIds));
    return filters;
  }

  @Override
  public List<MultiDimensionalNumericData> getIndexConstraints(final Index index) {
    return Collections.emptyList();
  }

  @Override
  public byte[] toBinary() {
    final int length =
        Arrays.stream(dataIds).map(
            i -> i.length + VarintUtils.unsignedIntByteLength(i.length)).reduce(0, Integer::sum);
    final ByteBuffer buf =
        ByteBuffer.allocate(length + VarintUtils.unsignedIntByteLength(dataIds.length));
    VarintUtils.writeUnsignedInt(dataIds.length, buf);
    Arrays.stream(dataIds).forEach(i -> {
      VarintUtils.writeUnsignedInt(i.length, buf);
      buf.put(i);
    });
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int length = VarintUtils.readUnsignedInt(buf);
    ByteArrayUtils.verifyBufferSize(buf, length);
    final byte[][] dataIds = new byte[length][];
    for (int i = 0; i < length; i++) {
      final int iLength = VarintUtils.readUnsignedInt(buf);
      dataIds[i] = ByteArrayUtils.safeRead(buf, iLength);;
    }
    this.dataIds = dataIds;
  }
}
