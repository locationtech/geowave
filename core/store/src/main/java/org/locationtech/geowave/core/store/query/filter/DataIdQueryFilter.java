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
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

public class DataIdQueryFilter implements QueryFilter {
  private Set<ByteArray> dataIds;

  public DataIdQueryFilter() {}

  public DataIdQueryFilter(final byte[][] dataIds) {
    this.dataIds = Arrays.stream(dataIds).map(i -> new ByteArray(i)).collect(Collectors.toSet());
  }

  @Override
  public boolean accept(
      final CommonIndexModel indexModel,
      final IndexedPersistenceEncoding persistenceEncoding) {
    return dataIds.contains(new ByteArray(persistenceEncoding.getDataId()));
  }


  @Override
  public byte[] toBinary() {
    int size = VarintUtils.unsignedIntByteLength(dataIds.size());
    for (final ByteArray id : dataIds) {
      size += (id.getBytes().length + VarintUtils.unsignedIntByteLength(id.getBytes().length));
    }
    final ByteBuffer buf = ByteBuffer.allocate(size);
    VarintUtils.writeUnsignedInt(dataIds.size(), buf);
    for (final ByteArray id : dataIds) {
      final byte[] idBytes = id.getBytes();
      VarintUtils.writeUnsignedInt(idBytes.length, buf);
      buf.put(idBytes);
    }
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int size = VarintUtils.readUnsignedInt(buf);
    dataIds = new HashSet<>(size);
    for (int i = 0; i < size; i++) {
      final int bsize = VarintUtils.readUnsignedInt(buf);
      final byte[] dataIdBytes = ByteArrayUtils.safeRead(buf, bsize);
      dataIds.add(new ByteArray(dataIdBytes));
    }
  }
}
