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
import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

/**
 * This class wraps a list of filters into a single filter such that if any one filter fails this
 * class will fail acceptance.
 */
public class FilterList implements QueryFilter {
  protected List<QueryFilter> filters;
  protected boolean logicalAnd = true;

  public FilterList() {}

  protected FilterList(final boolean logicalAnd) {
    this.logicalAnd = logicalAnd;
  }

  public FilterList(final List<QueryFilter> filters) {
    this.filters = filters;
  }

  public FilterList(final boolean logicalAnd, final List<QueryFilter> filters) {
    this.logicalAnd = logicalAnd;
    this.filters = filters;
  }

  @Override
  public boolean accept(
      final CommonIndexModel indexModel,
      final IndexedPersistenceEncoding<?> entry) {
    if (filters == null) {
      return true;
    }
    for (final QueryFilter filter : filters) {
      final boolean ok = filter.accept(indexModel, entry);
      if (!ok && logicalAnd) {
        return false;
      }
      if (ok && !logicalAnd) {
        return true;
      }
    }
    return logicalAnd;
  }

  @Override
  public byte[] toBinary() {
    int byteBufferLength = VarintUtils.unsignedIntByteLength(filters.size()) + 1;
    final List<byte[]> filterBinaries = new ArrayList<>(filters.size());
    for (final QueryFilter filter : filters) {
      final byte[] filterBinary = PersistenceUtils.toBinary(filter);
      byteBufferLength +=
          (VarintUtils.unsignedIntByteLength(filterBinary.length) + filterBinary.length);
      filterBinaries.add(filterBinary);
    }
    final ByteBuffer buf = ByteBuffer.allocate(byteBufferLength);
    buf.put((byte) (logicalAnd ? 1 : 0));
    VarintUtils.writeUnsignedInt(filters.size(), buf);
    for (final byte[] filterBinary : filterBinaries) {
      VarintUtils.writeUnsignedInt(filterBinary.length, buf);
      buf.put(filterBinary);
    }
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    logicalAnd = buf.get() > 0;
    final int numFilters = VarintUtils.readUnsignedInt(buf);
    ByteArrayUtils.verifyBufferSize(buf, numFilters);
    filters = new ArrayList<>(numFilters);
    for (int i = 0; i < numFilters; i++) {
      final byte[] filter = ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
      filters.add((QueryFilter) PersistenceUtils.fromBinary(filter));
    }
  }
}
