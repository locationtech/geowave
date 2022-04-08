/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.constraints;

import java.nio.ByteBuffer;
import java.util.List;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

/**
 * Allows the caller to provide explicit numeric constraints and filters for a query.
 */
public class ExplicitFilteredQuery implements QueryConstraints {

  private List<QueryFilter> filters;
  private List<MultiDimensionalNumericData> constraints;

  public ExplicitFilteredQuery() {}

  public ExplicitFilteredQuery(
      final List<MultiDimensionalNumericData> constraints,
      final List<QueryFilter> filters) {
    this.constraints = constraints;
    this.filters = filters;
  }

  @Override
  public byte[] toBinary() {
    final byte[] filterBytes = PersistenceUtils.toBinary(filters);
    final byte[] constraintBytes = PersistenceUtils.toBinary(constraints);
    final ByteBuffer buffer =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(filterBytes.length)
                + VarintUtils.unsignedIntByteLength(constraintBytes.length)
                + filterBytes.length
                + constraintBytes.length);
    VarintUtils.writeUnsignedInt(filterBytes.length, buffer);
    buffer.put(filterBytes);
    VarintUtils.writeUnsignedInt(constraintBytes.length, buffer);
    buffer.put(constraintBytes);
    return buffer.array();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void fromBinary(byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    final byte[] filterBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(filterBytes);
    final byte[] constraintBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(constraintBytes);
    filters = (List) PersistenceUtils.fromBinaryAsList(filterBytes);
    constraints = (List) PersistenceUtils.fromBinaryAsList(constraintBytes);
  }

  @Override
  public List<QueryFilter> createFilters(Index index) {
    return filters;
  }

  @Override
  public List<MultiDimensionalNumericData> getIndexConstraints(Index index) {
    return constraints;
  }

}
