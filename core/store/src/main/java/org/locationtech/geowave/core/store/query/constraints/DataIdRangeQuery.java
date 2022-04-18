/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.constraints;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.filter.DataIdRangeQueryFilter;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class DataIdRangeQuery implements QueryConstraints {
  private byte[] startDataIdInclusive;
  private byte[] endDataIdInclusive;
  private boolean reverse;

  public DataIdRangeQuery() {}

  public DataIdRangeQuery(final byte[] startDataIdInclusive, final byte[] endDataIdInclusive) {
    this(startDataIdInclusive, endDataIdInclusive, false);
  }

  public DataIdRangeQuery(
      final byte[] startDataIdInclusive,
      final byte[] endDataIdInclusive,
      final boolean reverse) {
    this.startDataIdInclusive = startDataIdInclusive;
    this.endDataIdInclusive = endDataIdInclusive;
    this.reverse = reverse;
  }


  public byte[] getStartDataIdInclusive() {
    return startDataIdInclusive;
  }

  public byte[] getEndDataIdInclusive() {
    return endDataIdInclusive;
  }

  public boolean isReverse() {
    return reverse;
  }

  @Override
  public List<QueryFilter> createFilters(final Index index) {
    final List<QueryFilter> filters = new ArrayList<>();
    filters.add(new DataIdRangeQueryFilter(startDataIdInclusive, endDataIdInclusive));
    return filters;
  }

  @Override
  public List<MultiDimensionalNumericData> getIndexConstraints(final Index index) {
    return Collections.emptyList();
  }

  @Override
  public byte[] toBinary() {
    return new DataIdRangeQueryFilter(startDataIdInclusive, endDataIdInclusive).toBinary();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final DataIdRangeQueryFilter filter = new DataIdRangeQueryFilter();
    filter.fromBinary(bytes);
    startDataIdInclusive = filter.getStartDataIdInclusive();
    endDataIdInclusive = filter.getEndDataIdInclusive();
  }
}
