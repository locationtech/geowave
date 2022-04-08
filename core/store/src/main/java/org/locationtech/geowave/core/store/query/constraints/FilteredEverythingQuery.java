/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.constraints;

import java.util.Collections;
import java.util.List;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

/**
 * Fully cans the index, but passes every entry through the given filters.
 */
public class FilteredEverythingQuery implements QueryConstraints {
  private List<QueryFilter> filters;

  public FilteredEverythingQuery() {}

  public FilteredEverythingQuery(final List<QueryFilter> filters) {
    this.filters = filters;
  }

  @Override
  public List<QueryFilter> createFilters(final Index index) {
    return filters;
  }

  @Override
  public List<MultiDimensionalNumericData> getIndexConstraints(final Index index) {
    return Collections.emptyList();
  }

  @Override
  public byte[] toBinary() {
    return PersistenceUtils.toBinary(filters);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void fromBinary(final byte[] bytes) {
    filters = (List) PersistenceUtils.fromBinaryAsList(bytes);
  }

  @Override
  public int hashCode() {
    return filters.hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof FilteredEverythingQuery)) {
      return false;
    }
    return filters.equals(((FilteredEverythingQuery) obj).filters);
  }
}
