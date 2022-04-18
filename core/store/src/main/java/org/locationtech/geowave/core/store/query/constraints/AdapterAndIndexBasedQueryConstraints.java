/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.constraints;

import java.util.List;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public interface AdapterAndIndexBasedQueryConstraints extends QueryConstraints {
  QueryConstraints createQueryConstraints(
      InternalDataAdapter<?> adapter,
      Index index,
      AdapterToIndexMapping indexMapping);

  @Override
  default List<QueryFilter> createFilters(final Index index) {
    return null;
  }

  @Override
  default List<MultiDimensionalNumericData> getIndexConstraints(final Index index) {
    return null;
  }
}
