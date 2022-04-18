/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.index;

import java.util.Map;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;

public interface IndexQueryStrategySPI {
  public enum QueryHint {
    MAX_RANGE_DECOMPOSITION
  }

  boolean requiresStats();

  CloseableIterator<Index> getIndices(
      DataStatisticsStore statisticsStore,
      AdapterIndexMappingStore indexMappingStore,
      QueryConstraints query,
      Index[] indices,
      InternalDataAdapter<?> adapter,
      Map<QueryHint, Object> hints);
}
