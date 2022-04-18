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
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import com.google.common.collect.Iterators;

/**
 * This Query Strategy chooses the index that satisfies the most dimensions of the underlying query
 * first and then if multiple are found it will choose the one that most closely preserves locality.
 * It won't be optimized for a single prefix query but it will choose the index with the most
 * dimensions defined, enabling more fine-grained contraints given a larger set of indexable ranges.
 */
public class ChooseHeuristicMatchIndexQueryStrategy implements IndexQueryStrategySPI {
  public static final String NAME = "Heuristic Match";

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public CloseableIterator<Index> getIndices(
      final DataStatisticsStore statisticsStore,
      final AdapterIndexMappingStore indexMappingStore,
      final QueryConstraints query,
      final Index[] indices,
      final InternalDataAdapter<?> adapter,
      final Map<QueryHint, Object> hints) {
    return new CloseableIterator.Wrapper<>(
        Iterators.singletonIterator(
            BaseDataStoreUtils.chooseBestIndex(indices, query, adapter, indexMappingStore)));
  }

  @Override
  public boolean requiresStats() {
    return false;
  }
}
