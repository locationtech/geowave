/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.index;

import java.util.Map;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass;
import org.opengis.feature.simple.SimpleFeature;
import org.spark_project.guava.collect.Iterators;

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
      final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> stats,
      final BasicQueryByClass query,
      final Index[] indices,
      final Map<QueryHint, Object> hints) {
    return new CloseableIterator.Wrapper<>(
        Iterators.singletonIterator(BaseDataStoreUtils.chooseBestIndex(indices, query)));
  }

  @Override
  public boolean requiresStats() {
    return false;
  }
}
