/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.query;

import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.statistics.index.IndexStatisticType;

/**
 * Statistic query builder implementation for index statistics.
 */
public class IndexStatisticQueryBuilder<V extends StatisticValue<R>, R> extends
    AbstractStatisticQueryBuilder<V, R, IndexStatisticQueryBuilder<V, R>> {

  protected String indexName = null;

  public IndexStatisticQueryBuilder(final IndexStatisticType<V> type) {
    super(type);
  }

  public IndexStatisticQueryBuilder<V, R> indexName(final String indexName) {
    this.indexName = indexName;
    return this;
  }

  @Override
  public AbstractStatisticQuery<V, R> build() {
    final String[] authorizationsArray = authorizations.toArray(new String[authorizations.size()]);
    return new IndexStatisticQuery<>(
        statisticType,
        indexName,
        tag,
        binConstraints,
        authorizationsArray);
  }
}
