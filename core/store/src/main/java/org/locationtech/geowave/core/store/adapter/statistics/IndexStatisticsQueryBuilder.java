/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p>See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter.statistics;

public class IndexStatisticsQueryBuilder<R>
    extends StatisticsQueryBuilderImpl<R, IndexStatisticsQueryBuilder<R>> {
  private String indexName;

  public IndexStatisticsQueryBuilder(
      final StatisticsType<R, IndexStatisticsQueryBuilder<R>> statsType) {
    this.statsType = statsType;
  }

  public IndexStatisticsQueryBuilder<R> indexName(final String indexName) {
    this.indexName = indexName;
    return this;
  }

  @Override
  protected String extendedId() {
    return indexName;
  }
}
