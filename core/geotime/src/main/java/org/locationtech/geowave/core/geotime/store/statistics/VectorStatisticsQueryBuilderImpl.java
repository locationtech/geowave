/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.statistics;

import org.locationtech.geowave.core.geotime.store.query.api.VectorStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.FieldStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsQueryBuilderImpl;
import org.locationtech.jts.geom.Envelope;
import org.threeten.extra.Interval;

public class VectorStatisticsQueryBuilderImpl<R> extends
    StatisticsQueryBuilderImpl<R, VectorStatisticsQueryBuilder<R>> implements
    VectorStatisticsQueryBuilder<R> {
  @Override
  public QueryByVectorStatisticsTypeFactory factory() {
    return QueryByVectorStatisticsTypeFactoryImpl.SINGLETON;
  }

  protected static class QueryByVectorStatisticsTypeFactoryImpl extends
      QueryByStatisticsTypeFactoryImpl implements
      QueryByVectorStatisticsTypeFactory {
    private static QueryByVectorStatisticsTypeFactory SINGLETON =
        new QueryByVectorStatisticsTypeFactoryImpl();

    @Override
    public FieldStatisticsQueryBuilder<Envelope> bbox() {
      return FeatureBoundingBoxStatistics.STATS_TYPE.newBuilder();
    }

    @Override
    public FieldStatisticsQueryBuilder<Interval> timeRange() {
      return FeatureTimeRangeStatistics.STATS_TYPE.newBuilder();
    }
  }
}
