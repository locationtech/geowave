/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.aggregate;

import java.util.Map;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.AggregationQueryBuilder;
import org.locationtech.geowave.core.store.api.BinningStrategy;
import org.locationtech.geowave.core.store.query.BaseQueryBuilderImpl;
import org.locationtech.geowave.core.store.query.options.AggregateTypeQueryOptions;

public class AggregationQueryBuilderImpl<P extends Persistable, R, T, A extends AggregationQueryBuilder<P, R, T, A>>
    extends
    BaseQueryBuilderImpl<R, AggregationQuery<P, R, T>, A> implements
    AggregationQueryBuilder<P, R, T, A> {
  protected AggregateTypeQueryOptions<P, R, T> options;

  public AggregationQueryBuilderImpl() {
    this.options = new AggregateTypeQueryOptions<>();
  }

  @Override
  public AggregationQuery<P, R, T> build() {
    return new AggregationQuery<>(
        newCommonQueryOptions(),
        newAggregateTypeQueryOptions(),
        newIndexQueryOptions(),
        constraints);
  }

  @Override
  public AggregationQuery<BinningAggregationOptions<P, T>, Map<ByteArray, R>, T> buildWithBinningStrategy(
      final BinningStrategy binningStrategy,
      final int maxBins) {
    final AggregateTypeQueryOptions<BinningAggregationOptions<P, T>, Map<ByteArray, R>, T> newOptions =
        new AggregateTypeQueryOptions<>(
            new BinningAggregation(this.options.getAggregation(), binningStrategy, maxBins),
            this.options.getTypeNames());
    return new AggregationQuery<>(
        newCommonQueryOptions(),
        newOptions,
        newIndexQueryOptions(),
        constraints);
  }

  @Override
  public A aggregate(final String typeName, final Aggregation<P, R, T> aggregation) {
    this.options.setAggregation(aggregation);
    this.options.setTypeNames(new String[] {typeName});
    return (A) this;
  }

  @Override
  public A count(final String... typeNames) {
    // this forces the result type of the aggregation to be Long,
    // and will fail at runtime otherwise.
    this.options.setAggregation((Aggregation<P, R, T>) new CountAggregation());
    this.options.setTypeNames(typeNames);
    return (A) this;
  }

  private AggregateTypeQueryOptions<P, R, T> newAggregateTypeQueryOptions() {
    return options;
  }
}
