/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.aggregate;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.AggregationQueryBuilder;
import org.locationtech.geowave.core.store.query.BaseQueryBuilderImpl;
import org.locationtech.geowave.core.store.query.options.AggregateTypeQueryOptions;

public class AggregationQueryBuilderImpl<P extends Persistable, R, T, A extends AggregationQueryBuilder<P, R, T, A>>
    extends
    BaseQueryBuilderImpl<R, AggregationQuery<P, R, T>, A> implements
    AggregationQueryBuilder<P, R, T, A> {
  protected AggregateTypeQueryOptions<P, R, T> options;

  @Override
  public AggregationQuery<P, R, T> build() {
    return new AggregationQuery<>(
        newCommonQueryOptions(),
        newAggregateTypeQueryOptions(),
        newIndexQueryOptions(),
        constraints);
  }

  @Override
  public A count(final String... typeNames) {
    this.options = new AggregateTypeQueryOptions<>((Aggregation) new CountAggregation(), typeNames);
    return (A) this;
  }

  @Override
  public A aggregate(final String typeName, final Aggregation<P, R, T> aggregation) {
    this.options = new AggregateTypeQueryOptions<>(aggregation, new String[] {typeName});
    return (A) this;
  }

  protected AggregateTypeQueryOptions<P, R, T> newAggregateTypeQueryOptions() {
    return options;
  }
}
