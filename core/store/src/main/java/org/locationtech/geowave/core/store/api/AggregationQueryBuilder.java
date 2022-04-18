/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import java.util.Map;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.query.BaseQueryBuilder;
import org.locationtech.geowave.core.store.query.aggregate.AggregationQueryBuilderImpl;
import org.locationtech.geowave.core.store.query.aggregate.BinningAggregationOptions;

/**
 * This and its extensions should be used to create an AggregationQuery.
 *
 * @param <P> input type for the aggregation
 * @param <R> result type for the aggregation
 * @param <T> data type of the entries for the aggregation
 * @param <A> the type of the builder, useful for extending this builder and maintaining the builder
 *        type
 */
public interface AggregationQueryBuilder<P extends Persistable, R, T, A extends AggregationQueryBuilder<P, R, T, A>>
    extends
    BaseQueryBuilder<R, AggregationQuery<P, R, T>, A> {
  /**
   * get a new default implementation of the builder
   *
   * @return an AggregationQueryBuilder
   */
  static <P extends Persistable, R, T, A extends AggregationQueryBuilder<P, R, T, A>> AggregationQueryBuilder<P, R, T, A> newBuilder() {
    return new AggregationQueryBuilderImpl<>();
  }

  /**
   * Instead of having a scalar aggregation, bin the results by a given strategy.
   *
   * Calling this produces a 'meta aggregation', which uses the current aggregation along with the
   * binning strategy to perform aggregations.
   *
   * entries of type {@link T} are binned using the strategy. When a new bin is required, it is
   * created by instantiating a fresh aggregation (based on the current aggregation)
   *
   * @param binningStrategy The strategy to bin the hashes of given data.
   * @param maxBins The maximum bins to allow in the aggregation. -1 for no limit.
   * @return A complete aggregation query, ready to consume data.
   */
  AggregationQuery<BinningAggregationOptions<P, T>, Map<ByteArray, R>, T> buildWithBinningStrategy(
      BinningStrategy binningStrategy,
      int maxBins);

  /**
   * Provide the Aggregation function and the type name to apply the aggregation on
   *
   * @param typeName the type name of the dataset
   * @param aggregation the aggregation function
   * @return an aggregation
   */
  A aggregate(String typeName, Aggregation<P, R, T> aggregation);

  /**
   * this is a convenience method to set the count aggregation if no type names are given it is
   * assumed to count every type
   *
   * @param typeNames the type names to count results
   * @return a count of how many entries match the query criteria
   */
  A count(String... typeNames);
}
