/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query;

import org.locationtech.geowave.core.store.api.QueryConstraintsFactory;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraintsFactoryImpl;
import org.locationtech.geowave.core.store.query.filter.expression.Filter;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions.HintKey;

/**
 * A base class for building queries
 *
 * @param <T> the type of the entries
 * @param <Q> the type of query (AggregationQuery or Query)
 * @param <R> the type of the builder, useful for extensions of this to maintain type
 */
public interface BaseQueryBuilder<T, Q extends BaseQuery<T, ?>, R extends BaseQueryBuilder<T, Q, R>> {
  /**
   * Choose the appropriate index from all available indices (the default behavior).
   *
   * @return this builder
   */
  R allIndices();

  /**
   * Query only using the specified index.
   *
   * @param indexName the name of the index
   * @return this builder
   */
  R indexName(String indexName);

  /**
   * Add an authorization to the query.
   *
   * @param authorization the authorization
   * @return this builder
   */
  R addAuthorization(String authorization);

  /**
   * Set the authorizations for this query (authorizations are intersected with row visibilities to
   * determine access).
   *
   * @param authorizations the authorizations
   * @return this builder
   */
  R setAuthorizations(String[] authorizations);

  /**
   * Set to no authorizations (default behavior).
   *
   * @return this builder
   */
  R noAuthorizations();

  /**
   * Set no limit for the number of entries (default behavior).
   *
   * @return this builder
   */
  R noLimit();

  /**
   * Set the limit for the number of entries.
   *
   * @param limit the limit
   * @return this builder
   */
  R limit(int limit);

  /**
   * Add a hint to the query.
   * 
   * @param key the hint key
   * @param value the hint value
   * @return this builder
   */
  <HintValueType> R addHint(HintKey<HintValueType> key, HintValueType value);

  /**
   * Clear out any hints (default is no hints).
   *
   * @return this builder
   */
  R noHints();

  /**
   * Use the specified constraints. Constraints can most easily be define by using the
   * constraintFactory().
   *
   * @param constraints the constraints
   * @return this builder
   */
  R constraints(QueryConstraints constraints);

  /**
   * Constrain the query with a filter expression. This is an alternate way of providing constraints
   * and will override any other constraints specified.
   * 
   * @param filter the filter expression
   * @return this builder
   */
  R filter(Filter filter);

  /**
   * This is the easiest approach to defining a set of constraints and can be used to create the
   * constraints that are provided to the constraints method.
   *
   * @return a constraints factory
   */
  default QueryConstraintsFactory constraintsFactory() {
    return QueryConstraintsFactoryImpl.SINGLETON_INSTANCE;
  }

  /**
   * Build the query represented by this builder.
   *
   * @return the query
   */
  Q build();
}
