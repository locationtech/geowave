/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import java.util.Arrays;
import org.locationtech.geowave.core.store.query.BaseQueryBuilder;
import org.locationtech.geowave.core.store.query.QueryBuilderImpl;

/**
 * A QueryBuilder can be used to easily construct a query which can be used to retrieve data from a
 * GeoWave datastore.
 *
 * @param <T> the data type
 * @param <R> the type of the builder so that extensions of this builder can maintain type
 */
public interface QueryBuilder<T, R extends QueryBuilder<T, R>> extends
    BaseQueryBuilder<T, Query<T>, R> {
  /**
   * retrieve all data types (this is the default behavior)
   *
   * @return this builder
   */
  R allTypes();

  /**
   * add a type name to filter by
   *
   * @param typeName the type name
   * @return this builder
   */
  R addTypeName(String typeName);

  /**
   * set the type names to filter by - an empty array will filter by all types.
   *
   * @param typeNames the type names
   * @return this builder
   */
  R setTypeNames(String[] typeNames);

  /**
   * Subset fields by field names. If empty it will get all fields.
   *
   * @param typeName the type name
   * @param fieldNames the field names to subset
   * @return the entry
   */
  R subsetFields(String typeName, String... fieldNames);

  /**
   * retrieve all fields (this is the default behavior)
   *
   * @return this builder
   */
  R allFields();

  /**
   * get a default query builder
   *
   * @return the new builder
   */
  static <T> QueryBuilder<T, ?> newBuilder() {
    return new QueryBuilderImpl<>();
  }

  static <T> QueryBuilder<T, ?> newBuilder(Class<T> clazz) {
    return new QueryBuilderImpl<>();
  }

  @SafeVarargs
  static <T> QueryBuilder<T, ?> newBuilder(
      DataTypeAdapter<T> adapter,
      DataTypeAdapter<T>... otherAdapters) {
    QueryBuilder<T, ?> queryBuilder = new QueryBuilderImpl<>();
    queryBuilder.addTypeName(adapter.getTypeName());
    if (otherAdapters != null && otherAdapters.length > 0) {
      Arrays.stream(otherAdapters).forEach(a -> queryBuilder.addTypeName(a.getTypeName()));
    }
    return queryBuilder;
  }
}
