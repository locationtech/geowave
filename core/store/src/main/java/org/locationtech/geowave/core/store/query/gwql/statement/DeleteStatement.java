/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql.statement;

import javax.annotation.Nullable;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.query.filter.expression.Filter;
import org.locationtech.geowave.core.store.query.gwql.ResultSet;
import org.locationtech.geowave.core.store.query.gwql.SingletonResultSet;
import com.google.common.collect.Lists;

/**
 * Deletes data from a GeoWave store.
 */
public class DeleteStatement<T> implements Statement {

  private final DataStore dataStore;
  private final DataTypeAdapter<T> adapter;
  private final Filter filter;

  /**
   *
   * @param typeName the type to delete data from
   * @param filter delete features that match this filter
   */
  public DeleteStatement(
      final DataStore dataStore,
      final DataTypeAdapter<T> adapter,
      final @Nullable Filter filter) {
    this.dataStore = dataStore;
    this.adapter = adapter;
    this.filter = filter;
  }

  @Override
  public ResultSet execute(final String... authorizations) {
    final QueryBuilder<T, ?> bldr =
        QueryBuilder.newBuilder(adapter.getDataClass()).addTypeName(adapter.getTypeName());
    bldr.setAuthorizations(authorizations);
    if (filter != null) {
      bldr.filter(filter);
    }
    final Query<T> query = bldr.build();
    final boolean success = dataStore.delete(query);
    return new SingletonResultSet(
        Lists.newArrayList("SUCCESS"),
        Lists.newArrayList(Boolean.class),
        Lists.newArrayList(success));
  }

  /**
   * @return the type that data will be deleted from
   */
  public DataTypeAdapter<T> getAdapter() {
    return adapter;
  }

  /**
   * @return the delete filter
   */
  public Filter getFilter() {
    return filter;
  }

}
