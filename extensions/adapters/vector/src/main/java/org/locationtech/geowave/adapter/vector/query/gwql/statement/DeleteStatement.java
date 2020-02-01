/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.gwql.statement;

import javax.annotation.Nullable;
import org.locationtech.geowave.adapter.vector.query.gwql.QualifiedTypeName;
import org.locationtech.geowave.adapter.vector.query.gwql.ResultSet;
import org.locationtech.geowave.adapter.vector.query.gwql.SingletonResultSet;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.spark_project.guava.collect.Lists;

/**
 * Deletes data from a GeoWave store.
 */
public class DeleteStatement implements Statement {

  private final QualifiedTypeName typeName;
  private final Filter filter;

  /**
   * 
   * @param typeName the type to delete data from
   * @param filter delete features that match this filter
   */
  public DeleteStatement(final QualifiedTypeName typeName, final @Nullable Filter filter) {
    this.typeName = typeName;
    this.filter = filter;
  }

  @Override
  public ResultSet execute(final DataStore dataStore) {
    final VectorQueryBuilder bldr =
        VectorQueryBuilder.newBuilder().addTypeName(typeName.typeName());
    if (filter != null) {
      bldr.constraints(bldr.constraintsFactory().filterConstraints(filter));
    }
    final Query<SimpleFeature> query = bldr.build();
    final boolean success = dataStore.delete(query);
    return new SingletonResultSet(
        Lists.newArrayList("SUCCESS"),
        Lists.newArrayList(Boolean.class),
        Lists.newArrayList(success));
  }

  /**
   * @return the type that data will be deleted from
   */
  public QualifiedTypeName typeName() {
    return typeName;
  }

  /**
   * @return the delete filter
   */
  public Filter filter() {
    return filter;
  }

  @Override
  public String getStoreName() {
    return typeName.storeName();
  }

}
