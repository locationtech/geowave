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
import org.locationtech.geowave.core.store.statistics.adapter.DataTypeStatisticType;

/**
 * Statistic query builder implementation for data type statistics.
 */
public class DataTypeStatisticQueryBuilder<V extends StatisticValue<R>, R> extends
    AbstractStatisticQueryBuilder<V, R, DataTypeStatisticQueryBuilder<V, R>> {

  protected String typeName = null;

  public DataTypeStatisticQueryBuilder(final DataTypeStatisticType<V> type) {
    super(type);
  }

  public DataTypeStatisticQueryBuilder<V, R> typeName(final String typeName) {
    this.typeName = typeName;
    return this;
  }

  @Override
  public AbstractStatisticQuery<V, R> build() {
    final String[] authorizationsArray = authorizations.toArray(new String[authorizations.size()]);
    return new DataTypeStatisticQuery<>(
        statisticType,
        typeName,
        tag,
        binConstraints,
        authorizationsArray);
  }
}
