/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.query;

import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.statistics.StatisticType;

/**
 * Statistic query implementation for data type statistics.
 */
public class DataTypeStatisticQuery<V extends StatisticValue<R>, R> extends
    AbstractStatisticQuery<V, R> {

  private final String typeName;

  public DataTypeStatisticQuery(
      final StatisticType<V> statisticType,
      final String typeName,
      final String tag,
      final BinConstraints binConstraints,
      final String[] authorizations) {
    super(statisticType, tag, binConstraints, authorizations);
    this.typeName = typeName;
  }

  public String typeName() {
    return typeName;
  }
}
