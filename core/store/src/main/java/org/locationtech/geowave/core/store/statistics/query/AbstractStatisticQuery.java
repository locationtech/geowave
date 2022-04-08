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
import org.locationtech.geowave.core.store.api.StatisticQuery;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.statistics.StatisticType;

/**
 * Base statistic query implementation.
 */
public abstract class AbstractStatisticQuery<V extends StatisticValue<R>, R> implements
    StatisticQuery<V, R> {

  private final StatisticType<V> statisticType;
  private final String tag;
  private final BinConstraints binConstraints;
  private final String[] authorizations;

  public AbstractStatisticQuery(
      final StatisticType<V> statisticType,
      final String tag,
      final BinConstraints binConstraints,
      final String[] authorizations) {
    this.statisticType = statisticType;
    this.tag = tag;
    this.binConstraints = binConstraints;
    this.authorizations = authorizations;
  }

  @Override
  public StatisticType<V> statisticType() {
    return statisticType;
  }

  @Override
  public String tag() {
    return tag;
  }

  @Override
  public BinConstraints binConstraints() {
    return binConstraints;
  }

  @Override
  public String[] authorizations() {
    return authorizations;
  }

}
