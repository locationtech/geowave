/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter.statistics;

public class BaseStatisticsQueryBuilder<R> extends
    StatisticsQueryBuilderImpl<R, BaseStatisticsQueryBuilder<R>> {

  public BaseStatisticsQueryBuilder(
      final StatisticsType<R, BaseStatisticsQueryBuilder<R>> statsType) {
    this.statsType = statsType;
  }
}
