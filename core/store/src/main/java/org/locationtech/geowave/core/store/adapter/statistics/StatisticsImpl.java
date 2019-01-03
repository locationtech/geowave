/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p>See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter.statistics;

import org.locationtech.geowave.core.store.api.Statistics;

public class StatisticsImpl<R> implements Statistics<R> {
  private final R result;
  private final StatisticsType<R, ?> statsType;
  private final String extendedId;
  private final String dataTypeName;

  public StatisticsImpl(
      final R result,
      final StatisticsType<R, ?> statsType,
      final String extendedId,
      final String dataTypeName) {
    this.result = result;
    this.statsType = statsType;
    this.extendedId = extendedId;
    this.dataTypeName = dataTypeName;
  }

  @Override
  public R getResult() {
    return result;
  }

  @Override
  public String getExtendedId() {
    return extendedId;
  }

  @Override
  public String getDataTypeName() {
    return dataTypeName;
  }

  @Override
  public StatisticsType<R, ?> getType() {
    return statsType;
  }
}
