/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.statistics;

import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic.BoundingBoxValue;
import org.locationtech.geowave.core.geotime.store.statistics.TimeRangeStatistic.TimeRangeValue;
import org.locationtech.geowave.core.geotime.store.statistics.binning.SpatialFieldValueBinningStrategy;
import org.locationtech.geowave.core.geotime.store.statistics.binning.TimeRangeFieldValueBinningStrategy;
import org.locationtech.geowave.core.store.statistics.StatisticsRegistrySPI;

public class GeotimeRegisteredStatistics implements StatisticsRegistrySPI {

  @Override
  public RegisteredStatistic[] getRegisteredStatistics() {
    return new RegisteredStatistic[] {
        // Field Statistics
        new RegisteredStatistic(
            BoundingBoxStatistic.STATS_TYPE,
            BoundingBoxStatistic::new,
            BoundingBoxValue::new,
            (short) 2100,
            (short) 2101),
        new RegisteredStatistic(
            TimeRangeStatistic.STATS_TYPE,
            TimeRangeStatistic::new,
            TimeRangeValue::new,
            (short) 2102,
            (short) 2103)};
  }

  @Override
  public RegisteredBinningStrategy[] getRegisteredBinningStrategies() {
    return new RegisteredBinningStrategy[] {
        new RegisteredBinningStrategy(
            TimeRangeFieldValueBinningStrategy.NAME,
            TimeRangeFieldValueBinningStrategy::new,
            (short) 2150),
        new RegisteredBinningStrategy(
            SpatialFieldValueBinningStrategy.NAME,
            SpatialFieldValueBinningStrategy::new,
            (short) 2151)};
  }
}
