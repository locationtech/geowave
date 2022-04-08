/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.stats;

import org.locationtech.geowave.adapter.raster.stats.RasterBoundingBoxStatistic.RasterBoundingBoxValue;
import org.locationtech.geowave.adapter.raster.stats.RasterFootprintStatistic.RasterFootprintValue;
import org.locationtech.geowave.adapter.raster.stats.RasterHistogramStatistic.RasterHistogramValue;
import org.locationtech.geowave.adapter.raster.stats.RasterOverviewStatistic.RasterOverviewValue;
import org.locationtech.geowave.core.store.statistics.StatisticsRegistrySPI;

public class RasterRegisteredStatistics implements StatisticsRegistrySPI {

  @Override
  public RegisteredStatistic[] getRegisteredStatistics() {
    return new RegisteredStatistic[] {
        // Adapter Statistics
        new RegisteredStatistic(
            RasterBoundingBoxStatistic.STATS_TYPE,
            RasterBoundingBoxStatistic::new,
            RasterBoundingBoxValue::new,
            (short) 2300,
            (short) 2301),
        new RegisteredStatistic(
            RasterFootprintStatistic.STATS_TYPE,
            RasterFootprintStatistic::new,
            RasterFootprintValue::new,
            (short) 2302,
            (short) 2303),
        new RegisteredStatistic(
            RasterHistogramStatistic.STATS_TYPE,
            RasterHistogramStatistic::new,
            RasterHistogramValue::new,
            (short) 2304,
            (short) 2305),
        new RegisteredStatistic(
            RasterOverviewStatistic.STATS_TYPE,
            RasterOverviewStatistic::new,
            RasterOverviewValue::new,
            (short) 2306,
            (short) 2307)};
  }

  @Override
  public RegisteredBinningStrategy[] getRegisteredBinningStrategies() {
    return new RegisteredBinningStrategy[] {};
  }

}
