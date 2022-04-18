/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.stats;

import java.util.Map;
import javax.media.jai.Histogram;
import org.locationtech.geowave.adapter.raster.Resolution;
import org.locationtech.geowave.adapter.raster.stats.RasterBoundingBoxStatistic.RasterBoundingBoxValue;
import org.locationtech.geowave.adapter.raster.stats.RasterFootprintStatistic.RasterFootprintValue;
import org.locationtech.geowave.adapter.raster.stats.RasterHistogramStatistic.RasterHistogramValue;
import org.locationtech.geowave.adapter.raster.stats.RasterOverviewStatistic.RasterOverviewValue;
import org.locationtech.geowave.core.store.api.StatisticQueryBuilder;
import org.locationtech.geowave.core.store.statistics.query.DataTypeStatisticQueryBuilder;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

public interface RasterStatisticQueryBuilder {

  /**
   * Create a new data type statistic query builder for a raster bounding box statistic.
   * 
   * @return the data type statistic query builder
   */
  static DataTypeStatisticQueryBuilder<RasterBoundingBoxValue, Envelope> bbox() {
    return StatisticQueryBuilder.newBuilder(RasterBoundingBoxStatistic.STATS_TYPE);
  }

  /**
   * Create a new data type statistic query builder for a raster footprint statistic.
   * 
   * @return the data type statistic query builder
   */
  static DataTypeStatisticQueryBuilder<RasterFootprintValue, Geometry> footprint() {
    return StatisticQueryBuilder.newBuilder(RasterFootprintStatistic.STATS_TYPE);
  }

  /**
   * Create a new data type statistic query builder for a raster histogram statistic.
   * 
   * @return the data type statistic query builder
   */
  static DataTypeStatisticQueryBuilder<RasterHistogramValue, Map<Resolution, Histogram>> histogram() {
    return StatisticQueryBuilder.newBuilder(RasterHistogramStatistic.STATS_TYPE);
  }

  /**
   * Create a new data type statistic query builder for a raster overview statistic.
   * 
   * @return the data type statistic query builder
   */
  static DataTypeStatisticQueryBuilder<RasterOverviewValue, Resolution[]> overview() {
    return StatisticQueryBuilder.newBuilder(RasterOverviewStatistic.STATS_TYPE);
  }

}
