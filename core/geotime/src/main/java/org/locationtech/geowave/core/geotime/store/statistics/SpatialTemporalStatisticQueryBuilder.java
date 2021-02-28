package org.locationtech.geowave.core.geotime.store.statistics;

import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic.BoundingBoxValue;
import org.locationtech.geowave.core.geotime.store.statistics.TimeRangeStatistic.TimeRangeValue;
import org.locationtech.geowave.core.store.api.StatisticQueryBuilder;
import org.locationtech.geowave.core.store.statistics.query.FieldStatisticQueryBuilder;
import org.locationtech.jts.geom.Envelope;
import org.threeten.extra.Interval;

public interface SpatialTemporalStatisticQueryBuilder {

  /**
   * Create a new field statistic query builder for a bounding box statistic.
   * 
   * @return the field statistic query builder
   */
  static FieldStatisticQueryBuilder<BoundingBoxValue, Envelope> bbox() {
    return StatisticQueryBuilder.newBuilder(BoundingBoxStatistic.STATS_TYPE);
  }

  /**
   * Create a new field statistic query builder for a time range statistic.
   * 
   * @return the field statistic query builder
   */
  static FieldStatisticQueryBuilder<TimeRangeValue, Interval> timeRange() {
    return StatisticQueryBuilder.newBuilder(TimeRangeStatistic.STATS_TYPE);
  }
}
