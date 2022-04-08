/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.statistics;

import java.util.Calendar;
import java.util.Date;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.field.FieldStatisticType;
import org.threeten.extra.Interval;

public class TimeRangeStatistic extends FieldStatistic<TimeRangeStatistic.TimeRangeValue> {
  public static final FieldStatisticType<TimeRangeValue> STATS_TYPE =
      new FieldStatisticType<>("TIME_RANGE");

  public TimeRangeStatistic() {
    super(STATS_TYPE);
  }

  public TimeRangeStatistic(final String typeName, final String fieldName) {
    super(STATS_TYPE, typeName, fieldName);
  }

  @Override
  public String getDescription() {
    return "Maintains the time range of a temporal field.";
  }

  @Override
  public TimeRangeValue createEmpty() {
    return new TimeRangeValue(this);
  }

  @Override
  public boolean isCompatibleWith(final Class<?> fieldClass) {
    return Date.class.isAssignableFrom(fieldClass)
        || Calendar.class.isAssignableFrom(fieldClass)
        || Number.class.isAssignableFrom(fieldClass);
  }

  public static class TimeRangeValue extends AbstractTimeRangeValue {

    public TimeRangeValue() {
      this(null);
    }

    public TimeRangeValue(final Statistic<?> statistic) {
      super(statistic);
    }

    @Override
    protected <T> Interval getInterval(
        final DataTypeAdapter<T> adapter,
        final T entry,
        final GeoWaveRow... rows) {
      final Object fieldValue =
          adapter.getFieldValue(entry, ((TimeRangeStatistic) statistic).getFieldName());
      return TimeUtils.getInterval(fieldValue);
    }

  }
}
