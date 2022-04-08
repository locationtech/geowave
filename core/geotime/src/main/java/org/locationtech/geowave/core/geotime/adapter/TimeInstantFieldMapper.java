/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.adapter;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.store.api.RowBuilder;
import org.threeten.extra.Interval;
import com.google.common.collect.Sets;

/**
 * Maps a single adapter field that represents an instant in time to an `Interval` index field.
 *
 * @param <N> the adapter field type
 */
public abstract class TimeInstantFieldMapper<N> extends TemporalIntervalFieldMapper<N> {

  @Override
  public Interval toIndex(List<N> nativeFieldValues) {
    return TimeUtils.getInterval(nativeFieldValues.get(0));
  }

  @Override
  public void toAdapter(final Interval indexFieldValue, final RowBuilder<?> rowBuilder) {
    rowBuilder.setField(
        adapterFields[0],
        TimeUtils.getTimeValue(
            this.adapterFieldType(),
            ((Interval) indexFieldValue).getStart().toEpochMilli()));
  }

  @Override
  public short adapterFieldCount() {
    return 1;
  }

  @Override
  public Set<String> getLowerCaseSuggestedFieldNames() {
    return Sets.newHashSet("timestamp", "date", "time");
  }

  /**
   * Maps a `Calendar` adapter field to an `Interval` index field.
   */
  public static class CalendarInstantFieldMapper extends TimeInstantFieldMapper<Calendar> {

    @Override
    public Class<Calendar> adapterFieldType() {
      return Calendar.class;
    }

  }

  /**
   * Maps a `Date` adapter field to an `Interval` index field.
   */
  public static class DateInstantFieldMapper extends TimeInstantFieldMapper<Date> {

    @Override
    public Class<Date> adapterFieldType() {
      return Date.class;
    }

  }

  /**
   * Maps a `Long` adapter field to an `Interval` index field.
   */
  public static class LongInstantFieldMapper extends TimeInstantFieldMapper<Long> {

    @Override
    public Class<Long> adapterFieldType() {
      return Long.class;
    }

  }

}
