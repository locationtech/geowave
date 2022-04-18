/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.adapter;

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.RowBuilder;
import org.threeten.extra.Interval;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Maps two adapter fields that represent a start and end time to an `Interval` index field.
 *
 * @param <N> the adapter field type
 */
public abstract class TimeRangeFieldMapper<N> extends TemporalIntervalFieldMapper<N> {
  private static Set<String> suggestedStartTimeFieldNames =
      Sets.newHashSet("starttime", "start", "start_time");
  private static Set<String> suggestedEndTimeNames = Sets.newHashSet("endtime", "end", "end_time");

  private boolean startFirst = true;

  @Override
  public void initFromOptions(
      final List<FieldDescriptor<N>> inputFieldDescriptors,
      final IndexFieldOptions options) {
    if (inputFieldDescriptors.size() != 2) {
      throw new RuntimeException("Time range field mapper expects exactly 2 fields.");
    }
    startFirst =
        inputFieldDescriptors.get(0).indexHints().contains(TimeField.START_TIME_DIMENSION_HINT)
            || !inputFieldDescriptors.get(1).indexHints().contains(
                TimeField.START_TIME_DIMENSION_HINT)
            || suggestedStartTimeFieldNames.contains(
                inputFieldDescriptors.get(0).fieldName().toLowerCase());
    super.initFromOptions(inputFieldDescriptors, options);
  }

  @Override
  public String[] getIndexOrderedAdapterFields() {
    if (!startFirst) {
      return new String[] {adapterFields[1], adapterFields[0]};
    }
    return adapterFields;
  }

  @Override
  public Interval toIndex(List<N> nativeFieldValues) {
    if (startFirst) {
      return TimeUtils.getInterval(nativeFieldValues.get(0), nativeFieldValues.get(1));
    } else {
      return TimeUtils.getInterval(nativeFieldValues.get(1), nativeFieldValues.get(0));
    }
  }

  @Override
  public void toAdapter(final Interval indexFieldValue, final RowBuilder<?> rowBuilder) {
    if (startFirst) {
      rowBuilder.setField(
          adapterFields[0],
          TimeUtils.getTimeValue(
              this.adapterFieldType(),
              ((Interval) indexFieldValue).getStart().toEpochMilli()));
      rowBuilder.setField(
          adapterFields[1],
          TimeUtils.getTimeValue(
              this.adapterFieldType(),
              ((Interval) indexFieldValue).getEnd().toEpochMilli()));
    } else {
      rowBuilder.setField(
          adapterFields[1],
          TimeUtils.getTimeValue(
              this.adapterFieldType(),
              ((Interval) indexFieldValue).getStart().toEpochMilli()));
      rowBuilder.setField(
          adapterFields[0],
          TimeUtils.getTimeValue(
              this.adapterFieldType(),
              ((Interval) indexFieldValue).getEnd().toEpochMilli()));
    }
  }

  @Override
  public short adapterFieldCount() {
    return 2;
  }

  @Override
  public Set<String> getLowerCaseSuggestedFieldNames() {
    return Sets.newHashSet(Iterables.concat(suggestedStartTimeFieldNames, suggestedEndTimeNames));
  }

  @Override
  protected int byteLength() {
    return super.byteLength() + 1;
  }

  protected void writeBytes(final ByteBuffer buffer) {
    super.writeBytes(buffer);
    buffer.put((byte) (startFirst ? 1 : 0));
  }

  protected void readBytes(final ByteBuffer buffer) {
    super.readBytes(buffer);
    startFirst = buffer.get() != 0;
  }


  /**
   * Maps two `Calendar` adapter fields to an `Interval` index field.
   */
  public static class CalendarRangeFieldMapper extends TimeRangeFieldMapper<Calendar> {

    @Override
    public Class<Calendar> adapterFieldType() {
      return Calendar.class;
    }

  }

  /**
   * Maps two `Date` adapter fields to an `Interval` index field.
   */
  public static class DateRangeFieldMapper extends TimeRangeFieldMapper<Date> {

    @Override
    public Class<Date> adapterFieldType() {
      return Date.class;
    }

  }

  /**
   * Maps two `Long` adapter fields to an `Interval` index field.
   */
  public static class LongRangeFieldMapper extends TimeRangeFieldMapper<Long> {

    @Override
    public Class<Long> adapterFieldType() {
      return Long.class;
    }

  }

}

