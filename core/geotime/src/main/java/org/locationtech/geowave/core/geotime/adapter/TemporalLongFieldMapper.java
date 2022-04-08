/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.adapter;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.IndexFieldMapper;
import org.locationtech.geowave.core.store.api.RowBuilder;

/**
 * Maps an adapter temporal field or fields to a `Long` index field.
 *
 * @param <N> the adapter field type
 */
public abstract class TemporalLongFieldMapper<N> extends IndexFieldMapper<N, Long> {

  @Override
  public Class<Long> indexFieldType() {
    return Long.class;
  }

  @Override
  public void transformFieldDescriptors(final FieldDescriptor<?>[] inputFieldDescriptors) {}

  @Override
  protected void initFromOptions(
      List<FieldDescriptor<N>> inputFieldDescriptors,
      IndexFieldOptions options) {}

  @Override
  public short adapterFieldCount() {
    return 1;
  }

  /**
   * Maps a `Calendar` adapter field to an `Long` index field.
   */
  public static class CalendarLongFieldMapper extends TemporalLongFieldMapper<Calendar> {

    @Override
    public Class<Calendar> adapterFieldType() {
      return Calendar.class;
    }

    @Override
    public Long toIndex(List<Calendar> nativeFieldValues) {
      return nativeFieldValues.get(0).getTimeInMillis();
    }

    @Override
    public void toAdapter(Long indexFieldValue, final RowBuilder<?> rowBuilder) {
      final Calendar calendar = Calendar.getInstance();
      calendar.setTimeInMillis(indexFieldValue);
      rowBuilder.setField(adapterFields[0], calendar);
    }

  }

  /**
   * Maps a `Date` adapter field to an `Long` index field.
   */
  public static class DateLongFieldMapper extends TemporalLongFieldMapper<Date> {

    @Override
    public Class<Date> adapterFieldType() {
      return Date.class;
    }

    @Override
    public Long toIndex(List<Date> nativeFieldValues) {
      return nativeFieldValues.get(0).getTime();
    }

    @Override
    public void toAdapter(Long indexFieldValue, final RowBuilder<?> rowBuilder) {
      rowBuilder.setField(adapterFields[0], new Date(indexFieldValue));
    }

  }

}
