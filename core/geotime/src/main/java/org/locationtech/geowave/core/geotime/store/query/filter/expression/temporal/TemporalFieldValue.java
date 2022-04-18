/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal;

import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.query.filter.expression.FieldValue;
import org.threeten.extra.Interval;

/**
 * A field value implementation for temporal adapter fields.
 */
public class TemporalFieldValue extends FieldValue<Interval> implements TemporalExpression {

  public TemporalFieldValue() {}

  public TemporalFieldValue(final String fieldName) {
    super(fieldName);
  }

  @Override
  public <T> Interval evaluateValue(final DataTypeAdapter<T> adapter, final T entry) {
    final Object value = super.evaluateValue(adapter, entry);
    if (value == null) {
      return null;
    }
    return TimeUtils.getInterval(value);
  }

  public static TemporalFieldValue of(final String fieldName) {
    return new TemporalFieldValue(fieldName);
  }

  @Override
  protected Interval evaluateValueInternal(final Object value) {
    if (value instanceof String) {
      final Interval interval = TemporalExpression.stringToInterval((String) value);
      if (interval == null) {
        throw new RuntimeException("'" + (String) value + "' is not in a supported date format.");
      }
      return interval;
    }
    return TimeUtils.getInterval(value);
  }

}
