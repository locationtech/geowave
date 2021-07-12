/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.locationtech.geowave.core.geotime.store.field.IntervalSerializationProvider.IntervalReader;
import org.locationtech.geowave.core.geotime.store.field.IntervalSerializationProvider.IntervalWriter;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.store.query.filter.expression.Literal;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextLiteral;
import org.threeten.extra.Interval;

/**
 * A temporal implementation of literal, representing temporal literal objects.
 */
public class TemporalLiteral extends Literal<Interval> implements TemporalExpression {

  public TemporalLiteral() {}

  public TemporalLiteral(final Interval literal) {
    super(literal);
  }

  public static SimpleDateFormat[] getSupportedDateFormats() {
    return new SimpleDateFormat[] {
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ"),
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")};
  }

  public static TemporalLiteral of(Object literal) {
    if (literal == null) {
      return new TemporalLiteral(null);
    }
    if (literal instanceof TextLiteral || literal instanceof String) {
      final String dateStr =
          literal instanceof String ? (String) literal
              : ((TextLiteral) literal).evaluateValue(null);
      for (final SimpleDateFormat format : getSupportedDateFormats()) {
        try {
          final Date date = format.parse(dateStr);
          literal = date;
          break;
        } catch (ParseException e) {
          // Did not match date format
        }
      }
    }
    final Interval time = TimeUtils.getInterval(literal);
    if (time != null) {
      return new TemporalLiteral(time);
    }
    throw new RuntimeException("Unable to resolve temporal literal.");
  }

  @Override
  public String toString() {
    if (literal.getStart().equals(literal.getEnd())) {
      return literal.getStart().toString();
    }
    return literal.getStart().toString() + "/" + literal.getEnd().toString();
  }

  @Override
  public byte[] toBinary() {
    if (literal == null) {
      return new byte[] {(byte) 0};
    }
    final byte[] intervalBytes = new IntervalWriter().writeField(literal);
    final ByteBuffer buffer = ByteBuffer.allocate(1 + intervalBytes.length);
    buffer.put((byte) 1);
    buffer.put(intervalBytes);
    return buffer.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    final byte nullByte = buffer.get();
    if (nullByte == 0) {
      literal = null;
      return;
    }
    final byte[] intervalBytes = new byte[buffer.remaining()];
    buffer.get(intervalBytes);
    literal = new IntervalReader().readField(intervalBytes);
  }


}
