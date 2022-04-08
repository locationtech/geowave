/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.store.query.filter.expression.ComparableExpression;
import org.locationtech.geowave.core.store.query.filter.expression.FieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.Predicate;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextFieldValue;
import org.threeten.extra.Interval;

/**
 * Interface for expressions that resolve to temporal objects.
 */
public interface TemporalExpression extends ComparableExpression<Interval> {

  // SimpleDateFormat is not thread safe
  public static final ThreadLocal<SimpleDateFormat[]> SUPPORTED_DATE_FORMATS =
      new ThreadLocal<SimpleDateFormat[]>() {
        @Override
        protected SimpleDateFormat[] initialValue() {
          return new SimpleDateFormat[] {
              new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ"),
              new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"),
              new SimpleDateFormat("yyyy-MM-dd")};
        }
      };

  /**
   * Create a predicate that tests to see if this expression is equal to the provided object. The
   * operand can be either another temporal expression, or any object that can be converted to a
   * temporal literal such as Date, Calendar, or Long.
   * 
   * @param other the temporal object to test against
   * @return the equals predicate
   */
  @Override
  default Predicate isEqualTo(final Object other) {
    return new TemporalEqualTo(this, toTemporalExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression is not equal to the provided object.
   * The operand can be either another temporal expression, or any object that can be converted to a
   * temporal literal such as Date, Calendar, or Long.
   * 
   * @param other the temporal object to test against
   * @return the not equals predicate
   */
  @Override
  default Predicate isNotEqualTo(final Object other) {
    return new TemporalNotEqualTo(this, toTemporalExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression is less than (before) the provided
   * object. The operand can be either another temporal expression, or any object that can be
   * converted to a temporal literal such as Date, Calendar, or Long.
   * 
   * @param other the temporal object to test against
   * @return the less than predicate
   */
  @Override
  default Predicate isLessThan(final Object other) {
    return isBefore(toTemporalExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression is less than or equal to (before or
   * during) the provided object. The operand can be either another temporal expression, or any
   * object that can be converted to a temporal literal such as Date, Calendar, or Long.
   * 
   * @param other the temporal object to test against
   * @return the less than or equal to predicate
   */
  @Override
  default Predicate isLessThanOrEqualTo(final Object other) {
    return isBeforeOrDuring(toTemporalExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression is greater than (after) the provided
   * object. The operand can be either another temporal expression, or any object that can be
   * converted to a temporal literal such as Date, Calendar, or Long.
   * 
   * @param other the temporal object to test against
   * @return the greater than predicate
   */
  @Override
  default Predicate isGreaterThan(final Object other) {
    return isAfter(toTemporalExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression is greater than or equal to (during or
   * after) the provided object. The operand can be either another temporal expression, or any
   * object that can be converted to a temporal literal such as Date, Calendar, or Long.
   * 
   * @param other the temporal object to test against
   * @return the greater than or equal to predicate
   */
  @Override
  default Predicate isGreaterThanOrEqualTo(final Object other) {
    return isDuringOrAfter(toTemporalExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression is between the provided lower and upper
   * bounds. The operands can be either temporal expressions, or any object that can be converted to
   * a temporal literal such as Date, Calendar, or Long.
   * 
   * @param lowerBound the lower bound to test against
   * @param upperBound the upper bound to test against
   * @return the between predicate
   */
  @Override
  default Predicate isBetween(final Object lowerBound, final Object upperBound) {
    return new TemporalBetween(
        this,
        toTemporalExpression(lowerBound),
        toTemporalExpression(upperBound));
  }

  /**
   * Create a predicate that tests to see if this expression is after the provided object. The
   * operand can be either another temporal expression, or any object that can be converted to a
   * temporal literal such as Date, Calendar, or Long.
   * 
   * @param other the temporal object to test against
   * @return the after predicate
   */
  default Predicate isAfter(final Object other) {
    return new After(this, toTemporalExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression is during or after to the provided
   * object. The operand can be either another temporal expression, or any object that can be
   * converted to a temporal literal such as Date, Calendar, or Long.
   * 
   * @param other the temporal object to test against
   * @return the during or after predicate
   */
  default Predicate isDuringOrAfter(final Object other) {
    return new DuringOrAfter(this, toTemporalExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression is before to the provided object. The
   * operand can be either another temporal expression, or any object that can be converted to a
   * temporal literal such as Date, Calendar, or Long.
   * 
   * @param other the temporal object to test against
   * @return the before predicate
   */
  default Predicate isBefore(final Object other) {
    return new Before(this, toTemporalExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression is before or during to the provided
   * object. The operand can be either another temporal expression, or any object that can be
   * converted to a temporal literal such as Date, Calendar, or Long.
   * 
   * @param other the temporal object to test against
   * @return the before or during predicate
   */
  default Predicate isBeforeOrDuring(final Object other) {
    return new BeforeOrDuring(this, toTemporalExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression is during to the provided object. The
   * operand can be either another temporal expression, or any object that can be converted to a
   * temporal literal such as Date, Calendar, or Long.
   * 
   * @param other the temporal object to test against
   * @return the equals predicate
   */
  default Predicate isDuring(final Object other) {
    return new During(this, toTemporalExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression contains the provided object. The
   * operand can be either another temporal expression, or any object that can be converted to a
   * temporal literal such as Date, Calendar, or Long.
   * 
   * @param other the temporal object to test against
   * @return the contains predicate
   */
  default Predicate contains(final Object other) {
    // this contains other if other is during this
    return new During(toTemporalExpression(other), this);
  }

  /**
   * Create a predicate that tests to see if this expression overlaps the provided object. The
   * operand can be either another temporal expression, or any object that can be converted to a
   * temporal literal such as Date, Calendar, or Long.
   * 
   * @param other the temporal object to test against
   * @return the overlaps predicate
   */
  default Predicate overlaps(final Object other) {
    return new TimeOverlaps(this, toTemporalExpression(other));
  }

  /**
   * Convert the given object into a temporal expression, if it is not already one.
   * 
   * @param obj the object to convert
   * @return the temporal expression
   */
  public static TemporalExpression toTemporalExpression(final Object obj) {
    if (obj instanceof TemporalExpression) {
      return (TemporalExpression) obj;
    }
    if (obj instanceof NumericFieldValue || obj instanceof TextFieldValue) {
      // Numeric and text field values could be interpreted as time if needed
      // e.g. dateField AFTER timestamp
      return TemporalFieldValue.of(((FieldValue<?>) obj).getFieldName());
    }
    return TemporalLiteral.of(obj);
  }

  public static Date stringToDate(final String dateStr) {
    for (final SimpleDateFormat format : SUPPORTED_DATE_FORMATS.get()) {
      try {
        return format.parse(dateStr);
      } catch (ParseException e) {
        // Did not match date format
      }
    }
    return null;
  }

  public static Interval stringToInterval(final String intervalStr) {
    // 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z
    if (intervalStr.contains("/")) {
      final String[] split = intervalStr.split("/");
      if (split.length == 2) {
        final Date date1 = stringToDate(split[0]);
        if (date1 != null) {
          final Date date2 = stringToDate(split[1]);
          if (date2 != null) {
            return TimeUtils.getInterval(date1, date2);
          }
        }
      }
      return null;
    }
    return TimeUtils.getInterval(stringToDate(intervalStr));
  }
}
