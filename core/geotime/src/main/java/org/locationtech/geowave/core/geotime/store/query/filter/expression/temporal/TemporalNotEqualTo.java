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
import org.locationtech.geowave.core.store.query.filter.expression.FilterRange;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericFieldConstraints;
import org.threeten.extra.Interval;
import com.google.common.collect.Lists;

/**
 * Predicate that passes when the first operand is not equal to the second operand.
 */
public class TemporalNotEqualTo extends BinaryTemporalPredicate {

  public TemporalNotEqualTo() {}

  public TemporalNotEqualTo(
      final TemporalExpression expression1,
      final TemporalExpression expression2) {
    super(expression1, expression2);
  }

  @Override
  public boolean evaluateInternal(final Interval value1, final Interval value2) {
    if (value1 == null) {
      return value2 != null;
    } else if (value2 == null) {
      return true;
    }
    return value1.getStart().compareTo(value2.getStart()) != 0
        || TimeUtils.getIntervalEnd(value1).compareTo(TimeUtils.getIntervalEnd(value2)) != 0;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(expression1.toString());
    sb.append(" <> ");
    sb.append(expression2.toString());
    return sb.toString();
  }

  @Override
  public NumericFieldConstraints getConstraints(
      final Interval literal,
      final Double minValue,
      final Double maxValue,
      final boolean reversed,
      final boolean exact) {
    if (exact) {
      if (literal.isEmpty()) {
        return NumericFieldConstraints.of(
            Lists.newArrayList(
                FilterRange.of(
                    minValue,
                    (double) literal.getStart().toEpochMilli(),
                    true,
                    false,
                    exact),
                FilterRange.of(
                    (double) literal.getStart().toEpochMilli(),
                    maxValue,
                    false,
                    true,
                    exact)));
      } else {
        return NumericFieldConstraints.of(Lists.newArrayList());
      }
    }
    return NumericFieldConstraints.of(minValue, maxValue, true, true, false);
  }

}
