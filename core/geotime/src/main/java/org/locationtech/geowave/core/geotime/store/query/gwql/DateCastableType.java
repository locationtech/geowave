/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.gwql;

import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalExpression;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalFieldValue;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalLiteral;
import org.locationtech.geowave.core.store.query.filter.expression.Expression;
import org.locationtech.geowave.core.store.query.filter.expression.InvalidFilterException;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextFieldValue;
import org.locationtech.geowave.core.store.query.gwql.CastableType;
import org.locationtech.geowave.core.store.query.gwql.GWQLParseException;
import org.threeten.extra.Interval;

public class DateCastableType implements CastableType<Interval> {

  @Override
  public String getName() {
    return "date";
  }

  @Override
  public TemporalExpression cast(Object objectOrExpression) {
    return toTemporalExpression(objectOrExpression);
  }

  public static TemporalExpression toTemporalExpression(Object objectOrExpression) {
    if (objectOrExpression instanceof TemporalExpression) {
      return (TemporalExpression) objectOrExpression;
    }
    if (objectOrExpression instanceof Expression
        && ((Expression<?>) objectOrExpression).isLiteral()) {
      objectOrExpression = ((Expression<?>) objectOrExpression).evaluateValue(null);
    }
    if (objectOrExpression instanceof Expression) {
      if (objectOrExpression instanceof NumericFieldValue) {
        return new TemporalFieldValue(((NumericFieldValue) objectOrExpression).getFieldName());
      } else if (objectOrExpression instanceof TextFieldValue) {
        return new TemporalFieldValue(((TextFieldValue) objectOrExpression).getFieldName());
      } else {
        throw new GWQLParseException("Unable to cast expression to date");
      }
    } else {
      try {
        return TemporalLiteral.of(objectOrExpression);
      } catch (InvalidFilterException e) {
        throw new GWQLParseException("Unable to cast literal to date", e);
      }
    }
  }

}
