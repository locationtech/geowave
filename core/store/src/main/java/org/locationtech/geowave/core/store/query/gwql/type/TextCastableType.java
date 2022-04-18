/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql.type;

import org.locationtech.geowave.core.store.query.filter.expression.Expression;
import org.locationtech.geowave.core.store.query.filter.expression.FieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.InvalidFilterException;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextExpression;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextLiteral;
import org.locationtech.geowave.core.store.query.gwql.CastableType;
import org.locationtech.geowave.core.store.query.gwql.GWQLParseException;

public class TextCastableType implements CastableType<String> {
  @Override
  public String getName() {
    return "text";
  }

  @Override
  public TextExpression cast(Object objectOrExpression) {
    return toTextExpression(objectOrExpression);
  }

  public static TextExpression toTextExpression(Object objectOrExpression) {
    if (objectOrExpression instanceof TextExpression) {
      return (TextExpression) objectOrExpression;
    }
    if (objectOrExpression instanceof Expression
        && ((Expression<?>) objectOrExpression).isLiteral()) {
      objectOrExpression = ((Expression<?>) objectOrExpression).evaluateValue(null);
    }
    if (objectOrExpression instanceof Expression) {
      if (objectOrExpression instanceof FieldValue) {
        return new TextFieldValue(((FieldValue<?>) objectOrExpression).getFieldName());
      } else {
        throw new GWQLParseException("Unable to cast expression to text");
      }
    } else {
      try {
        return TextLiteral.of(objectOrExpression.toString());
      } catch (InvalidFilterException e) {
        throw new GWQLParseException("Unable to cast literal to text", e);
      }
    }
  }
}
