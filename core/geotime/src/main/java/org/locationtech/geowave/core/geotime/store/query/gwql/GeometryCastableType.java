/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.gwql;

import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.FilterGeometry;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialExpression;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialLiteral;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.TextToSpatialExpression;
import org.locationtech.geowave.core.store.query.filter.expression.Expression;
import org.locationtech.geowave.core.store.query.filter.expression.InvalidFilterException;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextExpression;
import org.locationtech.geowave.core.store.query.gwql.CastableType;
import org.locationtech.geowave.core.store.query.gwql.GWQLParseException;

public class GeometryCastableType implements CastableType<FilterGeometry> {

  @Override
  public String getName() {
    return "geometry";
  }

  @Override
  public SpatialExpression cast(Object objectOrExpression) {
    return toSpatialExpression(objectOrExpression);
  }

  public static SpatialExpression toSpatialExpression(Object objectOrExpression) {
    if (objectOrExpression instanceof SpatialExpression) {
      return (SpatialExpression) objectOrExpression;
    }
    if (objectOrExpression instanceof Expression
        && ((Expression<?>) objectOrExpression).isLiteral()) {
      objectOrExpression = ((Expression<?>) objectOrExpression).evaluateValue(null);
    }
    if (objectOrExpression instanceof Expression) {
      if (objectOrExpression instanceof TextExpression) {
        return new TextToSpatialExpression((TextExpression) objectOrExpression);
      } else {
        throw new GWQLParseException("Unable to cast expression to geometry");
      }
    } else {
      try {
        return SpatialLiteral.of(objectOrExpression);
      } catch (InvalidFilterException e) {
        throw new GWQLParseException("Unable to cast literal to geometry", e);
      }
    }
  }

}
