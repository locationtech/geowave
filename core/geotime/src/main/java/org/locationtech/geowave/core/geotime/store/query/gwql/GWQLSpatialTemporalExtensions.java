/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.gwql;

import java.util.Calendar;
import java.util.Date;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialFieldValue;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalFieldValue;
import org.locationtech.geowave.core.store.query.gwql.CastableType;
import org.locationtech.geowave.core.store.query.gwql.GWQLExtensionRegistrySpi;
import org.locationtech.geowave.core.store.query.gwql.function.aggregation.AggregationFunction;
import org.locationtech.geowave.core.store.query.gwql.function.expression.ExpressionFunction;
import org.locationtech.geowave.core.store.query.gwql.function.operator.OperatorFunction;
import org.locationtech.geowave.core.store.query.gwql.function.predicate.PredicateFunction;
import org.locationtech.jts.geom.Geometry;
import com.google.common.collect.Lists;

/**
 * The built-in set of functions used by the GeoWave query language.
 */
public class GWQLSpatialTemporalExtensions implements GWQLExtensionRegistrySpi {

  @Override
  public AggregationFunction<?>[] getAggregationFunctions() {
    return new AggregationFunction<?>[] {new BboxFunction()};
  }

  @Override
  public PredicateFunction[] getPredicateFunctions() {
    return new PredicateFunction[] {
        new SpatialPredicates.BboxFunction(),
        new SpatialPredicates.BboxLooseFunction(),
        new SpatialPredicates.IntersectsFunction(),
        new SpatialPredicates.IntersectsLooseFunction(),
        new SpatialPredicates.DisjointFunction(),
        new SpatialPredicates.DisjointLooseFunction(),
        new SpatialPredicates.CrossesFunction(),
        new SpatialPredicates.OverlapsFunction(),
        new SpatialPredicates.ContainsFunction(),
        new SpatialPredicates.TouchesFunction(),
        new SpatialPredicates.WithinFunction(),
        new TemporalPredicates.OverlapsFunction(),
        new TemporalPredicates.ContainsFunction()};
  }

  @Override
  public ExpressionFunction<?>[] getExpressionFunctions() {
    return null;
  }

  @Override
  public OperatorFunction[] getOperatorFunctions() {
    return new OperatorFunction[] {
        new TemporalOperators.BeforeOperator(),
        new TemporalOperators.BeforeOrDuringOperator(),
        new TemporalOperators.DuringOperator(),
        new TemporalOperators.DuringOrAfterOperator(),
        new TemporalOperators.AfterOperator()};
  }

  @Override
  public CastableType<?>[] getCastableTypes() {
    return new CastableType<?>[] {new GeometryCastableType(), new DateCastableType()};
  }

  @Override
  public FieldValueBuilder[] getFieldValueBuilders() {
    return new FieldValueBuilder[] {
        new FieldValueBuilder(Lists.newArrayList(Geometry.class), (fieldName) -> {
          return SpatialFieldValue.of(fieldName);
        }),
        new FieldValueBuilder(Lists.newArrayList(Date.class, Calendar.class), (fieldName) -> {
          return TemporalFieldValue.of(fieldName);
        })};
  }

}
