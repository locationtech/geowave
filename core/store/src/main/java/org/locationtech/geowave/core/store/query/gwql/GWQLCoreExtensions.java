/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql;

import org.locationtech.geowave.core.store.query.filter.expression.BooleanFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextFieldValue;
import org.locationtech.geowave.core.store.query.gwql.function.aggregation.AggregationFunction;
import org.locationtech.geowave.core.store.query.gwql.function.aggregation.CountFunction;
import org.locationtech.geowave.core.store.query.gwql.function.aggregation.MaxFunction;
import org.locationtech.geowave.core.store.query.gwql.function.aggregation.MinFunction;
import org.locationtech.geowave.core.store.query.gwql.function.aggregation.SumFunction;
import org.locationtech.geowave.core.store.query.gwql.function.expression.AbsFunction;
import org.locationtech.geowave.core.store.query.gwql.function.expression.ConcatFunction;
import org.locationtech.geowave.core.store.query.gwql.function.expression.ExpressionFunction;
import org.locationtech.geowave.core.store.query.gwql.function.operator.OperatorFunction;
import org.locationtech.geowave.core.store.query.gwql.function.predicate.PredicateFunction;
import org.locationtech.geowave.core.store.query.gwql.function.predicate.TextPredicates;
import org.locationtech.geowave.core.store.query.gwql.type.NumberCastableType;
import org.locationtech.geowave.core.store.query.gwql.type.TextCastableType;
import com.google.common.collect.Lists;

/**
 * The built-in set of functions used by the GeoWave query language.
 */
public class GWQLCoreExtensions implements GWQLExtensionRegistrySpi {

  @Override
  public AggregationFunction<?>[] getAggregationFunctions() {
    return new AggregationFunction<?>[] {
        new CountFunction(),
        new MinFunction(),
        new MaxFunction(),
        new SumFunction()};
  }

  @Override
  public PredicateFunction[] getPredicateFunctions() {
    return new PredicateFunction[] {
        new TextPredicates.StrStartsWithFunction(),
        new TextPredicates.StrEndsWithFunction(),
        new TextPredicates.StrContainsFunction()};
  }

  @Override
  public ExpressionFunction<?>[] getExpressionFunctions() {
    return new ExpressionFunction<?>[] {new AbsFunction(), new ConcatFunction()};
  }

  @Override
  public OperatorFunction[] getOperatorFunctions() {
    return null;
  }

  @Override
  public CastableType<?>[] getCastableTypes() {
    return new CastableType<?>[] {new TextCastableType(), new NumberCastableType(),};
  }

  @Override
  public FieldValueBuilder[] getFieldValueBuilders() {
    return new FieldValueBuilder[] {
        new FieldValueBuilder(Lists.newArrayList(Number.class), (fieldName) -> {
          return NumericFieldValue.of(fieldName);
        }),
        new FieldValueBuilder(Lists.newArrayList(String.class), (fieldName) -> {
          return TextFieldValue.of(fieldName);
        }),
        new FieldValueBuilder(Lists.newArrayList(Boolean.class), (fieldName) -> {
          return BooleanFieldValue.of(fieldName);
        })};
  }

}
