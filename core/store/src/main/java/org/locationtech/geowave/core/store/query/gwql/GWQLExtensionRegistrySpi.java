/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql;

import java.util.List;
import java.util.function.Function;
import org.locationtech.geowave.core.store.query.filter.expression.FieldValue;
import org.locationtech.geowave.core.store.query.gwql.function.aggregation.AggregationFunction;
import org.locationtech.geowave.core.store.query.gwql.function.expression.ExpressionFunction;
import org.locationtech.geowave.core.store.query.gwql.function.operator.OperatorFunction;
import org.locationtech.geowave.core.store.query.gwql.function.predicate.PredicateFunction;

/**
 * Class for adding functionality to the GeoWave query language.
 */
public interface GWQLExtensionRegistrySpi {

  /**
   * @return a list of field value builders to add
   */
  FieldValueBuilder[] getFieldValueBuilders();

  /**
   * @return a list of castable types
   */
  CastableType<?>[] getCastableTypes();

  /**
   * @return the aggregation functions to add
   */
  AggregationFunction<?>[] getAggregationFunctions();

  /**
   * @return the predicate functions to add
   */
  PredicateFunction[] getPredicateFunctions();

  /**
   * @return the expression functions to add
   */
  ExpressionFunction<?>[] getExpressionFunctions();

  /**
   * @return the operator functions to add
   */
  OperatorFunction[] getOperatorFunctions();

  public static class FieldValueBuilder {
    private final List<Class<?>> supportedClasses;
    private final Function<String, FieldValue<?>> buildFunction;

    public FieldValueBuilder(
        final List<Class<?>> supportedClasses,
        final Function<String, FieldValue<?>> buildFunction) {
      this.supportedClasses = supportedClasses;
      this.buildFunction = buildFunction;
    }

    public boolean isSupported(final Class<?> fieldClass) {
      return supportedClasses.stream().anyMatch(c -> c.isAssignableFrom(fieldClass));
    }

    public FieldValue<?> createFieldValue(final String fieldName) {
      return buildFunction.apply(fieldName);
    }

  }
}
