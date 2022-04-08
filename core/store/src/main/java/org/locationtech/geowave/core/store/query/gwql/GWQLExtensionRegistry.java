/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.locationtech.geowave.core.index.SPIServiceRegistry;
import org.locationtech.geowave.core.store.query.filter.expression.FieldValue;
import org.locationtech.geowave.core.store.query.gwql.GWQLExtensionRegistrySpi.FieldValueBuilder;
import org.locationtech.geowave.core.store.query.gwql.function.aggregation.AggregationFunction;
import org.locationtech.geowave.core.store.query.gwql.function.expression.ExpressionFunction;
import org.locationtech.geowave.core.store.query.gwql.function.operator.OperatorFunction;
import org.locationtech.geowave.core.store.query.gwql.function.predicate.PredicateFunction;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Singleton registry for all GWQL extensions. Functionality can be added to the language using
 * {@link GWQLExtensionRegistrySpi}.
 */
public class GWQLExtensionRegistry {

  private static GWQLExtensionRegistry INSTANCE = null;

  private List<FieldValueBuilder> fieldValueBuilders = Lists.newArrayList();
  private Map<String, AggregationFunction<?>> aggregationFunctions = Maps.newHashMap();
  private Map<String, PredicateFunction> predicateFunctions = Maps.newHashMap();
  private Map<String, OperatorFunction> operatorFunctions = Maps.newHashMap();
  private Map<String, ExpressionFunction<?>> expressionFunctions = Maps.newHashMap();
  private Map<String, CastableType<?>> castableTypes = Maps.newHashMap();

  private GWQLExtensionRegistry() {
    final Iterator<GWQLExtensionRegistrySpi> spiIter =
        new SPIServiceRegistry(GWQLExtensionRegistry.class).load(GWQLExtensionRegistrySpi.class);
    while (spiIter.hasNext()) {
      final GWQLExtensionRegistrySpi functionSet = spiIter.next();
      final AggregationFunction<?>[] aggregations = functionSet.getAggregationFunctions();
      if (aggregations != null) {
        Arrays.stream(aggregations).forEach(f -> registerFunction(f, aggregationFunctions));
      }
      final PredicateFunction[] predicates = functionSet.getPredicateFunctions();
      if (predicates != null) {
        Arrays.stream(predicates).forEach(f -> registerFunction(f, predicateFunctions));
      }
      final OperatorFunction[] operators = functionSet.getOperatorFunctions();
      if (operators != null) {
        Arrays.stream(operators).forEach(f -> registerFunction(f, operatorFunctions));
      }
      final ExpressionFunction<?>[] expressions = functionSet.getExpressionFunctions();
      if (expressions != null) {
        Arrays.stream(expressions).forEach(f -> registerFunction(f, expressionFunctions));
      }
      final CastableType<?>[] types = functionSet.getCastableTypes();
      if (types != null) {
        Arrays.stream(types).forEach(t -> registerCastableType(t));
      }
      final FieldValueBuilder[] fieldValues = functionSet.getFieldValueBuilders();
      if (fieldValues != null) {
        Arrays.stream(fieldValues).forEach(f -> fieldValueBuilders.add(f));
      }
    }
  }

  public static GWQLExtensionRegistry instance() {
    if (INSTANCE == null) {
      INSTANCE = new GWQLExtensionRegistry();
    }
    return INSTANCE;
  }

  private <T extends QLFunction<?>> void registerFunction(
      final T function,
      final Map<String, T> registeredFunctions) {
    if (registeredFunctions.containsKey(function.getName())) {
      throw new RuntimeException(
          "A function with the name " + function.getName() + " is already registered.");
    }
    registeredFunctions.put(function.getName(), function);
  }

  private void registerCastableType(final CastableType<?> type) {
    if (castableTypes.containsKey(type.getName())) {
      throw new RuntimeException(
          "A type with the name " + type.getName() + " is already registered.");
    }
    castableTypes.put(type.getName(), type);
  }

  /**
   * Retrieves the aggregation function with the given name.
   * 
   * @param functionName the function name
   * @return the function that matches the given name, or {@code null} if it could not be found
   */
  public AggregationFunction<?> getAggregationFunction(final String functionName) {
    return aggregationFunctions.get(functionName.toUpperCase());
  }

  /**
   * Retrieves the predicate function with the given name.
   * 
   * @param functionName the function name
   * @return the function that matches the given name, or {@code null} if it could not be found
   */
  public PredicateFunction getPredicateFunction(final String functionName) {
    return predicateFunctions.get(functionName.toUpperCase());
  }

  /**
   * Retrieves the operator function with the given operator.
   * 
   * @param operator the operator
   * @return the function that matches the given operator, or {@code null} if it could not be found
   */
  public OperatorFunction getOperatorFunction(final String operator) {
    return operatorFunctions.get(operator.toUpperCase());
  }

  /**
   * Retrieves the expression function with the given name.
   * 
   * @param functionName the function name
   * @return the function that matches the given name, or {@code null} if it could not be found
   */
  public ExpressionFunction<?> getExpressionFunction(final String functionName) {
    return expressionFunctions.get(functionName.toUpperCase());
  }

  /**
   * Get a castable type with the given name.
   * 
   * @param typeName the castable type name
   * @return the castable type, or {@code null} if it could not befound
   */
  public CastableType<?> getCastableType(final String typeName) {
    return castableTypes.get(typeName.toLowerCase());
  }

  /**
   * Create a field value expression for the given field name and class.
   * 
   * @param fieldClass the class of the field
   * @param fieldName the name of the field
   * @return an appropriate field value expression for the field, or {@code null} if a matching
   *         field value builder could not be found
   */
  public FieldValue<?> createFieldValue(final Class<?> fieldClass, final String fieldName) {
    for (final FieldValueBuilder builder : fieldValueBuilders) {
      if (builder.isSupported(fieldClass)) {
        return builder.createFieldValue(fieldName);
      }
    }
    return null;
  }

}
