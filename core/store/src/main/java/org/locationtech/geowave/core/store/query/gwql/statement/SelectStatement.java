/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql.statement;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.locationtech.geowave.core.index.persist.PersistableList;
import org.locationtech.geowave.core.store.api.AggregationQueryBuilder;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.query.aggregate.CompositeAggregation;
import org.locationtech.geowave.core.store.query.filter.expression.Filter;
import org.locationtech.geowave.core.store.query.gwql.AdapterEntryResultSet;
import org.locationtech.geowave.core.store.query.gwql.AggregationSelector;
import org.locationtech.geowave.core.store.query.gwql.ColumnSelector;
import org.locationtech.geowave.core.store.query.gwql.GWQLExtensionRegistry;
import org.locationtech.geowave.core.store.query.gwql.ResultSet;
import org.locationtech.geowave.core.store.query.gwql.Selector;
import org.locationtech.geowave.core.store.query.gwql.Selector.SelectorType;
import org.locationtech.geowave.core.store.query.gwql.SingletonResultSet;
import org.locationtech.geowave.core.store.query.gwql.function.aggregation.AggregationFunction;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Select data from a GeoWave type. This can be an aggregation or a plain query.
 */
public class SelectStatement<T> implements Statement {

  private final DataStore dataStore;
  private final DataTypeAdapter<T> adapter;
  private List<Selector> selectors;
  private final Filter filter;
  private final Integer limit;

  /**
   * @param adapter the adapter to select data from
   * @param selectors the selectors to use
   * @param filter the filter to use
   * @param limit the limit to use
   */
  public SelectStatement(
      final DataStore dataStore,
      final DataTypeAdapter<T> adapter,
      final List<Selector> selectors,
      final @Nullable Filter filter,
      final @Nullable Integer limit) {
    this.dataStore = dataStore;
    this.adapter = adapter;
    this.selectors = selectors;
    this.filter = filter;
    this.limit = limit;
  }

  @Override
  public ResultSet execute(final String... authorizations) {
    final String typeName = adapter.getTypeName();

    if (isAggregation()) {
      final AggregationQueryBuilder<PersistableList, List<Object>, T, ?> bldr =
          AggregationQueryBuilder.newBuilder();
      bldr.setAuthorizations(authorizations);
      if (filter != null) {
        bldr.filter(filter);
      }
      if (limit != null) {
        bldr.limit(limit);
      }

      final CompositeAggregation<T> composite = new CompositeAggregation<>();
      final List<String> columnNames = Lists.newArrayListWithCapacity(selectors.size());
      final List<Class<?>> columnTypes = Lists.newArrayListWithCapacity(selectors.size());
      for (final Selector selector : selectors) {
        final AggregationSelector aggregation = (AggregationSelector) selector;
        final AggregationFunction<?> function =
            GWQLExtensionRegistry.instance().getAggregationFunction(aggregation.functionName());
        if (function == null) {
          throw new RuntimeException(
              "No aggregation function called '" + aggregation.functionName() + "' was found.");
        }
        composite.add(function.getAggregation(adapter, aggregation.functionArgs()));
        columnNames.add(selector.name());
        columnTypes.add(function.getReturnType());
      }
      bldr.aggregate(typeName, composite);
      return new SingletonResultSet(columnNames, columnTypes, dataStore.aggregate(bldr.build()));
    } else {
      final QueryBuilder<T, ?> bldr =
          QueryBuilder.newBuilder(adapter.getDataClass()).addTypeName(typeName);
      bldr.setAuthorizations(authorizations);
      if (filter != null) {
        bldr.filter(filter);
      }

      if ((selectors != null) && !selectors.isEmpty()) {
        final Set<String> usedAttributes = Sets.newHashSet();
        selectors.forEach(s -> usedAttributes.add(((ColumnSelector) s).columnName()));
        if (filter != null) {
          filter.addReferencedFields(usedAttributes);
        }
        for (final String attribute : usedAttributes) {
          if (adapter.getFieldDescriptor(attribute) == null) {
            throw new RuntimeException(
                "No column named " + attribute + " was found in " + typeName);
          }
        }
        bldr.subsetFields(typeName, usedAttributes.toArray(new String[usedAttributes.size()]));
      } else {
        selectors =
            Lists.transform(
                Arrays.asList(adapter.getFieldDescriptors()),
                f -> new ColumnSelector(f.fieldName()));
      }
      if (limit != null) {
        bldr.limit(limit);
      }
      return new AdapterEntryResultSet<>(selectors, adapter, dataStore.query(bldr.build()));
    }
  }

  /**
   * @return {@code true} if this select statement represents an aggregation, {@code false}
   *         otherwise
   */
  public boolean isAggregation() {
    return (selectors != null)
        && !selectors.isEmpty()
        && (selectors.get(0).type() == SelectorType.AGGREGATION);
  }

  /**
   * @return the type to select data from
   */
  public DataTypeAdapter<?> getAdapter() {
    return adapter;
  }

  /**
   * @return the filter for the query
   */
  public Filter getFilter() {
    return filter;
  }

  /**
   * @return the limit for the query
   */
  public Integer getLimit() {
    return limit;
  }

  /**
   * @return the selectors for the query
   */
  public List<Selector> getSelectors() {
    return selectors;
  }
}
