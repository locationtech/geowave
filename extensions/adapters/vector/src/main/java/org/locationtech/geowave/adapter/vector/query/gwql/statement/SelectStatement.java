/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.gwql.statement;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.geotools.data.DataUtilities;
import org.locationtech.geowave.adapter.vector.query.aggregation.CompositeVectorAggregation;
import org.locationtech.geowave.adapter.vector.query.gwql.AggregationSelector;
import org.locationtech.geowave.adapter.vector.query.gwql.ColumnSelector;
import org.locationtech.geowave.adapter.vector.query.gwql.Selector;
import org.locationtech.geowave.adapter.vector.query.gwql.Selector.SelectorType;
import org.locationtech.geowave.adapter.vector.query.gwql.function.QLVectorAggregationFunction;
import org.locationtech.geowave.adapter.vector.query.gwql.function.QLFunction;
import org.locationtech.geowave.adapter.vector.query.gwql.function.QLFunctionRegistry;
import org.locationtech.geowave.adapter.vector.query.gwql.QualifiedTypeName;
import org.locationtech.geowave.adapter.vector.query.gwql.ResultSet;
import org.locationtech.geowave.adapter.vector.query.gwql.SimpleFeatureResultSet;
import org.locationtech.geowave.adapter.vector.query.gwql.SingletonResultSet;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.api.VectorAggregationQueryBuilder;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.index.persist.PersistableList;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Select data from a GeoWave type. This can be an aggregation or a plain query.
 */
public class SelectStatement implements Statement {

  private final QualifiedTypeName typeName;
  private List<Selector> selectors;
  private final Filter filter;
  private final Integer limit;

  /**
   * @param typeName the type to select data from
   * @param selectors the selectors to use
   * @param filter the filter to use
   * @param limit the limit to use
   */
  public SelectStatement(
      final QualifiedTypeName typeName,
      final List<Selector> selectors,
      final @Nullable Filter filter,
      final @Nullable Integer limit) {
    this.typeName = typeName;
    this.selectors = selectors;
    this.filter = filter;
    this.limit = limit;
  }

  @Override
  public ResultSet execute(final DataStore dataStore) {
    final DataTypeAdapter<?> dataAdapter = dataStore.getType(typeName.typeName());
    if (!(dataAdapter instanceof GeotoolsFeatureDataAdapter)) {
      throw new RuntimeException("Statement can only be used on vector types.");
    }
    final GeotoolsFeatureDataAdapter<?> adapter = (GeotoolsFeatureDataAdapter<?>) dataAdapter;
    final SimpleFeatureType featureType = adapter.getFeatureType();

    if (isAggregation()) {
      final VectorAggregationQueryBuilder<PersistableList, List<Object>> bldr =
          VectorAggregationQueryBuilder.newBuilder();
      if (filter != null) {
        bldr.constraints(bldr.constraintsFactory().filterConstraints(filter));
      }
      if (limit != null) {
        bldr.limit(limit);
      }

      final CompositeVectorAggregation composite = new CompositeVectorAggregation();
      final List<String> columnNames = Lists.newArrayListWithCapacity(selectors.size());
      final List<Class<?>> columnTypes = Lists.newArrayListWithCapacity(selectors.size());
      for (final Selector selector : selectors) {
        final AggregationSelector aggregation = (AggregationSelector) selector;
        QLFunction function = QLFunctionRegistry.instance().getFunction(aggregation.functionName());
        if (function == null) {
          throw new RuntimeException(
              "No function called '" + aggregation.functionName() + "' was found.");
        }
        if (!(function instanceof QLVectorAggregationFunction)) {
          throw new RuntimeException(
              "Only aggregation functions can be used with other aggregation functions.");
        }
        composite.add(
            ((QLVectorAggregationFunction) function).getAggregation(
                featureType,
                aggregation.functionArgs()));
        columnNames.add(selector.name());
        columnTypes.add(function.returnType());
      }
      bldr.aggregate(typeName.typeName(), composite);
      return new SingletonResultSet(columnNames, columnTypes, dataStore.aggregate(bldr.build()));
    } else {
      final VectorQueryBuilder bldr =
          VectorQueryBuilder.newBuilder().addTypeName(typeName.typeName());
      if (filter != null) {
        bldr.constraints(bldr.constraintsFactory().filterConstraints(filter));
      }

      if ((selectors != null) && !selectors.isEmpty()) {
        Set<String> usedAttributes = Sets.newHashSet();
        selectors.forEach(s -> usedAttributes.add(((ColumnSelector) s).columnName()));
        if (filter != null) {
          usedAttributes.addAll(Arrays.asList(DataUtilities.attributeNames(filter)));
        }
        for (String attribute : usedAttributes) {
          if (featureType.getDescriptor(attribute) == null) {
            throw new RuntimeException(
                "No column named " + attribute + " was found in " + typeName.typeName());
          }
        }
        bldr.subsetFields(
            typeName.typeName(),
            usedAttributes.toArray(new String[usedAttributes.size()]));
      } else {
        selectors =
            Lists.transform(
                featureType.getAttributeDescriptors(),
                ad -> new ColumnSelector(ad.getLocalName()));
      }
      if (limit != null) {
        bldr.limit(limit);
      }
      return new SimpleFeatureResultSet(selectors, dataStore.query(bldr.build()), featureType);
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
  public QualifiedTypeName typeName() {
    return typeName;
  }

  /**
   * @return the filter for the query
   */
  public Filter filter() {
    return filter;
  }

  /**
   * @return the limit for the query
   */
  public Integer limit() {
    return limit;
  }

  /**
   * @return the selectors for the query
   */
  public List<Selector> selectors() {
    return selectors;
  }

  @Override
  public String getStoreName() {
    return typeName.storeName();
  }
}
