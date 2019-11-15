/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.gwql;

import java.util.List;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.spark_project.guava.collect.Lists;

/**
 * A result set that wraps SimpleFeature results using a given set of column selectors.
 */
public class SimpleFeatureResultSet implements ResultSet {

  private final List<Selector> selectors;
  private final CloseableIterator<SimpleFeature> features;
  private final SimpleFeatureType featureType;

  /**
   * @param selectors the columns to select from the features
   * @param features the feature results
   * @param featureType the feature type of the features
   */
  public SimpleFeatureResultSet(
      final List<Selector> selectors,
      final CloseableIterator<SimpleFeature> features,
      final SimpleFeatureType featureType) {
    this.selectors = selectors;
    this.features = features;
    this.featureType = featureType;
  }

  @Override
  public void close() {
    features.close();
  }

  @Override
  public boolean hasNext() {
    return features.hasNext();
  }

  @Override
  public Result next() {
    SimpleFeature feature = features.next();
    List<Object> values = Lists.newArrayListWithCapacity(selectors.size());
    for (Selector column : selectors) {
      if (column instanceof ColumnSelector) {
        values.add(feature.getAttribute(((ColumnSelector) column).columnName()));
      }
    }

    return new Result(values);
  }

  @Override
  public int columnCount() {
    return selectors.size();
  }

  @Override
  public String columnName(final int index) {
    return selectors.get(index).name();
  }

  @Override
  public Class<?> columnType(final int index) {
    ColumnSelector column = (ColumnSelector) selectors.get(index);
    return featureType.getDescriptor(column.columnName()).getType().getBinding();
  }

  /**
   * @return the feature type
   */
  public SimpleFeatureType featureType() {
    return featureType;
  }

  @Override
  public CoordinateReferenceSystem getCRS() {
    return featureType.getCoordinateReferenceSystem();
  }

}
