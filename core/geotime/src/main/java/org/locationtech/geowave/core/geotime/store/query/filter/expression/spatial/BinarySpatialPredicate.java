/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial;

import java.util.Map;
import java.util.Set;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IndexFieldMapper;
import org.locationtech.geowave.core.store.query.filter.expression.BinaryPredicate;
import org.locationtech.geowave.core.store.query.filter.expression.FieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.FilterConstraints;
import org.locationtech.geowave.core.store.query.filter.expression.FilterRange;
import org.locationtech.geowave.core.store.query.filter.expression.IndexFieldConstraints;
import org.locationtech.geowave.core.store.query.filter.expression.IndexFieldConstraints.DimensionConstraints;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericFieldConstraints;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Abstract class for comparing two spatial expressions. It handles any necessary CRS
 * transformations and delegates the actual comparison operation to the child classes.
 */
public abstract class BinarySpatialPredicate extends BinaryPredicate<SpatialExpression> {

  public BinarySpatialPredicate() {}

  public BinarySpatialPredicate(
      final SpatialExpression expression1,
      final SpatialExpression expression2) {
    super(expression1, expression2);
  }

  @Override
  public void prepare(
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    CoordinateReferenceSystem expression1Crs = expression1.getCRS(adapter);
    CoordinateReferenceSystem expression2Crs = expression2.getCRS(adapter);
    if (expression1.isLiteral() && !(expression1 instanceof SpatialLiteral)) {
      expression1 = SpatialLiteral.of(expression1.evaluateValue(null), expression1Crs);
    }
    if (expression2.isLiteral() && !(expression2 instanceof SpatialLiteral)) {
      expression2 = SpatialLiteral.of(expression2.evaluateValue(null), expression2Crs);
    }
    if ((expression1 instanceof FieldValue)
        && isFieldMappedToIndex(((FieldValue<?>) expression1).getFieldName(), indexMapping)) {
      expression1Crs = GeometryUtils.getIndexCrs(index);
    }
    if ((expression2 instanceof FieldValue)
        && isFieldMappedToIndex(((FieldValue<?>) expression2).getFieldName(), indexMapping)) {
      expression2Crs = GeometryUtils.getIndexCrs(index);
    }
    if (expression1.isLiteral()) {
      ((SpatialLiteral) expression1).prepare(expression2Crs);
    } else if (expression2.isLiteral()) {
      ((SpatialLiteral) expression2).prepare(expression1Crs);
    }
  }

  private boolean isFieldMappedToIndex(
      final String fieldName,
      final AdapterToIndexMapping indexMapping) {
    for (final IndexFieldMapper<?, ?> mapper : indexMapping.getIndexFieldMappers()) {
      for (final String adapterField : mapper.getAdapterFields()) {
        if (adapterField.equals(fieldName)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean evaluate(final Map<String, Object> fieldValues) {
    final Object value1 = expression1.evaluateValue(fieldValues);
    final Object value2 = expression2.evaluateValue(fieldValues);
    if ((value1 == null) || (value2 == null)) {
      return false;
    }
    return evaluateInternal((FilterGeometry) value1, (FilterGeometry) value2);
  }

  @Override
  public <T> boolean evaluate(final DataTypeAdapter<T> adapter, final T entry) {
    final Object value1 = expression1.evaluateValue(adapter, entry);
    final Object value2 = expression2.evaluateValue(adapter, entry);
    if ((value1 == null) || (value2 == null)) {
      return false;
    }
    return evaluateInternal((FilterGeometry) value1, (FilterGeometry) value2);
  }

  protected abstract boolean evaluateInternal(
      final FilterGeometry value1,
      final FilterGeometry value2);



  @Override
  public Set<String> getConstrainableFields() {
    if ((expression1 instanceof FieldValue) && expression2.isLiteral()) {
      return Sets.newHashSet(((FieldValue<?>) expression1).getFieldName());
    } else if ((expression2 instanceof FieldValue) && expression1.isLiteral()) {
      return Sets.newHashSet(((FieldValue<?>) expression2).getFieldName());
    }
    return Sets.newHashSet();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V extends Comparable<V>> FilterConstraints<V> getConstraints(
      final Class<V> constraintClass,
      final DataStatisticsStore statsStore,
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index,
      final Set<String> indexedFields) {
    if (!constraintClass.isAssignableFrom(Double.class)) {
      return FilterConstraints.empty();
    }
    final Map<Integer, DimensionConstraints<Double>> dimensionRanges = Maps.newHashMap();
    FilterGeometry literal = null;
    String fieldName = null;
    CoordinateReferenceSystem literalCRS = GeometryUtils.getDefaultCRS();
    if ((expression1 instanceof FieldValue)
        && indexedFields.contains(((FieldValue<?>) expression1).getFieldName())
        && expression2.isLiteral()) {
      literal = expression2.evaluateValue(null, null);
      if (expression2 instanceof SpatialExpression) {
        literalCRS = expression2.getCRS(adapter);
      }
      fieldName = ((FieldValue<?>) expression1).getFieldName();

    } else if ((expression2 instanceof FieldValue)
        && indexedFields.contains(((FieldValue<?>) expression2).getFieldName())
        && expression1.isLiteral()) {
      literal = expression1.evaluateValue(null, null);
      if (expression1 instanceof SpatialExpression) {
        literalCRS = expression1.getCRS(adapter);
      }
      fieldName = ((FieldValue<?>) expression2).getFieldName();
    }
    if ((literal != null) && (fieldName != null)) {
      final CoordinateReferenceSystem indexCRS = GeometryUtils.getIndexCrs(index);
      Geometry literalGeometry = literal.getGeometry();
      if ((indexCRS != null) && !indexCRS.equals(literalCRS)) {
        try {
          literalGeometry =
              GeometryUtils.crsTransform(
                  literalGeometry,
                  CRS.findMathTransform(literalCRS, indexCRS));
        } catch (final FactoryException e) {
          throw new RuntimeException("Unable to transform spatial literal to the index CRS.");
        }
      }
      final Envelope envelope = literalGeometry.getEnvelopeInternal();
      if (!envelope.isNull()) {
        dimensionRanges.put(
            0,
            DimensionConstraints.of(
                Lists.newArrayList(
                    FilterRange.of(
                        envelope.getMinX(),
                        envelope.getMaxX(),
                        true,
                        true,
                        isExact()))));
        dimensionRanges.put(
            1,
            DimensionConstraints.of(
                Lists.newArrayList(
                    FilterRange.of(
                        envelope.getMinY(),
                        envelope.getMaxY(),
                        true,
                        true,
                        isExact()))));
      }
    }
    if (dimensionRanges.isEmpty()) {
      return FilterConstraints.empty();
    }
    return FilterConstraints.of(
        adapter,
        indexMapping,
        index,
        fieldName,
        (IndexFieldConstraints<V>) NumericFieldConstraints.of(dimensionRanges));
  }

  protected abstract boolean isExact();

}
