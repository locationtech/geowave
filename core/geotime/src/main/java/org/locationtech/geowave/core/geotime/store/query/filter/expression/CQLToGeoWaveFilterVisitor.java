/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.filter.expression;

import java.util.Calendar;
import java.util.Date;
import org.apache.commons.lang.StringUtils;
import org.locationtech.geowave.core.geotime.adapter.SpatialFieldDescriptor;
import org.locationtech.geowave.core.geotime.adapter.TemporalFieldDescriptor;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialExpression;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialFieldValue;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialLiteral;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalExpression;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalFieldValue;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalLiteral;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.query.filter.expression.And;
import org.locationtech.geowave.core.store.query.filter.expression.BooleanFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.BooleanLiteral;
import org.locationtech.geowave.core.store.query.filter.expression.ComparableExpression;
import org.locationtech.geowave.core.store.query.filter.expression.Expression;
import org.locationtech.geowave.core.store.query.filter.expression.Filter;
import org.locationtech.geowave.core.store.query.filter.expression.GenericFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.GenericLiteral;
import org.locationtech.geowave.core.store.query.filter.expression.Literal;
import org.locationtech.geowave.core.store.query.filter.expression.Not;
import org.locationtech.geowave.core.store.query.filter.expression.Or;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericExpression;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericLiteral;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextExpression;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextLiteral;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.filter.FilterVisitor;
import org.opengis.filter.expression.ExpressionVisitor;

/**
 * This filter attempts to convert a CQL filter into a GeoWave filter. Since GeoWave filters are a
 * subset of the functionality found in CQL, an exception will be thrown if the filter cannot be
 * mapped exactly.
 */
public class CQLToGeoWaveFilterVisitor implements FilterVisitor, ExpressionVisitor {

  private enum ExpressionType {
    ANY, NUMERIC, TEXT, SPATIAL, TEMPORAL, BOOLEAN,
  }

  private final DataTypeAdapter<?> adapter;

  public CQLToGeoWaveFilterVisitor(final DataTypeAdapter<?> adapter) {
    this.adapter = adapter;
  }

  @Override
  public Object visit(
      final org.opengis.filter.expression.NilExpression expression,
      final Object extraData) {
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.expression.Add expression, final Object extraData) {
    final Object expr1 = expression.getExpression1().accept(this, ExpressionType.NUMERIC);
    final Object expr2 = expression.getExpression2().accept(this, ExpressionType.NUMERIC);
    if ((expr1 instanceof NumericExpression) && (expr2 instanceof NumericExpression)) {
      return ((NumericExpression) expr1).add(expr2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(
      final org.opengis.filter.expression.Subtract expression,
      final Object extraData) {
    final Object expr1 = expression.getExpression1().accept(this, ExpressionType.NUMERIC);
    final Object expr2 = expression.getExpression2().accept(this, ExpressionType.NUMERIC);
    if ((expr1 instanceof NumericExpression) && (expr2 instanceof NumericExpression)) {
      return ((NumericExpression) expr1).subtract(expr2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(
      final org.opengis.filter.expression.Multiply expression,
      final Object extraData) {
    final Object expr1 = expression.getExpression1().accept(this, ExpressionType.NUMERIC);
    final Object expr2 = expression.getExpression2().accept(this, ExpressionType.NUMERIC);
    if ((expr1 instanceof NumericExpression) && (expr2 instanceof NumericExpression)) {
      return ((NumericExpression) expr1).multiplyBy(expr2);
    }
    throw new CQLToGeoWaveConversionException();
  }


  @Override
  public Object visit(
      final org.opengis.filter.expression.Divide expression,
      final Object extraData) {
    final Object expr1 = expression.getExpression1().accept(this, ExpressionType.NUMERIC);
    final Object expr2 = expression.getExpression2().accept(this, ExpressionType.NUMERIC);
    if ((expr1 instanceof NumericExpression) && (expr2 instanceof NumericExpression)) {
      return ((NumericExpression) expr1).divideBy(expr2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(
      final org.opengis.filter.expression.Function expression,
      final Object extraData) {
    // TODO: Add support for commonly used functions (abs, strConcat, strEndsWith,
    // strEqualsIgnoreCase, strStartsWith)
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(
      final org.opengis.filter.expression.Literal expression,
      final Object extraData) {
    final Object value = expression.getValue();
    if ((extraData != null) && (extraData instanceof ExpressionType)) {
      switch ((ExpressionType) extraData) {
        case NUMERIC:
          return NumericLiteral.of((Number) value);
        case SPATIAL:
          return SpatialLiteral.of(value);
        case TEMPORAL:
          return TemporalLiteral.of(value);
        case TEXT:
          return TextLiteral.of((String) value);
        case BOOLEAN:
          return BooleanLiteral.of(value);
        default:
          break;
      }
    }
    return inferLiteral(value);
  }

  private Literal<?> inferLiteral(final Object object) {
    if ((object instanceof Geometry) || (object instanceof Envelope)) {
      return SpatialLiteral.of(object);
    }
    if (object instanceof Boolean) {
      return BooleanLiteral.of(object);
    }
    if (object instanceof Number) {
      return NumericLiteral.of((Number) object);
    }
    if (object instanceof String) {
      return TextLiteral.of((String) object);
    }
    if ((object instanceof Date) || (object instanceof Calendar)) {
      return TemporalLiteral.of(object);
    }
    return GenericLiteral.of(object);
  }

  @Override
  public Object visit(
      final org.opengis.filter.expression.PropertyName expression,
      final Object extraData) {
    String value = expression.getPropertyName();
    FieldDescriptor<?> descriptor = adapter.getFieldDescriptor(value);
    if (descriptor == null && (value == null || value.length() == 0)) {
      if (extraData != null && extraData.equals(ExpressionType.SPATIAL)) {
        // Attempt to infer the default geometry field
        final FieldDescriptor<?>[] descriptors = adapter.getFieldDescriptors();
        for (final FieldDescriptor<?> field : descriptors) {
          if (Geometry.class.isAssignableFrom(field.bindingClass())) {
            value = field.fieldName();
            descriptor = field;
            break;
          }
        }
      }
    }
    if (descriptor == null) {
      throw new CQLToGeoWaveConversionException();
    }
    if ((extraData != null) && (extraData instanceof ExpressionType)) {
      switch ((ExpressionType) extraData) {
        case NUMERIC:
          return NumericFieldValue.of(value);
        case SPATIAL:
          return SpatialFieldValue.of(value);
        case TEMPORAL:
          return TemporalFieldValue.of(value);
        case TEXT:
          return TextFieldValue.of(value);
        case BOOLEAN:
          return BooleanFieldValue.of(value);
        default:
          break;
      }
    }
    if ((descriptor instanceof SpatialFieldDescriptor)
        || Geometry.class.isAssignableFrom(descriptor.bindingClass())
        || Envelope.class.isAssignableFrom(descriptor.bindingClass())) {
      return SpatialFieldValue.of(value);
    }
    if ((descriptor instanceof TemporalFieldDescriptor)
        || Date.class.isAssignableFrom(descriptor.bindingClass())
        || Calendar.class.isAssignableFrom(descriptor.bindingClass())) {
      return TemporalFieldValue.of(value);
    }
    if (Boolean.class.isAssignableFrom(descriptor.bindingClass())) {
      return BooleanFieldValue.of(value);
    }
    if (Number.class.isAssignableFrom(descriptor.bindingClass())) {
      return NumericFieldValue.of(value);
    }
    if (String.class.isAssignableFrom(descriptor.bindingClass())) {
      return TextFieldValue.of(value);
    }
    return GenericFieldValue.of(value);
  }

  @Override
  public Object visitNullFilter(final Object extraData) {
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.ExcludeFilter filter, final Object extraData) {
    return Filter.exclude();
  }

  @Override
  public Object visit(final org.opengis.filter.IncludeFilter filter, final Object extraData) {
    return Filter.include();
  }

  @Override
  public Object visit(final org.opengis.filter.And filter, final Object extraData) {
    final Filter[] children =
        filter.getChildren().stream().map(f -> f.accept(this, extraData)).filter(
            f -> f instanceof Filter).toArray(Filter[]::new);
    if (children.length == filter.getChildren().size()) {
      return new And(children);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.Or filter, final Object extraData) {
    final Filter[] children =
        filter.getChildren().stream().map(f -> f.accept(this, extraData)).filter(
            f -> f instanceof Filter).toArray(Filter[]::new);
    if (children.length == filter.getChildren().size()) {
      return new Or(children);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.Not filter, final Object extraData) {
    final Object transformed = filter.getFilter().accept(this, extraData);
    if (transformed instanceof Filter) {
      return new Not((Filter) transformed);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.Id filter, final Object extraData) {
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.PropertyIsBetween filter, final Object extraData) {
    final Object expression = filter.getExpression().accept(this, ExpressionType.ANY);
    final Object lowerBound = filter.getLowerBoundary().accept(this, ExpressionType.ANY);
    final Object upperBound = filter.getUpperBoundary().accept(this, ExpressionType.ANY);
    if ((expression instanceof ComparableExpression)
        && (lowerBound instanceof ComparableExpression)
        && (upperBound instanceof ComparableExpression)) {
      return ((ComparableExpression<?>) expression).isBetween(lowerBound, upperBound);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.PropertyIsEqualTo filter, final Object extraData) {
    final Object expression1 = filter.getExpression1().accept(this, ExpressionType.ANY);
    final Object expression2 = filter.getExpression2().accept(this, ExpressionType.ANY);
    if ((expression1 instanceof Expression) && (expression2 instanceof Expression)) {
      return ((Expression<?>) expression1).isEqualTo(expression2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(
      final org.opengis.filter.PropertyIsNotEqualTo filter,
      final Object extraData) {
    final Object expression1 = filter.getExpression1().accept(this, ExpressionType.ANY);
    final Object expression2 = filter.getExpression2().accept(this, ExpressionType.ANY);
    if ((expression1 instanceof Expression) && (expression2 instanceof Expression)) {
      return ((Expression<?>) expression1).isNotEqualTo(expression2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(
      final org.opengis.filter.PropertyIsGreaterThan filter,
      final Object extraData) {
    final Object expression1 = filter.getExpression1().accept(this, ExpressionType.ANY);
    final Object expression2 = filter.getExpression2().accept(this, ExpressionType.ANY);
    if ((expression1 instanceof ComparableExpression)
        && (expression2 instanceof ComparableExpression)) {
      return ((ComparableExpression<?>) expression1).isGreaterThan(expression2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(
      final org.opengis.filter.PropertyIsGreaterThanOrEqualTo filter,
      final Object extraData) {
    final Object expression1 = filter.getExpression1().accept(this, ExpressionType.ANY);
    final Object expression2 = filter.getExpression2().accept(this, ExpressionType.ANY);
    if ((expression1 instanceof ComparableExpression)
        && (expression2 instanceof ComparableExpression)) {
      return ((ComparableExpression<?>) expression1).isGreaterThanOrEqualTo(expression2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.PropertyIsLessThan filter, final Object extraData) {
    final Object expression1 = filter.getExpression1().accept(this, ExpressionType.ANY);
    final Object expression2 = filter.getExpression2().accept(this, ExpressionType.ANY);
    if ((expression1 instanceof ComparableExpression)
        && (expression2 instanceof ComparableExpression)) {
      return ((ComparableExpression<?>) expression1).isLessThan(expression2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(
      final org.opengis.filter.PropertyIsLessThanOrEqualTo filter,
      final Object extraData) {
    final Object expression1 = filter.getExpression1().accept(this, ExpressionType.ANY);
    final Object expression2 = filter.getExpression2().accept(this, ExpressionType.ANY);
    if ((expression1 instanceof ComparableExpression)
        && (expression2 instanceof ComparableExpression)) {
      return ((ComparableExpression<?>) expression1).isLessThanOrEqualTo(expression2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.PropertyIsLike filter, final Object extraData) {
    final Object expression = filter.getExpression().accept(this, ExpressionType.TEXT);
    if (!(expression instanceof TextExpression)) {
      throw new CQLToGeoWaveConversionException();
    }
    final String likeStr = filter.getLiteral();
    if (likeStr.matches(
        ".*(^\\b|[^\\" + filter.getEscape() + "])" + filter.getSingleChar() + ".*")) {
      // We can't handle character wildcards
      throw new CQLToGeoWaveConversionException();
    }
    final int count = StringUtils.countMatches(likeStr, filter.getWildCard());
    if (count == 0) {
      return ((TextExpression) expression).isEqualTo(
          StringUtils.replace(likeStr, filter.getEscape(), ""),
          !filter.isMatchingCase());
    } else if (count == 1) {
      if (likeStr.startsWith(filter.getWildCard())) {
        return ((TextExpression) expression).endsWith(
            likeStr.substring(filter.getWildCard().length()),
            !filter.isMatchingCase());
      }
      if (likeStr.endsWith(filter.getWildCard())) {
        return ((TextExpression) expression).startsWith(
            likeStr.substring(0, likeStr.length() - filter.getWildCard().length()),
            !filter.isMatchingCase());
      }
    } else if (count == 2) {
      if (likeStr.startsWith(filter.getWildCard()) && likeStr.endsWith(filter.getWildCard())) {
        return ((TextExpression) expression).contains(
            likeStr.substring(
                filter.getWildCard().length(),
                likeStr.length() - filter.getWildCard().length()),
            !filter.isMatchingCase());
      }
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.PropertyIsNull filter, final Object extraData) {
    final Object expression = filter.getExpression().accept(this, ExpressionType.ANY);
    if (expression instanceof Expression) {
      return ((Expression<?>) expression).isNull();
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.PropertyIsNil filter, final Object extraData) {
    final Object expression = filter.getExpression().accept(this, ExpressionType.ANY);
    if (expression instanceof Expression) {
      return ((Expression<?>) expression).isNull();
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.spatial.BBOX filter, final Object extraData) {
    final Object expression = filter.getExpression1().accept(this, ExpressionType.SPATIAL);
    if (expression instanceof SpatialExpression) {
      return ((SpatialExpression) expression).bbox(
          filter.getBounds().getMinX(),
          filter.getBounds().getMinY(),
          filter.getBounds().getMaxX(),
          filter.getBounds().getMaxY(),
          filter.getBounds().getCoordinateReferenceSystem());
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.spatial.Beyond filter, final Object extraData) {
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.spatial.Contains filter, final Object extraData) {
    final Object expression1 = filter.getExpression1().accept(this, ExpressionType.SPATIAL);
    final Object expression2 = filter.getExpression2().accept(this, ExpressionType.SPATIAL);
    if ((expression1 instanceof SpatialExpression) && (expression2 instanceof SpatialExpression)) {
      return ((SpatialExpression) expression1).contains(expression2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.spatial.Crosses filter, final Object extraData) {
    final Object expression1 = filter.getExpression1().accept(this, ExpressionType.SPATIAL);
    final Object expression2 = filter.getExpression2().accept(this, ExpressionType.SPATIAL);
    if ((expression1 instanceof SpatialExpression) && (expression2 instanceof SpatialExpression)) {
      return ((SpatialExpression) expression1).crosses(expression2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.spatial.Disjoint filter, final Object extraData) {
    final Object expression1 = filter.getExpression1().accept(this, ExpressionType.SPATIAL);
    final Object expression2 = filter.getExpression2().accept(this, ExpressionType.SPATIAL);
    if ((expression1 instanceof SpatialExpression) && (expression2 instanceof SpatialExpression)) {
      return ((SpatialExpression) expression1).disjoint(expression2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.spatial.DWithin filter, final Object extraData) {
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.spatial.Equals filter, final Object extraData) {
    final Object expression1 = filter.getExpression1().accept(this, ExpressionType.SPATIAL);
    final Object expression2 = filter.getExpression2().accept(this, ExpressionType.SPATIAL);
    if ((expression1 instanceof SpatialExpression) && (expression2 instanceof SpatialExpression)) {
      return ((SpatialExpression) expression1).isEqualTo(expression2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.spatial.Intersects filter, final Object extraData) {
    final Object expression1 = filter.getExpression1().accept(this, ExpressionType.SPATIAL);
    final Object expression2 = filter.getExpression2().accept(this, ExpressionType.SPATIAL);
    if ((expression1 instanceof SpatialExpression) && (expression2 instanceof SpatialExpression)) {
      return ((SpatialExpression) expression1).intersects(expression2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.spatial.Overlaps filter, final Object extraData) {
    final Object expression1 = filter.getExpression1().accept(this, ExpressionType.SPATIAL);
    final Object expression2 = filter.getExpression2().accept(this, ExpressionType.SPATIAL);
    if ((expression1 instanceof SpatialExpression) && (expression2 instanceof SpatialExpression)) {
      return ((SpatialExpression) expression1).overlaps(expression2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.spatial.Touches filter, final Object extraData) {
    final Object expression1 = filter.getExpression1().accept(this, ExpressionType.SPATIAL);
    final Object expression2 = filter.getExpression2().accept(this, ExpressionType.SPATIAL);
    if ((expression1 instanceof SpatialExpression) && (expression2 instanceof SpatialExpression)) {
      return ((SpatialExpression) expression1).touches(expression2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.spatial.Within filter, final Object extraData) {
    final Object expression1 = filter.getExpression1().accept(this, ExpressionType.SPATIAL);
    final Object expression2 = filter.getExpression2().accept(this, ExpressionType.SPATIAL);
    if ((expression1 instanceof SpatialExpression) && (expression2 instanceof SpatialExpression)) {
      return ((SpatialExpression) expression1).within(expression2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.temporal.After after, final Object extraData) {
    final Object expression1 = after.getExpression1().accept(this, ExpressionType.TEMPORAL);
    final Object expression2 = after.getExpression2().accept(this, ExpressionType.TEMPORAL);
    if ((expression1 instanceof TemporalExpression)
        && (expression2 instanceof TemporalExpression)) {
      return ((TemporalExpression) expression1).isAfter(expression2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(
      final org.opengis.filter.temporal.AnyInteracts anyInteracts,
      final Object extraData) {
    final Object expression1 = anyInteracts.getExpression1().accept(this, ExpressionType.TEMPORAL);
    final Object expression2 = anyInteracts.getExpression2().accept(this, ExpressionType.TEMPORAL);
    if ((expression1 instanceof TemporalExpression)
        && (expression2 instanceof TemporalExpression)) {
      return ((TemporalExpression) expression1).overlaps(expression2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.temporal.Before before, final Object extraData) {
    final Object expression1 = before.getExpression1().accept(this, ExpressionType.TEMPORAL);
    final Object expression2 = before.getExpression2().accept(this, ExpressionType.TEMPORAL);
    if ((expression1 instanceof TemporalExpression)
        && (expression2 instanceof TemporalExpression)) {
      return ((TemporalExpression) expression1).isBefore(expression2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.temporal.Begins begins, final Object extraData) {
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.temporal.BegunBy begunBy, final Object extraData) {
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.temporal.During during, final Object extraData) {
    final Object expression1 = during.getExpression1().accept(this, ExpressionType.TEMPORAL);
    final Object expression2 = during.getExpression2().accept(this, ExpressionType.TEMPORAL);
    if ((expression1 instanceof TemporalExpression)
        && (expression2 instanceof TemporalExpression)) {
      return ((TemporalExpression) expression1).isDuring(expression2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.temporal.EndedBy endedBy, final Object extraData) {
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.temporal.Ends ends, final Object extraData) {
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.temporal.Meets meets, final Object extraData) {
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.temporal.MetBy metBy, final Object extraData) {
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(
      final org.opengis.filter.temporal.OverlappedBy overlappedBy,
      final Object extraData) {
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(
      final org.opengis.filter.temporal.TContains contains,
      final Object extraData) {
    final Object expression1 = contains.getExpression1().accept(this, ExpressionType.TEMPORAL);
    final Object expression2 = contains.getExpression2().accept(this, ExpressionType.TEMPORAL);
    if ((expression1 instanceof TemporalExpression)
        && (expression2 instanceof TemporalExpression)) {
      // This is really just the inverse of `During`
      return ((TemporalExpression) expression2).isDuring(expression1);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(final org.opengis.filter.temporal.TEquals equals, final Object extraData) {
    final Object expression1 = equals.getExpression1().accept(this, ExpressionType.TEMPORAL);
    final Object expression2 = equals.getExpression2().accept(this, ExpressionType.TEMPORAL);
    if ((expression1 instanceof Expression) && (expression2 instanceof Expression)) {
      return ((TemporalExpression) expression1).isEqualTo(expression2);
    }
    throw new CQLToGeoWaveConversionException();
  }

  @Override
  public Object visit(
      final org.opengis.filter.temporal.TOverlaps contains,
      final Object extraData) {
    throw new CQLToGeoWaveConversionException();
  }

}
