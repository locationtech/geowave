/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.util;

import org.apache.commons.lang3.tuple.Pair;
import org.geotools.filter.visitor.NullFilterVisitor;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter.CompareOperation;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.filter.And;
import org.opengis.filter.ExcludeFilter;
import org.opengis.filter.Filter;
import org.opengis.filter.Id;
import org.opengis.filter.IncludeFilter;
import org.opengis.filter.Not;
import org.opengis.filter.Or;
import org.opengis.filter.PropertyIsBetween;
import org.opengis.filter.PropertyIsEqualTo;
import org.opengis.filter.PropertyIsGreaterThan;
import org.opengis.filter.PropertyIsGreaterThanOrEqualTo;
import org.opengis.filter.PropertyIsLessThan;
import org.opengis.filter.PropertyIsLessThanOrEqualTo;
import org.opengis.filter.PropertyIsLike;
import org.opengis.filter.PropertyIsNotEqualTo;
import org.opengis.filter.PropertyIsNull;
import org.opengis.filter.expression.Add;
import org.opengis.filter.expression.Divide;
import org.opengis.filter.expression.Function;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.Multiply;
import org.opengis.filter.expression.NilExpression;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.expression.Subtract;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.spatial.Beyond;
import org.opengis.filter.spatial.Contains;
import org.opengis.filter.spatial.Crosses;
import org.opengis.filter.spatial.DWithin;
import org.opengis.filter.spatial.Disjoint;
import org.opengis.filter.spatial.Equals;
import org.opengis.filter.spatial.Intersects;
import org.opengis.filter.spatial.Overlaps;
import org.opengis.filter.spatial.Touches;
import org.opengis.filter.spatial.Within;
import org.opengis.geometry.BoundingBox;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to exact single query geometry and its associated predicate from a CQL
 * expression. There are three possible outcomes based on the extracted results. 1) If CQL
 * expression is simple then we are able to extract query geometry and predicate successfully. 2) If
 * CQL expression combines multiple dissimilar geometric relationships (i.e. "BBOX(geom,...) AND
 * TOUCHES(geom,...)") then we wont be able combine that into a single query geometry and predicate.
 * In which case, we will only return query geometry for the purpose of creating linear constraints
 * and predicate value will be null. However, we are able to combine multiple geometric
 * relationships into one query/predicate if their predicates are same (i.e. "INTERSECTS(geom,...)
 * AND INTERSECTS(geom,...)") 3) In some case, we won't be able to extract query geometry and
 * predicate at all. In that case, we simply return null. This occurs if CQL expression doesn't
 * contain any geometric constraints or CQL expression has non-inclusive filter (i.e. NOT or
 * DISJOINT(...)).
 */
public class ExtractGeometryFilterVisitor extends NullFilterVisitor {

  private static Logger LOGGER = LoggerFactory.getLogger(ExtractGeometryFilterVisitor.class);

  private final CoordinateReferenceSystem crs;

  private final String attributeOfInterest;

  /**
   * This FilterVisitor is stateless - use ExtractGeometryFilterVisitor.BOUNDS_VISITOR. You may also
   * subclass in order to reuse this functionality in your own FilterVisitor implementation.
   */
  public ExtractGeometryFilterVisitor(
      final CoordinateReferenceSystem crs,
      final String attributeOfInterest) {
    this.crs = crs;
    this.attributeOfInterest = attributeOfInterest;
  }

  /**
   * @param filter
   * @param crs
   * @return null if empty constraint (infinite not supported)
   */
  public static ExtractGeometryFilterVisitorResult getConstraints(
      final Filter filter,
      final CoordinateReferenceSystem crs,
      final String attributeOfInterest) {
    final ExtractGeometryFilterVisitorResult geoAndCompareOpData =
        (ExtractGeometryFilterVisitorResult) filter.accept(
            new ExtractGeometryFilterVisitor(crs, attributeOfInterest),
            null);
    if (geoAndCompareOpData == null) {
      return null;
    }
    final Geometry geo = geoAndCompareOpData.getGeometry();
    // empty or infinite geometry simply return null as we can't create
    // linear constraints from
    if ((geo == null) || geo.isEmpty()) {
      return null;
    }
    final double area = geo.getArea();
    if (Double.isInfinite(area) || Double.isNaN(area)) {
      return null;
    }
    return geoAndCompareOpData;
  }

  /**
   * Produce an ReferencedEnvelope from the provided data parameter.
   *
   * @param data
   * @return ReferencedEnvelope
   */
  private Geometry bbox(final Object data) {
    try {
      if (data == null) {
        return null;
      } else if (data instanceof Geometry) {
        return (Geometry) data;
      } else if (data instanceof ReferencedEnvelope) {
        return new GeometryFactory().toGeometry(((ReferencedEnvelope) data).transform(crs, true));

      } else if (data instanceof Envelope) {
        return new GeometryFactory().toGeometry((Envelope) data);
      } else if (data instanceof CoordinateReferenceSystem) {
        return new GeometryFactory().toGeometry(
            new ReferencedEnvelope((CoordinateReferenceSystem) data).transform(crs, true));
      }
    } catch (TransformException | FactoryException e) {
      LOGGER.warn("Unable to transform geometry", e);
      return null;
    }
    throw new ClassCastException("Could not cast data to ReferencedEnvelope");
  }

  @Override
  public Object visit(final ExcludeFilter filter, final Object data) {
    return new ExtractGeometryFilterVisitorResult(null, null);
  }

  @Override
  public Object visit(final IncludeFilter filter, final Object data) {
    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }

  private Geometry infinity() {
    return GeometryUtils.infinity();
  }

  @Override
  public Object visit(final BBOX filter, final Object data) {
    if (attributeOfInterest.equals(filter.getExpression1().toString())) {
      final Geometry bbox = bbox(data);
      final BoundingBox referencedBBox = filter.getBounds();
      Geometry bounds =
          new GeometryFactory().toGeometry(
              new Envelope(
                  referencedBBox.getMinX(),
                  referencedBBox.getMaxX(),
                  referencedBBox.getMinY(),
                  referencedBBox.getMaxY()));

      if ((crs != null)
          && (referencedBBox.getCoordinateReferenceSystem() != null)
          && !crs.equals(referencedBBox.getCoordinateReferenceSystem())) {
        try {
          bounds =
              JTS.transform(
                  bounds,
                  CRS.findMathTransform(referencedBBox.getCoordinateReferenceSystem(), crs, true));
        } catch (MismatchedDimensionException | TransformException | FactoryException e) {
          LOGGER.error("Unable to transforma bbox", e);
        }
      }
      if (bbox != null) {
        return bbox.union(bounds);
      } else {
        return new ExtractGeometryFilterVisitorResult(bounds, CompareOperation.INTERSECTS);
      }
    } else {
      return new ExtractGeometryFilterVisitorResult(infinity(), null);
    }
  }

  /**
   * Please note we are only visiting literals involved in spatial operations.
   *
   * @param expression hopefully a Geometry or Envelope
   * @param data Incoming BoundingBox (or Envelope or CRS)
   * @return ReferencedEnvelope updated to reflect literal
   */
  @Override
  public Object visit(final Literal expression, final Object data) {
    final Object value = expression.getValue();
    if (value instanceof Geometry) {
      final Geometry geometry = (Geometry) value;
      return geometry;
    } else {
      LOGGER.info("LiteralExpression ignored!");
    }
    return bbox(data);
  }

  @Override
  public Object visit(final And filter, final Object data) {
    ExtractGeometryFilterVisitorResult finalResult = null;
    for (final Filter f : filter.getChildren()) {
      final Object obj = f.accept(this, data);
      if ((obj != null) && (obj instanceof ExtractGeometryFilterVisitorResult)) {
        final ExtractGeometryFilterVisitorResult currentResult =
            (ExtractGeometryFilterVisitorResult) obj;
        final Geometry currentGeom = currentResult.getGeometry();
        final double currentArea = currentGeom.getArea();

        if (finalResult == null) {
          finalResult = currentResult;
        } else if (!Double.isInfinite(currentArea) && !Double.isNaN(currentArea)) {
          // if predicates match then we can combine the geometry as
          // well as predicate
          if (currentResult.matchPredicate(finalResult)) {
            finalResult =
                new ExtractGeometryFilterVisitorResult(
                    finalResult.getGeometry().intersection(currentGeom),
                    currentResult.getCompareOp());
          } else {
            // if predicate doesn't match then still combine
            // geometry but set predicate to null
            finalResult =
                new ExtractGeometryFilterVisitorResult(
                    finalResult.getGeometry().intersection(currentGeom),
                    null);
          }
        } else {
          finalResult = new ExtractGeometryFilterVisitorResult(finalResult.getGeometry(), null);
        }
      }
    }
    return finalResult;
  }

  @Override
  public Object visit(final Not filter, final Object data) {
    // no matter what we have to return an infinite envelope
    // rationale
    // !(finite envelope) -> an unbounded area -> infinite
    // !(non spatial filter) -> infinite (no spatial concern)
    // !(infinite) -> ... infinite, as the first infinite could be the
    // result
    // of !(finite envelope)

    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }

  @Override
  public Object visit(final Or filter, final Object data) {
    ExtractGeometryFilterVisitorResult finalResult =
        new ExtractGeometryFilterVisitorResult(
            new GeometryFactory().toGeometry(new Envelope()),
            null);
    for (final Filter f : filter.getChildren()) {
      final Object obj = f.accept(this, data);
      if ((obj != null) && (obj instanceof ExtractGeometryFilterVisitorResult)) {
        final ExtractGeometryFilterVisitorResult currentResult =
            (ExtractGeometryFilterVisitorResult) obj;
        final Geometry currentGeom = currentResult.getGeometry();
        final double currentArea = currentGeom.getArea();
        if (finalResult.getGeometry().isEmpty()) {
          finalResult = currentResult;
        } else if (!Double.isInfinite(currentArea) && !Double.isNaN(currentArea)) {
          if (currentResult.matchPredicate(finalResult)) {
            finalResult =
                new ExtractGeometryFilterVisitorResult(
                    finalResult.getGeometry().union(currentGeom),
                    currentResult.getCompareOp());
          } else {
            finalResult =
                new ExtractGeometryFilterVisitorResult(
                    finalResult.getGeometry().union(currentGeom),
                    null);
          }
        } else {
          finalResult = new ExtractGeometryFilterVisitorResult(finalResult.getGeometry(), null);
        }
      }
    }
    if (finalResult.getGeometry().isEmpty()) {
      return new ExtractGeometryFilterVisitorResult(infinity(), null);
    }
    return finalResult;
  }

  @Override
  public Object visit(final Beyond filter, final Object data) {
    // beyond a certain distance from a finite object, no way to limit it
    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }

  @Override
  public Object visit(final Contains filter, Object data) {
    if (!attributeOfInterest.equals(filter.getExpression1().toString())) {
      return new ExtractGeometryFilterVisitorResult(infinity(), null);
    }
    data = filter.getExpression2().accept(this, data);

    // since predicate is defined relative to the query geometry we are
    // using WITHIN
    // which is converse of CONTAINS operator
    // CQL Expression "CONTAINS(geo, QueryGeometry)" is equivalent to
    // QueryGeometry.WITHIN(geo)
    return new ExtractGeometryFilterVisitorResult((Geometry) data, CompareOperation.WITHIN);
  }

  @Override
  public Object visit(final Crosses filter, Object data) {
    if (!attributeOfInterest.equals(filter.getExpression1().toString())) {
      return new ExtractGeometryFilterVisitorResult(infinity(), null);
    }
    data = filter.getExpression2().accept(this, data);
    return new ExtractGeometryFilterVisitorResult((Geometry) data, CompareOperation.CROSSES);
  }

  @Override
  public Object visit(final Disjoint filter, final Object data) {
    // disjoint does not define a rectangle, but a hole in the
    // Cartesian plane, no way to limit it
    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }

  @Override
  public Object visit(final DWithin filter, final Object data) {
    final Geometry bbox = bbox(data);
    if (!attributeOfInterest.equals(filter.getExpression1().toString())) {
      return new ExtractGeometryFilterVisitorResult(infinity(), null);
    }

    // we have to take the reference geometry bbox and
    // expand it by the distance.
    // We ignore the unit of measure for the moment
    Literal geometry = null;
    if ((filter.getExpression1() instanceof PropertyName)
        && (filter.getExpression2() instanceof Literal)) {
      geometry = (Literal) filter.getExpression2();
    }
    if ((filter.getExpression2() instanceof PropertyName)
        && (filter.getExpression1() instanceof Literal)) {
      geometry = (Literal) filter.getExpression1();
    }

    // we cannot desume a bbox from this filter
    if (geometry == null) {
      return null;
    }

    final Geometry geom = geometry.evaluate(null, Geometry.class);
    if (geom == null) {
      return new ExtractGeometryFilterVisitorResult(infinity(), null);
    }
    Pair<Geometry, Double> geometryAndDegrees;
    try {
      geometryAndDegrees =
          GeometryUtils.buffer(crs, geom, filter.getDistanceUnits(), filter.getDistance());
    } catch (final TransformException e) {
      LOGGER.error("Cannot transform geometry to CRS", e);
      geometryAndDegrees = Pair.of(geom, filter.getDistance());
    }

    if (bbox != null) {
      return geometryAndDegrees.getLeft().union(bbox);
    } else {
      return new ExtractGeometryFilterVisitorResult(
          geometryAndDegrees.getLeft(),
          CompareOperation.INTERSECTS);
    }
  }

  @Override
  public Object visit(final Equals filter, Object data) {
    if (!attributeOfInterest.equals(filter.getExpression1().toString())) {
      return new ExtractGeometryFilterVisitorResult(infinity(), null);
    }
    data = filter.getExpression2().accept(this, data);
    return new ExtractGeometryFilterVisitorResult((Geometry) data, CompareOperation.EQUALS);
  }

  @Override
  public Object visit(final Intersects filter, Object data) {
    if (!attributeOfInterest.equals(filter.getExpression1().toString())) {
      return new ExtractGeometryFilterVisitorResult(infinity(), null);
    }
    data = filter.getExpression2().accept(this, data);
    return new ExtractGeometryFilterVisitorResult((Geometry) data, CompareOperation.INTERSECTS);
  }

  @Override
  public Object visit(final Overlaps filter, Object data) {
    if (!attributeOfInterest.equals(filter.getExpression1().toString())) {
      return new ExtractGeometryFilterVisitorResult(infinity(), null);
    }
    data = filter.getExpression2().accept(this, data);

    return new ExtractGeometryFilterVisitorResult((Geometry) data, CompareOperation.OVERLAPS);
  }

  @Override
  public Object visit(final Touches filter, Object data) {
    if (!attributeOfInterest.equals(filter.getExpression1().toString())) {
      return new ExtractGeometryFilterVisitorResult(infinity(), null);
    }
    data = filter.getExpression2().accept(this, data);

    return new ExtractGeometryFilterVisitorResult((Geometry) data, CompareOperation.TOUCHES);
  }

  @Override
  public Object visit(final Within filter, Object data) {
    if (!attributeOfInterest.equals(filter.getExpression1().toString())) {
      return new ExtractGeometryFilterVisitorResult(infinity(), null);
    }
    data = filter.getExpression2().accept(this, data);
    // since predicate is defined relative to the query geometry we are
    // using CONTAIN
    // which is converse of WITHIN operator
    // CQL Expression "WITHIN(geo, QueryGeometry)" is equivalent to
    // QueryGeometry.CONTAINS(geo)
    return new ExtractGeometryFilterVisitorResult((Geometry) data, CompareOperation.CONTAINS);
  }

  @Override
  public Object visit(final Add expression, final Object data) {
    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }

  @Override
  public Object visit(final Divide expression, final Object data) {
    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }

  @Override
  public Object visit(final Function expression, final Object data) {
    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }

  @Override
  public Object visit(final Id filter, final Object data) {
    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }

  @Override
  public Object visit(final Multiply expression, final Object data) {
    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }

  @Override
  public Object visit(final NilExpression expression, final Object data) {
    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }

  @Override
  public Object visit(final PropertyIsBetween filter, final Object data) {
    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }

  @Override
  public Object visit(final PropertyIsEqualTo filter, final Object data) {
    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }

  @Override
  public Object visit(final PropertyIsGreaterThan filter, final Object data) {
    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }

  @Override
  public Object visit(final PropertyIsGreaterThanOrEqualTo filter, final Object data) {
    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }

  @Override
  public Object visit(final PropertyIsLessThan filter, final Object data) {
    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }

  @Override
  public Object visit(final PropertyIsLessThanOrEqualTo filter, final Object data) {
    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }

  @Override
  public Object visit(final PropertyIsLike filter, final Object data) {
    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }

  @Override
  public Object visit(final PropertyIsNotEqualTo filter, final Object data) {
    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }

  @Override
  public Object visit(final PropertyIsNull filter, final Object data) {
    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }

  @Override
  public Object visit(final PropertyName expression, final Object data) {
    return new ExtractGeometryFilterVisitorResult(null, null);
  }

  @Override
  public Object visit(final Subtract expression, final Object data) {
    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }

  @Override
  public Object visitNullFilter(final Object data) {
    return new ExtractGeometryFilterVisitorResult(infinity(), null);
  }
}
