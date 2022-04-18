/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.gwql;

import java.util.List;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialExpression;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.query.filter.expression.Expression;
import org.locationtech.geowave.core.store.query.filter.expression.Predicate;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericExpression;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextExpression;
import org.locationtech.geowave.core.store.query.gwql.GWQLParseException;
import org.locationtech.geowave.core.store.query.gwql.function.predicate.PredicateFunction;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class SpatialPredicates {

  private static abstract class AbstractBboxFunction implements PredicateFunction {

    @Override
    public Predicate create(List<Expression<?>> arguments) {
      if (arguments.size() < 5 && arguments.size() > 6) {
        throw new GWQLParseException("Function expects 5 or 6 arguments, got " + arguments.size());
      }
      final SpatialExpression expression =
          GeometryCastableType.toSpatialExpression(arguments.get(0));
      final double minX = getNumber(arguments.get(1));
      final double minY = getNumber(arguments.get(2));
      final double maxX = getNumber(arguments.get(3));
      final double maxY = getNumber(arguments.get(4));
      if (arguments.size() == 6) {
        if (arguments.get(5).isLiteral() && arguments.get(5) instanceof TextExpression) {
          final String crsStr = ((TextExpression) arguments.get(5)).evaluateValue(null);
          return bbox(expression, minX, minY, maxX, maxY, GeometryUtils.decodeCRS(crsStr));
        }
        throw new GWQLParseException(
            "Expected a text literal for the coordinate reference system.");
      } else {
        return bbox(expression, minX, minY, maxX, maxY, null);
      }
    }

    protected abstract Predicate bbox(
        final SpatialExpression expression,
        final double minX,
        final double minY,
        final double maxX,
        final double maxY,
        final CoordinateReferenceSystem crs);

    private double getNumber(final Expression<?> expression) {
      if (expression.isLiteral() && expression instanceof NumericExpression) {
        return ((NumericExpression) expression).evaluateValue(null);
      }
      throw new GWQLParseException("Expected a numeric literal for bounding box constraints.");
    }

  }

  public static class BboxFunction extends AbstractBboxFunction {
    @Override
    public String getName() {
      return "BBOX";
    }

    @Override
    protected Predicate bbox(
        SpatialExpression expression,
        double minX,
        double minY,
        double maxX,
        double maxY,
        CoordinateReferenceSystem crs) {
      if (crs == null) {
        return expression.bbox(minX, minY, maxX, maxY);
      }
      return expression.bbox(minX, minY, maxX, maxY, crs);
    }
  }

  public static class BboxLooseFunction extends AbstractBboxFunction {
    @Override
    public String getName() {
      return "BBOXLOOSE";
    }

    @Override
    protected Predicate bbox(
        SpatialExpression expression,
        double minX,
        double minY,
        double maxX,
        double maxY,
        CoordinateReferenceSystem crs) {
      if (crs == null) {
        return expression.bboxLoose(minX, minY, maxX, maxY);
      }
      return expression.bboxLoose(minX, minY, maxX, maxY, crs);
    }
  }

  private static abstract class SpatialPredicateFunction implements PredicateFunction {
    @Override
    public Predicate create(List<Expression<?>> arguments) {
      if (arguments.size() == 2) {
        final SpatialExpression expression1 =
            GeometryCastableType.toSpatialExpression(arguments.get(0));
        final SpatialExpression expression2 =
            GeometryCastableType.toSpatialExpression(arguments.get(1));
        return createInternal(expression1, expression2);
      }
      throw new GWQLParseException("Function expects 2 arguments, got " + arguments.size());
    }

    protected abstract Predicate createInternal(
        final SpatialExpression expression1,
        final SpatialExpression expression2);
  }

  public static class IntersectsFunction extends SpatialPredicateFunction {
    @Override
    public String getName() {
      return "INTERSECTS";
    }

    @Override
    protected Predicate createInternal(
        final SpatialExpression expression1,
        final SpatialExpression expression2) {
      return expression1.intersects(expression2);
    }
  }

  public static class IntersectsLooseFunction extends SpatialPredicateFunction {
    @Override
    public String getName() {
      return "INTERSECTSLOOSE";
    }

    @Override
    protected Predicate createInternal(
        final SpatialExpression expression1,
        final SpatialExpression expression2) {
      return expression1.intersectsLoose(expression2);
    }
  }

  public static class DisjointFunction extends SpatialPredicateFunction {
    @Override
    public String getName() {
      return "DISJOINT";
    }

    @Override
    protected Predicate createInternal(
        final SpatialExpression expression1,
        final SpatialExpression expression2) {
      return expression1.disjoint(expression2);
    }
  }

  public static class DisjointLooseFunction extends SpatialPredicateFunction {
    @Override
    public String getName() {
      return "DISJOINTLOOSE";
    }

    @Override
    protected Predicate createInternal(
        final SpatialExpression expression1,
        final SpatialExpression expression2) {
      return expression1.disjointLoose(expression2);
    }
  }

  public static class CrossesFunction extends SpatialPredicateFunction {
    @Override
    public String getName() {
      return "CROSSES";
    }

    @Override
    protected Predicate createInternal(
        final SpatialExpression expression1,
        final SpatialExpression expression2) {
      return expression1.crosses(expression2);
    }
  }

  public static class OverlapsFunction extends SpatialPredicateFunction {
    @Override
    public String getName() {
      return "OVERLAPS";
    }

    @Override
    protected Predicate createInternal(
        final SpatialExpression expression1,
        final SpatialExpression expression2) {
      return expression1.overlaps(expression2);
    }
  }

  public static class TouchesFunction extends SpatialPredicateFunction {
    @Override
    public String getName() {
      return "TOUCHES";
    }

    @Override
    protected Predicate createInternal(
        final SpatialExpression expression1,
        final SpatialExpression expression2) {
      return expression1.touches(expression2);
    }
  }

  public static class WithinFunction extends SpatialPredicateFunction {
    @Override
    public String getName() {
      return "WITHIN";
    }

    @Override
    protected Predicate createInternal(
        final SpatialExpression expression1,
        final SpatialExpression expression2) {
      return expression1.within(expression2);
    }
  }

  public static class ContainsFunction extends SpatialPredicateFunction {
    @Override
    public String getName() {
      return "CONTAINS";
    }

    @Override
    protected Predicate createInternal(
        final SpatialExpression expression1,
        final SpatialExpression expression2) {
      return expression1.contains(expression2);
    }
  }

}
