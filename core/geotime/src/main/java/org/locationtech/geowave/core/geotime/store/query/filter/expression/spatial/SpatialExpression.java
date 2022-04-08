/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial;

import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.query.filter.expression.Expression;
import org.locationtech.geowave.core.store.query.filter.expression.Predicate;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * Interface for expressions that resolve to spatial geometry objects.
 */
public interface SpatialExpression extends Expression<FilterGeometry> {

  /**
   * Get the coordinate reference system for this expression. In cases where a field value geometry
   * is not indexed, the CRS will be derived from the field descriptor of the adapter.
   * 
   * @param adapter the adapter being filtered
   * @return the coordinate reference system of this expression
   */
  CoordinateReferenceSystem getCRS(final DataTypeAdapter<?> adapter);

  /**
   * Create a predicate that tests this expression against the provided bounding box.
   * 
   * @param minX the minimum X value
   * @param minY the minimum Y value
   * @param maxX the maximum X value
   * @param maxY the maximum Y value
   * @return the bounding box predicate
   */
  default Predicate bbox(
      final double minX,
      final double minY,
      final double maxX,
      final double maxY) {
    return new BBox(this, minX, minY, maxX, maxY, false);
  }

  /**
   * Create a predicate that tests this expression against the provided bounding box in the given
   * coordinate reference system.
   * 
   * @param minX the minimum X value
   * @param minY the minimum Y value
   * @param maxX the maximum X value
   * @param maxY the maximum Y value
   * @param crs the coordinate reference system of the bounding box
   * @return the bounding box predicate
   */
  default Predicate bbox(
      final double minX,
      final double minY,
      final double maxX,
      final double maxY,
      final CoordinateReferenceSystem crs) {
    return new BBox(this, minX, minY, maxX, maxY, crs, false);
  }

  /**
   * Create a predicate that loosely tests this expression against the provided bounding box. This
   * is meant to be a faster implementation for situations where exact accuracy is not needed.
   * 
   * @param minX the minimum X value
   * @param minY the minimum Y value
   * @param maxX the maximum X value
   * @param maxY the maximum Y value
   * @return the bounding box predicate
   */
  default Predicate bboxLoose(
      final double minX,
      final double minY,
      final double maxX,
      final double maxY) {
    return new BBox(this, minX, minY, maxX, maxY, true);
  }

  /**
   * Create a predicate that loosely tests this expression against the provided bounding box in the
   * given coordinate reference system. This is meant to be a faster implementation for situations
   * where exact accuracy is not needed.
   * 
   * @param minX the minimum X value
   * @param minY the minimum Y value
   * @param maxX the maximum X value
   * @param maxY the maximum Y value
   * @param crs the coordinate reference system of the bounding box
   * @return the bounding box predicate
   */
  default Predicate bboxLoose(
      final double minX,
      final double minY,
      final double maxX,
      final double maxY,
      final CoordinateReferenceSystem crs) {
    return new BBox(this, minX, minY, maxX, maxY, crs, true);
  }

  /**
   * Create a predicate that tests to see if this expression intersects the provided spatial object.
   * The operand can be either another spatial expression, or any object that can be converted to a
   * spatial literal.
   * 
   * @param other the spatial object to test against
   * @return the intersection predicate
   */
  default Predicate intersects(final Object other) {
    return new Intersects(this, toSpatialExpression(other), false);
  }

  /**
   * Create a predicate that tests to see if this expression intersects the provided spatial object.
   * This is meant to be a faster implementation for situations where accuracy is not needed. The
   * operand can be either another spatial expression, or any object that can be converted to a
   * spatial literal.
   * 
   * @param other the spatial object to test against
   * @return the intersection predicate
   */
  default Predicate intersectsLoose(final Object other) {
    return new Intersects(this, toSpatialExpression(other), true);
  }

  /**
   * Create a predicate that tests to see if this expression is disjoint to the provided spatial
   * object. The operand can be either another spatial expression, or any object that can be
   * converted to a spatial literal.
   * 
   * @param other the spatial object to test against
   * @return the disjoint predicate
   */
  default Predicate disjoint(final Object other) {
    return new Disjoint(this, toSpatialExpression(other), false);
  }

  /**
   * Create a predicate that tests to see if this expression is disjoint to the provided spatial
   * object. This is meant to be a faster implementation for situations where accuracy is not
   * needed. The operand can be either another spatial expression, or any object that can be
   * converted to a spatial literal.
   * 
   * @param other the spatial object to test against
   * @return the disjoint predicate
   */
  default Predicate disjointLoose(final Object other) {
    return new Disjoint(this, toSpatialExpression(other), true);
  }

  /**
   * Create a predicate that tests to see if this expression contains the provided spatial object.
   * The operand can be either another spatial expression, or any object that can be converted to a
   * spatial literal.
   * 
   * @param other the spatial object to test against
   * @return the contains predicate
   */
  default Predicate contains(final Object other) {
    return new SpatialContains(this, toSpatialExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression is within the provided spatial object.
   * The operand can be either another spatial expression, or any object that can be converted to a
   * spatial literal.
   * 
   * @param other the spatial object to test against
   * @return the within predicate
   */
  default Predicate within(final Object other) {
    return new Within(this, toSpatialExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression touches the provided spatial object.
   * The operand can be either another spatial expression, or any object that can be converted to a
   * spatial literal.
   * 
   * @param other the spatial object to test against
   * @return the touches predicate
   */
  default Predicate touches(final Object other) {
    return new Touches(this, toSpatialExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression crosses the provided spatial object.
   * The operand can be either another spatial expression, or any object that can be converted to a
   * spatial literal.
   * 
   * @param other the spatial object to test against
   * @return the crosses predicate
   */
  default Predicate crosses(final Object other) {
    return new Crosses(this, toSpatialExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression overlaps the provided spatial object.
   * The operand can be either another spatial expression, or any object that can be converted to a
   * spatial literal.
   * 
   * @param other the spatial object to test against
   * @return the overlaps predicate
   */
  default Predicate overlaps(final Object other) {
    return new Overlaps(this, toSpatialExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression is topologically equal to the provided
   * spatial object. The operand can be either another spatial expression, or any object that can be
   * converted to a spatial literal.
   * 
   * @param other the spatial object to test against
   * @return the equals predicate
   */
  @Override
  default Predicate isEqualTo(final Object other) {
    return new SpatialEqualTo(this, toSpatialExpression(other));
  }

  /**
   * Create a predicate that tests to see if this expression is not topologically equal to the
   * provided spatial object. The operand can be either another spatial expression, or any object
   * that can be converted to a spatial literal.
   * 
   * @param other the spatial object to test against
   * @return the not equals predicate
   */
  @Override
  default Predicate isNotEqualTo(final Object other) {
    return new SpatialNotEqualTo(this, toSpatialExpression(other));
  }

  /**
   * Convert the given object into a spatial expression, if it is not already one.
   * 
   * @param obj the object to convert
   * @return the spatial expression
   */
  public static SpatialExpression toSpatialExpression(final Object obj) {
    if (obj instanceof SpatialExpression) {
      return (SpatialExpression) obj;
    }
    return SpatialLiteral.of(obj);
  }
}
