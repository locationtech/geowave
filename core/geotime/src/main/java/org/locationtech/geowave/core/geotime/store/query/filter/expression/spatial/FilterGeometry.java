/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.jts.geom.Geometry;

/**
 * Interface for geometries within filter expressions. This is primarily to avoid having to check
 * for prepared vs non-prepared geometries throughout the expression implementations.
 */
public interface FilterGeometry extends Persistable {

  /**
   * @return the raw geometry
   */
  public Geometry getGeometry();

  /**
   * Check to see if this geometry intersects the provided geometry.
   * 
   * @param other the geometry to test against
   * @return {@code true} if the geometries intersect
   */
  boolean intersects(FilterGeometry other);

  /**
   * Check to see if this geometry is disjoint from the provided geometry.
   * 
   * @param other the geometry to test against
   * @return {@code true} if the geometries are disjoint
   */
  boolean disjoint(FilterGeometry other);

  /**
   * Check to see if this geometry crosses the provided geometry.
   * 
   * @param other the geometry to test against
   * @return {@code true} if this geometry crosses the provided geometry
   */
  boolean crosses(FilterGeometry other);

  /**
   * Check to see if this geometry overlaps the provided geometry.
   * 
   * @param other the geometry to test against
   * @return {@code true} if the geometries overlap
   */
  boolean overlaps(FilterGeometry other);

  /**
   * Check to see if this geometry touches the provided geometry.
   * 
   * @param other the geometry to test against
   * @return {@code true} if the geometries touch
   */
  boolean touches(FilterGeometry other);

  /**
   * Check to see if this geometry is within the provided geometry.
   * 
   * @param other the geomtery to test against
   * @return {@code true} if this geometry is within the provided geometry
   */
  boolean within(FilterGeometry other);

  /**
   * Check to see if this geometry contains the provided geometry.
   * 
   * @param other the geomtery to test against
   * @return {@code true} if this geometry contains the provided geometry
   */
  boolean contains(FilterGeometry other);

  /**
   * Check to see if this geometry is topologically equal to the provided geometry.
   * 
   * @param other the geomtery to test against
   * @return {@code true} if this geometry is topologically equal to the provided geometry
   */
  boolean isEqualTo(FilterGeometry other);
}
