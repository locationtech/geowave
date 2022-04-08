/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial;

import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.jts.geom.Geometry;

/**
 * A {@link FilterGeometry} implementation for unprepared geometries. It attempts to optimize the
 * spatial operation by utilizing the other operand if it is a prepared geometry.
 */
public class UnpreparedFilterGeometry implements FilterGeometry {

  private Geometry geometry;

  public UnpreparedFilterGeometry() {}

  public UnpreparedFilterGeometry(final Geometry geometry) {
    this.geometry = geometry;
  }

  @Override
  public Geometry getGeometry() {
    return geometry;
  }

  @Override
  public boolean intersects(final FilterGeometry other) {
    if (other instanceof PreparedFilterGeometry) {
      return other.intersects(this);
    }
    return geometry.intersects(other.getGeometry());
  }

  @Override
  public boolean disjoint(final FilterGeometry other) {
    if (other instanceof PreparedFilterGeometry) {
      return other.disjoint(this);
    }
    return geometry.disjoint(other.getGeometry());
  }

  @Override
  public boolean crosses(final FilterGeometry other) {
    if (other instanceof PreparedFilterGeometry) {
      return other.crosses(this);
    }
    return geometry.crosses(other.getGeometry());
  }

  @Override
  public boolean overlaps(final FilterGeometry other) {
    if (other instanceof PreparedFilterGeometry) {
      return other.overlaps(this);
    }
    return geometry.overlaps(other.getGeometry());
  }

  @Override
  public boolean touches(final FilterGeometry other) {
    if (other instanceof PreparedFilterGeometry) {
      return other.touches(this);
    }
    return geometry.touches(other.getGeometry());
  }

  @Override
  public boolean within(final FilterGeometry other) {
    if (other instanceof PreparedFilterGeometry) {
      // contains is the inverse of within
      return other.contains(this);
    }
    return geometry.within(other.getGeometry());
  }

  @Override
  public boolean contains(final FilterGeometry other) {
    if (other instanceof PreparedFilterGeometry) {
      // within is the inverse of contains
      return other.within(this);
    }
    return geometry.contains(other.getGeometry());
  }

  @Override
  public boolean isEqualTo(final FilterGeometry other) {
    return geometry.equalsTopo(other.getGeometry());
  }

  @Override
  public byte[] toBinary() {
    return GeometryUtils.geometryToBinary(geometry, null);
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    geometry = GeometryUtils.geometryFromBinary(bytes, null);
  }

}
