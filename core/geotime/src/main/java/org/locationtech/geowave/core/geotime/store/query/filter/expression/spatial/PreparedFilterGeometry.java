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
import org.locationtech.jts.geom.prep.PreparedGeometry;

/**
 * A {@link FilterGeometry} implementation for prepared geometries.
 */
public class PreparedFilterGeometry implements FilterGeometry {

  private PreparedGeometry geometry;

  public PreparedFilterGeometry() {}

  public PreparedFilterGeometry(final PreparedGeometry geometry) {
    this.geometry = geometry;
  }

  @Override
  public Geometry getGeometry() {
    return geometry.getGeometry();
  }

  @Override
  public boolean intersects(final FilterGeometry other) {
    return geometry.intersects(other.getGeometry());
  }

  @Override
  public boolean disjoint(final FilterGeometry other) {
    return geometry.disjoint(other.getGeometry());
  }

  @Override
  public boolean crosses(final FilterGeometry other) {
    return geometry.crosses(other.getGeometry());
  }

  @Override
  public boolean overlaps(final FilterGeometry other) {
    return geometry.overlaps(other.getGeometry());
  }

  @Override
  public boolean touches(final FilterGeometry other) {
    return geometry.touches(other.getGeometry());
  }

  @Override
  public boolean within(final FilterGeometry other) {
    return geometry.within(other.getGeometry());
  }

  @Override
  public boolean contains(final FilterGeometry other) {
    return geometry.contains(other.getGeometry());
  }

  @Override
  public boolean isEqualTo(final FilterGeometry other) {
    return geometry.getGeometry().equalsTopo(other.getGeometry());
  }

  @Override
  public byte[] toBinary() {
    return GeometryUtils.geometryToBinary(getGeometry(), null);
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final Geometry unprepared = GeometryUtils.geometryFromBinary(bytes, null);
    geometry = GeometryUtils.PREPARED_GEOMETRY_FACTORY.create(unprepared);
  }

}
