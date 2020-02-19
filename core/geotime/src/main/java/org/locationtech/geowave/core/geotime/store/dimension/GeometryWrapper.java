/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.dimension;

import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import com.google.common.math.DoubleMath;

/**
 * This class wraps JTS geometry with visibility so that it can be used within GeoWave as a
 * CommonIndexValue
 */
public class GeometryWrapper implements CommonIndexValue {
  public static final String DEFAULT_GEOMETRY_FIELD_NAME = "default_geom_dimension";
  private byte[] visibility;
  private final org.locationtech.jts.geom.Geometry geometry;
  private static final double DOUBLE_TOLERANCE = 1E-12d;

  public GeometryWrapper(final org.locationtech.jts.geom.Geometry geometry) {
    this.geometry = geometry;
  }

  public GeometryWrapper(
      final org.locationtech.jts.geom.Geometry geometry,
      final byte[] visibility) {
    this.visibility = visibility;
    this.geometry = geometry;
  }

  @Override
  public void setVisibility(final byte[] visibility) {
    this.visibility = visibility;
  }

  @Override
  public byte[] getVisibility() {
    return visibility;
  }

  public org.locationtech.jts.geom.Geometry getGeometry() {
    return geometry;
  }

  /** Expects Longitude before Latitude */
  @Override
  public boolean overlaps(final NumericDimensionField[] fields, final NumericData[] rangeData) {

    final int latPosition = fields[0] instanceof LatitudeField ? 0 : 1;
    final int longPosition = fields[0] instanceof LatitudeField ? 1 : 0;
    if (fields.length == 1) {
      final Envelope env = geometry.getEnvelopeInternal();
      final NumericRange r =
          latPosition == 0 ? new NumericRange(env.getMinY(), env.getMaxY())
              : new NumericRange(env.getMinX(), env.getMaxX());
      return ((rangeData[0].getMin() < r.getMax())
          || DoubleMath.fuzzyEquals(rangeData[0].getMin(), r.getMax(), DOUBLE_TOLERANCE))
          && ((rangeData[0].getMax() > r.getMin())
              || DoubleMath.fuzzyEquals(rangeData[0].getMax(), r.getMin(), DOUBLE_TOLERANCE));
    }
    return geometry.getFactory().createPolygon(
        new Coordinate[] {
            new Coordinate(
                rangeData[longPosition].getMin() - DOUBLE_TOLERANCE,
                rangeData[latPosition].getMin() - DOUBLE_TOLERANCE),
            new Coordinate(
                rangeData[longPosition].getMin() - DOUBLE_TOLERANCE,
                rangeData[latPosition].getMax() + DOUBLE_TOLERANCE),
            new Coordinate(
                rangeData[longPosition].getMax() + DOUBLE_TOLERANCE,
                rangeData[latPosition].getMax() + DOUBLE_TOLERANCE),
            new Coordinate(
                rangeData[longPosition].getMax() + DOUBLE_TOLERANCE,
                rangeData[latPosition].getMin() - DOUBLE_TOLERANCE),
            new Coordinate(
                rangeData[longPosition].getMin() - DOUBLE_TOLERANCE,
                rangeData[latPosition].getMin() - DOUBLE_TOLERANCE)}).intersects(geometry);
  }
}
