/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.binning;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import com.google.common.collect.ImmutableMap;

public class SpatialBinningTypeTest {
  private final static Map<SpatialBinningType, Double> TYPE_TO_ERROR_THRESHOLD =
      ImmutableMap.of(
          SpatialBinningType.GEOHASH,
          1E-14,
          SpatialBinningType.S2,
          0.01,
          SpatialBinningType.H3,
          // H3 approximations can just be *bad*
          0.25);

  @Test
  public void testPolygons() {
    testGeometry(
        GeometryUtils.GEOMETRY_FACTORY.createPolygon(
            new Coordinate[] {
                new Coordinate(33, 33),
                new Coordinate(34, 34),
                new Coordinate(33, 34),
                new Coordinate(33, 33)}),
        Geometry::getArea);
    testGeometry(
        GeometryUtils.GEOMETRY_FACTORY.createPolygon(
            new Coordinate[] {
                new Coordinate(0.5, 0.6),
                new Coordinate(0.7, 0.8),
                new Coordinate(1, 0.9),
                new Coordinate(0.8, 0.7),
                new Coordinate(0.5, 0.6)}),
        Geometry::getArea);
    testGeometry(
        GeometryUtils.GEOMETRY_FACTORY.createPolygon(
            GeometryUtils.GEOMETRY_FACTORY.createLinearRing(
                new Coordinate[] {
                    new Coordinate(33, 33),
                    new Coordinate(33, 34),
                    new Coordinate(34, 34),
                    new Coordinate(34, 33),
                    new Coordinate(33, 33)}),
            new LinearRing[] {
                GeometryUtils.GEOMETRY_FACTORY.createLinearRing(
                    new Coordinate[] {
                        new Coordinate(33.25, 33.25),
                        new Coordinate(33.75, 33.25),
                        new Coordinate(33.75, 33.75),
                        new Coordinate(33.25, 33.75),
                        new Coordinate(33.25, 33.25)})}),
        Geometry::getArea);
  }

  @Test
  public void testLines() {
    testGeometry(
        GeometryUtils.GEOMETRY_FACTORY.createLineString(
            new Coordinate[] {new Coordinate(33, 33), new Coordinate(34, 34)}),
        Geometry::getLength);
    testGeometry(
        GeometryUtils.GEOMETRY_FACTORY.createLineString(
            new Coordinate[] {
                new Coordinate(33, 33),
                new Coordinate(33, 34),
                new Coordinate(34, 34),
                new Coordinate(34, 33),
                new Coordinate(33, 33)}),
        Geometry::getLength);
    testGeometry(
        GeometryUtils.GEOMETRY_FACTORY.createLineString(
            new Coordinate[] {
                new Coordinate(0.5, 0.6),
                new Coordinate(0.7, 0.8),
                new Coordinate(1, 0.9),
                new Coordinate(0.8, 0.7),
                new Coordinate(0.5, 0.6)}),
        Geometry::getLength);
  }

  private void testGeometry(
      final Geometry geom,
      final Function<Geometry, Double> measurementFunction) {
    final double originalMeasurement = measurementFunction.apply(geom);
    for (final SpatialBinningType type : SpatialBinningType.values()) {
      final double errorThreshold = TYPE_TO_ERROR_THRESHOLD.get(type);
      for (int precision = 1; precision < 7; precision++) {
        final int finalPrecision = type.equals(SpatialBinningType.S2) ? precision * 2 : precision;
        final ByteArray[] bins = type.getSpatialBins(geom, finalPrecision);
        double weight = 0;
        final List<Geometry> cellGeoms = new ArrayList<>();
        for (final ByteArray bin : bins) {
          final Geometry binGeom = type.getBinGeometry(bin, finalPrecision);
          cellGeoms.add(binGeom.intersection(geom));

          final double intersectionMeasurement =
              measurementFunction.apply(binGeom.intersection(geom));
          final double fieldWeight = intersectionMeasurement / originalMeasurement;
          weight += fieldWeight;
        }
        // cumulative weight should be 1, within the threshold of error
        Assert.assertEquals(
            String.format(
                "Combined weight is off by more than threshold for type '%s' with precision '%d' for geometry '%s'",
                type,
                finalPrecision,
                geom),
            1,
            weight,
            errorThreshold);
        // the union of the geometries should be within the within the threshold of error of the
        // original measurement
        Assert.assertEquals(
            String.format(
                "Measurement on geometric union is off by more than threshold for type '%s' with precision '%d' for geometry '%s'",
                type,
                finalPrecision,
                geom),
            1,
            measurementFunction.apply(GeometryUtils.GEOMETRY_FACTORY.buildGeometry(cellGeoms))
                / originalMeasurement,
            errorThreshold);
      }
    }
  }
}
