/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic;

import java.util.LinkedList;
import java.util.List;
import javax.measure.Unit;
import javax.measure.quantity.Length;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.GeodeticCalculator;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.opengis.geometry.DirectPosition;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.units.indriya.unit.Units;

public class GeometryCalculations {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeometryCalculations.class);

  final GeometryFactory factory;
  final CoordinateReferenceSystem crs;
  final double xMin, yMin, xMax, yMax;

  public GeometryCalculations(final CoordinateReferenceSystem crs) {
    factory = new GeometryFactory(new PrecisionModel(), 4326);
    this.crs = crs;
    xMin = crs.getCoordinateSystem().getAxis(0).getMinimumValue();
    xMax = crs.getCoordinateSystem().getAxis(0).getMaximumValue();
    yMin = crs.getCoordinateSystem().getAxis(1).getMinimumValue();
    yMax = crs.getCoordinateSystem().getAxis(1).getMaximumValue();
  }

  /**
   * Build geometries with the provided coordinate at the center. The width of the geometry is twice
   * the distance provided. More than one geometry is return when passing the date line.
   *
   * @param distances [x,y] = [longitude, latitude]
   * @param unit
   * @param coordinate
   * @return the geometries that were built
   */
  public List<Geometry> buildSurroundingGeometries(
      final double[] distances,
      final Unit<Length> unit,
      final Coordinate coordinate) {
    final List<Geometry> geos = new LinkedList<>();
    final GeodeticCalculator geoCalc = new GeodeticCalculator();
    geoCalc.setStartingGeographicPoint(coordinate.x, coordinate.y);
    try {
      geoCalc.setDirection(0, unit.getConverterTo(Units.METRE).convert(distances[1]));
      final DirectPosition north = geoCalc.getDestinationPosition();
      geoCalc.setDirection(90, unit.getConverterTo(Units.METRE).convert(distances[0]));
      final DirectPosition east = geoCalc.getDestinationPosition();
      geoCalc.setStartingGeographicPoint(coordinate.x, coordinate.y);
      geoCalc.setDirection(-90, unit.getConverterTo(Units.METRE).convert(distances[0]));
      final DirectPosition west = geoCalc.getDestinationPosition();
      geoCalc.setDirection(180, unit.getConverterTo(Units.METRE).convert(distances[1]));
      final DirectPosition south = geoCalc.getDestinationPosition();

      final double x1 = west.getOrdinate(0);
      final double x2 = east.getOrdinate(0);
      final double y1 = north.getOrdinate(1);
      final double y2 = south.getOrdinate(1);

      handleBoundaries(geos, coordinate, x1, x2, y1, y2);
      return geos;
    } catch (final TransformException ex) {
      LOGGER.error("Unable to build geometry", ex);
    }

    return null;
  }

  private void handleBoundaries(
      final List<Geometry> geos,
      final Coordinate coordinate,
      final double x1,
      final double x2,
      final double y1,
      final double y2) {

    if (Math.signum(x1) > Math.signum(coordinate.x)) {
      ReferencedEnvelope bounds =
          new ReferencedEnvelope(x1, xMax, Math.max(y1, yMin), Math.min(y2, yMax), crs);
      geos.add(factory.toGeometry(bounds));
      bounds = new ReferencedEnvelope(xMin, x2, Math.max(y1, yMin), Math.min(y2, yMax), crs);
      geos.add(factory.toGeometry(bounds));
    } else if (Math.signum(x2) < Math.signum(coordinate.x)) {
      ReferencedEnvelope bounds =
          new ReferencedEnvelope(xMin, x2, Math.max(y1, yMin), Math.min(y2, yMax), crs);
      geos.add(factory.toGeometry(bounds));
      bounds = new ReferencedEnvelope(x1, xMax, Math.max(y1, yMin), Math.min(y2, yMax), crs);
      geos.add(factory.toGeometry(bounds));
    } else {
      final ReferencedEnvelope bounds =
          new ReferencedEnvelope(x1, x2, Math.max(y1, yMin), Math.min(y2, yMax), crs);
      geos.add(factory.toGeometry(bounds));
    }
  }
}
