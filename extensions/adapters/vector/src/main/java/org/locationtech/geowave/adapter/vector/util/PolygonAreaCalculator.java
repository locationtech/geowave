/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.util;

import java.util.HashMap;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.jts.densify.Densifier;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

public class PolygonAreaCalculator {
  private static final double DEFAULT_DENSIFY_VERTEX_COUNT = 1000.0;
  private static final double SQM_2_SQKM = 1.0 / 1000000.0;
  private double densifyVertexCount = DEFAULT_DENSIFY_VERTEX_COUNT;

  private final HashMap<String, CoordinateReferenceSystem> crsMap = new HashMap<>();

  public PolygonAreaCalculator() {}

  private CoordinateReferenceSystem lookupUtmCrs(final double centerLat, final double centerLon)
      throws NoSuchAuthorityCodeException, FactoryException {
    final int epsgCode =
        (32700 - (Math.round((45f + (float) centerLat) / 90f) * 100))
            + Math.round((183f + (float) centerLon) / 6f);

    final String crsId = "EPSG:" + Integer.toString(epsgCode);

    CoordinateReferenceSystem crs = crsMap.get(crsId);

    if (crs == null) {
      crs = CRS.decode(crsId, true);

      crsMap.put(crsId, crs);
    }

    return crs;
  }

  public double getAreaSimple(final Geometry polygon) throws Exception {
    final Point centroid = polygon.getCentroid();
    final CoordinateReferenceSystem equalAreaCRS = lookupUtmCrs(centroid.getY(), centroid.getX());

    final MathTransform transform =
        CRS.findMathTransform(DefaultGeographicCRS.WGS84, equalAreaCRS, true);

    final Geometry transformedPolygon = JTS.transform(polygon, transform);

    return transformedPolygon.getArea() * SQM_2_SQKM;
  }

  public double getAreaDensify(final Geometry polygon) throws Exception {
    final Point centroid = polygon.getCentroid();
    final CoordinateReferenceSystem equalAreaCRS = lookupUtmCrs(centroid.getY(), centroid.getX());

    final double vertexSpacing = polygon.getLength() / densifyVertexCount;
    final Geometry densePolygon = Densifier.densify(polygon, vertexSpacing);

    final MathTransform transform =
        CRS.findMathTransform(DefaultGeographicCRS.WGS84, equalAreaCRS, true);

    final Geometry transformedPolygon = JTS.transform(densePolygon, transform);

    return transformedPolygon.getArea() * SQM_2_SQKM;
  }

  public void setDensifyVertexCount(final double densifyVertexCount) {
    this.densifyVertexCount = densifyVertexCount;
  }
}
