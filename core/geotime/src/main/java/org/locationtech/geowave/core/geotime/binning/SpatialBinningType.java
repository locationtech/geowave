/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.binning;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.jts.geom.Geometry;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

public enum SpatialBinningType implements SpatialBinningHelper {
  H3(new H3BinningHelper()), S2(new S2BinningHelper()), GEOHASH(new GeohashBinningHelper());

  private SpatialBinningHelper helperDelegate;

  public static int WGS84_SRID = 4326;
  public static String WGS84_SRID_EPSG = "EPSG:4326";

  private SpatialBinningType(final SpatialBinningHelper helperDelegate) {
    this.helperDelegate = helperDelegate;
  }

  /**
   * Converts a JTS geometry to WGS84 CRS.
   * 
   * @param geometry {Geometry} The input geometry to be processed.
   * @return {Geometry} Returns the JTS geometry in WGS84 CRS.
   */
  public static Geometry convertToWGS84(Geometry geometry) {

    // Get the source CRS from the user data that is set in OptimizedSimpleFeatureBuilder.java
    CoordinateReferenceSystem sourceCRS = (CoordinateReferenceSystem) geometry.getUserData();

    MathTransform transform;
    Geometry targetGeometry = null;

    if (sourceCRS != null) {
      // Only proceed if CRS is not WGS84
      Boolean isWGS84 = sourceCRS.getName().getCode().equals("WGS 84");

      if (!isWGS84) {
        try {
          // Decode the target CRS of "EPSG:4326"
          CoordinateReferenceSystem targetCRS = CRS.decode(WGS84_SRID_EPSG);

          // Get the transform from source CRS to target CRS with leniency
          transform = CRS.findMathTransform(sourceCRS, targetCRS, true);
          try {
            // Transform the JTS geometry
            targetGeometry = JTS.transform(geometry, transform);

            // Set the SRID, although this is not necessary
            targetGeometry.setSRID(WGS84_SRID);
          } catch (MismatchedDimensionException | TransformException e) {
            e.printStackTrace();
          }
        } catch (FactoryException e) {
          e.printStackTrace();
        }
      }
    }

    return targetGeometry != null ? targetGeometry : geometry;
  }

  /**
   * Gets the spatial bins. Note: Spatial binning aggregations call this (runs on each individual
   * SimpleFeature).
   * 
   * @param geometry {Geometry} The input geometry to be processed.
   * @param precision {Integer} The spatial binning precision.
   * @return {ByteArray[]} Returns a ByteArray of spatial bins.
   */
  @Override
  public ByteArray[] getSpatialBins(final Geometry geometry, final int precision) {
    Geometry targetGeometry = convertToWGS84(geometry);

    return helperDelegate.getSpatialBins(targetGeometry, precision);
  }

  /**
   * Gets the geometry constraints. Note: Spatial binning statistics call this - runs once on whole
   * extent.
   * 
   * @param geom {Geometry} The input geometry to be processed.
   * @param precision {Integer} The spatial binning precision.
   * @return {ByteArrayConstraints} Returns a ByteArrayConstraints of geometry constraints.
   */
  @Override
  public ByteArrayConstraints getGeometryConstraints(final Geometry geom, final int precision) {

    Geometry targetGeometry = convertToWGS84(geom);

    return helperDelegate.getGeometryConstraints(targetGeometry, precision);
  }


  @Override
  public Geometry getBinGeometry(final ByteArray bin, final int precision) {
    return helperDelegate.getBinGeometry(bin, precision);
  }

  @Override
  public String binToString(final byte[] binId) {
    return helperDelegate.binToString(binId);
  }

  @Override
  public int getBinByteLength(final int precision) {
    return helperDelegate.getBinByteLength(precision);
  }

  // is used by python converter
  public static SpatialBinningType fromString(final String code) {

    for (final SpatialBinningType output : SpatialBinningType.values()) {
      if (output.toString().equalsIgnoreCase(code)) {
        return output;
      }
    }

    return null;
  }
}
