/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin.heatmap;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTS;
import org.geotools.measure.Measure;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.TDigestNumericHistogram;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.query.aggregate.FieldNameParam;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.geo.LatLong;
import si.uom.SI;
import org.locationtech.geowave.adapter.vector.plugin.GeoWaveDataStoreComponents;
import org.locationtech.geowave.adapter.vector.plugin.heatmap.HeatMapAggregations;
import org.geotools.measure.Measure;

/**
 * Utility methods to support HeatMap queries.
 * 
 * @author M. Zagorski <br>
 * @apiNote Date: 3-25-2022 <br>
 *
 * @apiNote Changelog: <br>
 * 
 */
// public class HeatMapUtils implements Aggregation<FieldNameParam, Long, SimpleFeature> {
public class HeatMapUtils {

  public static int SQ_KM_CONV = 1000 * 1000;

  /**
   * Builds a simple feature.
   * 
   * @param featureType {SimpleFeatureType} The feature type of the simple feature.
   * @param geohashId {ByteArray} The geohash grid cell ID.
   * @param value {Double} The value calculated by the aggregation or statistics query.
   * @param precision {Integer} The Geohash precision level (1-12).
   * @param weightAttr {String} The target data field name.
   * @param source {String} The code that indicates the type of query.
   * @return {SimpleFeature} Returns a SimpleFeature containing the query value and relevant
   *         information.
   */
  public static SimpleFeature buildSimpleFeature(
      // final TDigestNumericHistogram histogram,
      final SimpleFeatureType featureType,
      final ByteArray geohashId,
      final Double value,
      final Integer precision,
      final String weightAttr,
      final String source) {

    // Get the coordinate reference system
    CoordinateReferenceSystem oldCRS = featureType.getCoordinateReferenceSystem();
    String oldName = featureType.getTypeName();

    // Convert the value to a double
    double valDbl = value.doubleValue();

    // Get the histogram-weighted value
    // valDbl = histogram.cdf(valDbl);

    // Convert GeoHash ID to string
    String geoHashIdStr = geohashId.getString();

    // Get centroid of GeoHash cell
    final LatLong ll = GeoHash.decodeHash(geohashId.getString());
    Geometry centroid =
        GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(ll.getLon(), ll.getLat()));

    // Initialize new SimpleFeatureTypeBuilder
    final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();

    // Set Name and CRS
    typeBuilder.setName(oldName);
    typeBuilder.setCRS(oldCRS);

    // Add keys to the typeBuilder
    typeBuilder.add("the_geom", Geometry.class);
    typeBuilder.add("field_name", String.class);
    typeBuilder.add(weightAttr, Double.class);
    typeBuilder.add("geohashId", String.class);
    typeBuilder.add("source", String.class);
    typeBuilder.add("geohashPrec", Integer.class);

    // Build the new type
    SimpleFeatureType newType = typeBuilder.buildFeatureType();

    // Initialize the new SimpleFeatureBuilder using the new type
    final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(newType);

    // Set values
    builder.set("the_geom", centroid);
    builder.set("field_name", weightAttr);
    builder.set(weightAttr, valDbl);
    builder.set("geohashId", geoHashIdStr);
    builder.set("source", source);
    builder.set("geohashPrec", precision);

    return builder.buildFeature(geoHashIdStr);
  }


  /**
   * Get an appropriate Geohash precision based on the approximate area (square kilometers) of a
   * grid cell.
   * 
   * @param cellArea {double} The area (square kilometers) of the grid cell (from the GeoServer
   *        mapping extent).
   * @return Returns an integer for the Geohash precision (1-12).
   */
  public static int getGeohashPrecision(double cellArea) {
    if (cellArea >= 10000000)
      return 1;
    if (cellArea >= 500000)
      return 2;
    if (cellArea >= 15000)
      return 3;
    if (cellArea >= 500)
      return 4;
    if (cellArea >= 15)
      return 5;
    if (cellArea >= 1)
      return 6;
    if (cellArea >= 0.01)
      return 7;
    if (cellArea >= 0.0005)
      return 8;
    if (cellArea >= 0.00002)
      return 9;
    if (cellArea >= 0.00005)
      return 10;
    if (cellArea >= 0.00000002)
      return 11;
    if (cellArea >= 0)
      return 12;
    return 4;
  }


  /**
   * Calculate the area of a geometry in square kilometers.
   * 
   * @param geom {Geometry} The input geometry to be processed.
   * @return {double} Returns a double representing the area of the input geometry.
   */
  public static double calcAreaSqKm(Geometry geom) {

    double geomArea = 0;

    // Get centroid of geometry
    Point centroid = geom.getCentroid();

    // Get the location
    String code = "AUTO:42001," + centroid.getX() + "," + centroid.getY();
    CoordinateReferenceSystem crs;

    try {
      // Decode the location to get the CRS
      crs = CRS.decode(code);

      // Get the transform
      MathTransform transform = CRS.findMathTransform(DefaultGeographicCRS.WGS84, crs);

      // Project the geometry using the transform
      Geometry geomProj = JTS.transform(geom, transform);

      // Calculate the area (square kilometers) based on the projected geometry
      geomArea = geomProj.getArea() / SQ_KM_CONV;

    } catch (FactoryException e) {
      e.printStackTrace();
    } catch (MismatchedDimensionException e) {
      e.printStackTrace();
    } catch (TransformException e) {
      e.printStackTrace();
    }
    return geomArea;
  }


  /**
   * Returns the cell count of the GeoServer map viewer extent.
   * 
   * @param width {Integer} The width of the GeoServer map viewer extent.
   * @param height {Integer} The height of the GeoServer map viewer extent.
   * @param pixelsPerCell {Integer} The count of pixels per cell.
   * @return {Integer} Returns an integer representing the cell count in the GeoServer map viewer
   *         extent.
   */
  public static int getExtentCellCount(int width, int height, int pixelsPerCell) {

    // Get the count of grid cells for the width and height of the extent
    int cntCellsWidth = width / pixelsPerCell;
    int cntCellsHeight = height / pixelsPerCell;

    // Get the total count of grid cells in the extent
    int extentCellCount = cntCellsWidth * cntCellsHeight;

    return extentCellCount;
  }


  /**
   * Returns the approximate area of a single cell in the GeoServer map viewer extent.
   * 
   * @param extentAreaSqKm {Double} The area of the GeoServer map viewer extent in square
   *        kilometers.
   * @param totCellsTarget {Integer} The total count of cells in the GeoServer map viewer extent.
   * @return {Double} Returns a double representing the approximate area of each cell in the
   *         GeoServer map viewer extent.
   */
  public static double getCellArea(double extentAreaSqKm, int totCellsTarget) {
    return extentAreaSqKm / totCellsTarget;
  }


  /**
   * Automatic selection of an appropriate Geohash precision.
   * 
   * @param height {Integer} The height of the GeoServer map viewer extent.
   * @param width {Integer} The width of the GeoServer map viewer extent.
   * @param pixelsPerCell {Integer} The number of pixels per GeoServer map viewer cell.
   * @param jtsBounds {Geometry} The geometry that represents the GeoServer map viewer extent.
   * @return {Integer} Returns an integer representing an appropriate Geohash precision.
   */
  public static int autoSelectGeohashPrecision(
      int height,
      int width,
      int pixelsPerCell,
      Geometry jtsBounds) {

    // Get total count of cells in GeoServer map viewer extent
    int totCellsTarget = HeatMapUtils.getExtentCellCount(width, height, pixelsPerCell);

    // Get the area of the GeoServer map viewer extent in square kilometers
    double extentAreaSqKm = HeatMapUtils.calcAreaSqKm(jtsBounds);

    // Get approximate area of a single cell in square kilometers
    double cellArea = HeatMapUtils.getCellArea(extentAreaSqKm, totCellsTarget);

    // Get the most appropriate Geohash precision (e.g. 1-12) based on the cell area
    int geohashPrec = HeatMapUtils.getGeohashPrecision(cellArea);

    return geohashPrec;
  }


  /**
   * Get the field name of the geometry column from the input data.
   * 
   * @param components {GeoWaveDataStoreComponents} The base components of the data.
   * @return {String} Returns a string representing the field name of the geometry column from the
   *         input data.
   */
  public static String getGeometryFieldName(GeoWaveDataStoreComponents components) {
    return components.getFeatureType().getGeometryDescriptor().getLocalName();
  }

}
