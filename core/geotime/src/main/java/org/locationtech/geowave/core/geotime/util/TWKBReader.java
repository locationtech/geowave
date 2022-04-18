/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;

public class TWKBReader {
  public TWKBReader() {}

  public Geometry read(final byte[] bytes) throws ParseException {
    return read(ByteBuffer.wrap(bytes));
  }

  public Geometry read(final ByteBuffer input) throws ParseException {
    try {
      final byte typeAndPrecision = input.get();
      final byte type = (byte) (typeAndPrecision & 0x0F);
      final int basePrecision = TWKBUtils.zigZagDecode((typeAndPrecision & 0xF0) >> 4);
      final byte metadata = input.get();
      PrecisionReader precision;
      if ((metadata & TWKBUtils.EXTENDED_DIMENSIONS) != 0) {
        final byte extendedDimensions = input.get();
        precision = new ExtendedPrecisionReader(basePrecision, extendedDimensions);
      } else {
        precision = new PrecisionReader(basePrecision);
      }
      switch (type) {
        case TWKBUtils.POINT_TYPE:
          return readPoint(precision, metadata, input);
        case TWKBUtils.LINESTRING_TYPE:
          return readLineString(precision, metadata, input);
        case TWKBUtils.POLYGON_TYPE:
          return readPolygon(precision, metadata, input);
        case TWKBUtils.MULTIPOINT_TYPE:
          return readMultiPoint(precision, metadata, input);
        case TWKBUtils.MULTILINESTRING_TYPE:
          return readMultiLineString(precision, metadata, input);
        case TWKBUtils.MULTIPOLYGON_TYPE:
          return readMultiPolygon(precision, metadata, input);
        case TWKBUtils.GEOMETRYCOLLECTION_TYPE:
          return readGeometryCollection(input, metadata);
      }
      return null;
    } catch (final IOException e) {
      throw new ParseException("Error reading TWKB geometry.", e);
    }
  }

  private Point readPoint(
      final PrecisionReader precision,
      final byte metadata,
      final ByteBuffer input) throws IOException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createPoint();
    }

    final Coordinate coordinate = precision.readPoint(input);
    return GeometryUtils.GEOMETRY_FACTORY.createPoint(coordinate);
  }

  private LineString readLineString(
      final PrecisionReader precision,
      final byte metadata,
      final ByteBuffer input) throws IOException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createLineString();
    }

    final Coordinate[] coordinates = precision.readPointArray(input);
    return GeometryUtils.GEOMETRY_FACTORY.createLineString(coordinates);
  }

  private Polygon readPolygon(
      final PrecisionReader precision,
      final byte metadata,
      final ByteBuffer input) throws IOException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createPolygon();
    }
    final int numRings = VarintUtils.readUnsignedInt(input);
    final LinearRing exteriorRing =
        GeometryUtils.GEOMETRY_FACTORY.createLinearRing(precision.readPointArray(input));
    final LinearRing[] interiorRings = new LinearRing[numRings - 1];
    for (int i = 0; i < (numRings - 1); i++) {
      interiorRings[i] =
          GeometryUtils.GEOMETRY_FACTORY.createLinearRing(precision.readPointArray(input));
    }
    return GeometryUtils.GEOMETRY_FACTORY.createPolygon(exteriorRing, interiorRings);
  }

  private MultiPoint readMultiPoint(
      final PrecisionReader precision,
      final byte metadata,
      final ByteBuffer input) throws IOException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createMultiPoint();
    }
    final Coordinate[] points = precision.readPointArray(input);
    return GeometryUtils.GEOMETRY_FACTORY.createMultiPointFromCoords(points);
  }

  private MultiLineString readMultiLineString(
      final PrecisionReader precision,
      final byte metadata,
      final ByteBuffer input) throws IOException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createMultiLineString();
    }
    final int numLines = VarintUtils.readUnsignedInt(input);
    final LineString[] lines = new LineString[numLines];
    for (int i = 0; i < numLines; i++) {
      lines[i] = GeometryUtils.GEOMETRY_FACTORY.createLineString(precision.readPointArray(input));
    }
    return GeometryUtils.GEOMETRY_FACTORY.createMultiLineString(lines);
  }

  private MultiPolygon readMultiPolygon(
      final PrecisionReader precision,
      final byte metadata,
      final ByteBuffer input) throws IOException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createMultiPolygon();
    }
    final int numPolygons = VarintUtils.readUnsignedInt(input);
    final Polygon[] polygons = new Polygon[numPolygons];
    int numRings;
    for (int i = 0; i < numPolygons; i++) {
      numRings = VarintUtils.readUnsignedInt(input);
      if (numRings == 0) {
        polygons[i] = GeometryUtils.GEOMETRY_FACTORY.createPolygon();
        continue;
      }
      final LinearRing exteriorRing =
          GeometryUtils.GEOMETRY_FACTORY.createLinearRing(precision.readPointArray(input));
      final LinearRing[] interiorRings = new LinearRing[numRings - 1];
      for (int j = 0; j < (numRings - 1); j++) {
        interiorRings[j] =
            GeometryUtils.GEOMETRY_FACTORY.createLinearRing(precision.readPointArray(input));
      }
      polygons[i] = GeometryUtils.GEOMETRY_FACTORY.createPolygon(exteriorRing, interiorRings);
    }
    return GeometryUtils.GEOMETRY_FACTORY.createMultiPolygon(polygons);
  }

  private GeometryCollection readGeometryCollection(final ByteBuffer input, final byte metadata)
      throws ParseException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createGeometryCollection();
    }
    final int numGeometries = VarintUtils.readUnsignedInt(input);
    final Geometry[] geometries = new Geometry[numGeometries];
    for (int i = 0; i < numGeometries; i++) {
      geometries[i] = read(input);
    }
    return GeometryUtils.GEOMETRY_FACTORY.createGeometryCollection(geometries);
  }

  private static class PrecisionReader {
    protected double precisionMultiplier;

    public PrecisionReader(final int precision) {
      precisionMultiplier = Math.pow(10, precision);
    }

    public Coordinate readPoint(final ByteBuffer input) throws IOException {
      return new Coordinate(
          (VarintUtils.readSignedLong(input)) / precisionMultiplier,
          (VarintUtils.readSignedLong(input)) / precisionMultiplier);
    }

    public Coordinate[] readPointArray(final ByteBuffer input) throws IOException {
      final int numCoordinates = VarintUtils.readUnsignedInt(input);
      final Coordinate[] coordinates = new Coordinate[numCoordinates];
      long lastX = 0;
      long lastY = 0;
      for (int i = 0; i < numCoordinates; i++) {
        lastX = VarintUtils.readSignedLong(input) + lastX;
        lastY = VarintUtils.readSignedLong(input) + lastY;
        coordinates[i] =
            new Coordinate((lastX) / precisionMultiplier, (lastY) / precisionMultiplier);
      }
      return coordinates;
    }
  }

  private static class ExtendedPrecisionReader extends PrecisionReader {
    private boolean hasZ = false;
    private double zPrecisionMultiplier = 0;
    private boolean hasM = false;
    private double mPrecisionMultiplier = 0;

    public ExtendedPrecisionReader(final int precision, final byte extendedDimensions) {
      super(precision);
      if ((extendedDimensions & 0x1) != 0) {
        hasZ = true;
        zPrecisionMultiplier =
            Math.pow(10, TWKBUtils.zigZagDecode((extendedDimensions >> 2) & 0x7));
      }
      if ((extendedDimensions & 0x2) != 0) {
        hasM = true;
        mPrecisionMultiplier =
            Math.pow(10, TWKBUtils.zigZagDecode((extendedDimensions >> 5) & 0x7));
      }
    }

    @Override
    public Coordinate readPoint(final ByteBuffer input) throws IOException {
      final Coordinate coordinate = super.readPoint(input);
      if (hasZ) {
        coordinate.setZ(VarintUtils.readSignedLong(input) / zPrecisionMultiplier);
      }
      if (hasM) {
        coordinate.setM(VarintUtils.readSignedLong(input) / mPrecisionMultiplier);
      }
      return coordinate;
    }

    @Override
    public Coordinate[] readPointArray(final ByteBuffer input) throws IOException {
      final int numCoordinates = VarintUtils.readUnsignedInt(input);
      final Coordinate[] coordinates = new Coordinate[numCoordinates];
      long lastX = 0;
      long lastY = 0;
      long lastZ = 0;
      long lastM = 0;
      for (int i = 0; i < numCoordinates; i++) {
        lastX = VarintUtils.readSignedLong(input) + lastX;
        lastY = VarintUtils.readSignedLong(input) + lastY;
        coordinates[i] =
            new Coordinate((lastX) / precisionMultiplier, (lastY) / precisionMultiplier);
        if (hasZ) {
          lastZ = VarintUtils.readSignedLong(input) + lastZ;
          coordinates[i].setZ((lastZ) / zPrecisionMultiplier);
        }
        if (hasM) {
          lastM = VarintUtils.readSignedLong(input) + lastM;
          coordinates[i].setM((lastM) / mPrecisionMultiplier);
        }
      }
      return coordinates;
    }
  }
}
