/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p>See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.util;

import com.clearspring.analytics.util.Varint;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
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

  public Geometry read(byte[] bytes) throws ParseException {
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
      DataInput input = new DataInputStream(in);
      return read(input);
    } catch (IOException e) {
      throw new ParseException("Error reading TWKB geometry.", e);
    }
  }

  public Geometry read(DataInput input) throws IOException {
    byte typeAndPrecision = input.readByte();
    byte type = (byte) (typeAndPrecision & 0x0F);
    int basePrecision = TWKBUtils.zigZagDecode((typeAndPrecision & 0xF0) >> 4);
    byte metadata = input.readByte();
    PrecisionReader precision;
    if ((metadata & TWKBUtils.EXTENDED_DIMENSIONS) != 0) {
      byte extendedDimensions = input.readByte();
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
  }

  private Point readPoint(PrecisionReader precision, byte metadata, DataInput input)
      throws IOException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createPoint();
    }

    Coordinate coordinate = precision.readPoint(input);
    return GeometryUtils.GEOMETRY_FACTORY.createPoint(coordinate);
  }

  private LineString readLineString(PrecisionReader precision, byte metadata, DataInput input)
      throws IOException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createLineString();
    }

    Coordinate[] coordinates = precision.readPointArray(input);
    return GeometryUtils.GEOMETRY_FACTORY.createLineString(coordinates);
  }

  private Polygon readPolygon(PrecisionReader precision, byte metadata, DataInput input)
      throws IOException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createPolygon();
    }
    int numRings = Varint.readUnsignedVarInt(input);
    LinearRing exteriorRing =
        GeometryUtils.GEOMETRY_FACTORY.createLinearRing(precision.readPointArray(input));
    LinearRing[] interiorRings = new LinearRing[numRings - 1];
    for (int i = 0; i < numRings - 1; i++) {
      interiorRings[i] =
          GeometryUtils.GEOMETRY_FACTORY.createLinearRing(precision.readPointArray(input));
    }
    return GeometryUtils.GEOMETRY_FACTORY.createPolygon(exteriorRing, interiorRings);
  }

  private MultiPoint readMultiPoint(PrecisionReader precision, byte metadata, DataInput input)
      throws IOException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createMultiPoint();
    }
    Coordinate[] points = precision.readPointArray(input);
    return GeometryUtils.GEOMETRY_FACTORY.createMultiPointFromCoords(points);
  }

  private MultiLineString readMultiLineString(
      PrecisionReader precision, byte metadata, DataInput input) throws IOException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createMultiLineString();
    }
    int numLines = Varint.readUnsignedVarInt(input);
    LineString[] lines = new LineString[numLines];
    for (int i = 0; i < numLines; i++) {
      lines[i] = GeometryUtils.GEOMETRY_FACTORY.createLineString(precision.readPointArray(input));
    }
    return GeometryUtils.GEOMETRY_FACTORY.createMultiLineString(lines);
  }

  private MultiPolygon readMultiPolygon(PrecisionReader precision, byte metadata, DataInput input)
      throws IOException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createMultiPolygon();
    }
    int numPolygons = Varint.readUnsignedVarInt(input);
    Polygon[] polygons = new Polygon[numPolygons];
    int numRings;
    for (int i = 0; i < numPolygons; i++) {
      numRings = Varint.readUnsignedVarInt(input);
      if (numRings == 0) {
        polygons[i] = GeometryUtils.GEOMETRY_FACTORY.createPolygon();
        continue;
      }
      LinearRing exteriorRing =
          GeometryUtils.GEOMETRY_FACTORY.createLinearRing(precision.readPointArray(input));
      LinearRing[] interiorRings = new LinearRing[numRings - 1];
      for (int j = 0; j < numRings - 1; j++) {
        interiorRings[j] =
            GeometryUtils.GEOMETRY_FACTORY.createLinearRing(precision.readPointArray(input));
      }
      polygons[i] = GeometryUtils.GEOMETRY_FACTORY.createPolygon(exteriorRing, interiorRings);
    }
    return GeometryUtils.GEOMETRY_FACTORY.createMultiPolygon(polygons);
  }

  private GeometryCollection readGeometryCollection(DataInput input, byte metadata)
      throws IOException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createGeometryCollection();
    }
    int numGeometries = Varint.readUnsignedVarInt(input);
    Geometry[] geometries = new Geometry[numGeometries];
    for (int i = 0; i < numGeometries; i++) {
      geometries[i] = read(input);
    }
    return GeometryUtils.GEOMETRY_FACTORY.createGeometryCollection(geometries);
  }

  private static class PrecisionReader {
    protected double precisionMultiplier;

    public PrecisionReader(int precision) {
      precisionMultiplier = Math.pow(10, precision);
    }

    public Coordinate readPoint(DataInput input) throws IOException {
      return new Coordinate(
          ((double) Varint.readSignedVarLong(input)) / precisionMultiplier,
          ((double) Varint.readSignedVarLong(input)) / precisionMultiplier);
    }

    public Coordinate[] readPointArray(DataInput input) throws IOException {
      int numCoordinates = Varint.readUnsignedVarInt(input);
      Coordinate[] coordinates = new Coordinate[numCoordinates];
      long lastX = 0;
      long lastY = 0;
      for (int i = 0; i < numCoordinates; i++) {
        lastX = Varint.readSignedVarLong(input) + lastX;
        lastY = Varint.readSignedVarLong(input) + lastY;
        coordinates[i] =
            new Coordinate(
                ((double) lastX) / precisionMultiplier, ((double) lastY) / precisionMultiplier);
      }
      return coordinates;
    }
  }

  private static class ExtendedPrecisionReader extends PrecisionReader {
    private boolean hasZ = false;
    private double zPrecisionMultiplier = 0;
    private boolean hasM = false;
    private double mPrecisionMultiplier = 0;

    public ExtendedPrecisionReader(int precision, byte extendedDimensions) {
      super(precision);
      if ((extendedDimensions & 0x1) != 0) {
        hasZ = true;
        zPrecisionMultiplier =
            Math.pow(10, TWKBUtils.zigZagDecode((extendedDimensions >> 2) & 0x7));
      }
      if ((extendedDimensions & 0x2) != 0) {
        hasM = true;
        zPrecisionMultiplier =
            Math.pow(10, TWKBUtils.zigZagDecode((extendedDimensions >> 5) & 0x7));
      }
    }

    @Override
    public Coordinate readPoint(DataInput input) throws IOException {
      Coordinate coordinate = super.readPoint(input);
      if (hasZ) {
        coordinate.setZ(Varint.readSignedVarLong(input) / zPrecisionMultiplier);
      }
      if (hasM) {
        coordinate.setM(Varint.readSignedVarLong(input) / mPrecisionMultiplier);
      }
      return coordinate;
    }

    @Override
    public Coordinate[] readPointArray(DataInput input) throws IOException {
      int numCoordinates = Varint.readUnsignedVarInt(input);
      Coordinate[] coordinates = new Coordinate[numCoordinates];
      long lastX = 0;
      long lastY = 0;
      long lastZ = 0;
      long lastM = 0;
      for (int i = 0; i < numCoordinates; i++) {
        lastX = Varint.readSignedVarLong(input) + lastX;
        lastY = Varint.readSignedVarLong(input) + lastY;
        coordinates[i] =
            new Coordinate(
                ((double) lastX) / precisionMultiplier, ((double) lastY) / precisionMultiplier);
        if (hasZ) {
          lastZ = Varint.readSignedVarLong(input) + lastZ;
          coordinates[i].setZ(((double) lastZ) / zPrecisionMultiplier);
        }
        if (hasM) {
          lastM = Varint.readSignedVarLong(input) + lastM;
          coordinates[i].setM(((double) lastM) / mPrecisionMultiplier);
        }
      }
      return coordinates;
    }
  }
}
