/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.util;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
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
import com.clearspring.analytics.util.Varint;

public class TWKBReader {
  private static class ByteBufferInputStream extends InputStream {
    private final ByteBuffer byteBuffer;

    public ByteBufferInputStream(final ByteBuffer byteBuffer) {
      this.byteBuffer = byteBuffer;
    }

    @Override
    public int read() throws IOException {
      if (!byteBuffer.hasRemaining()) {
        return -1;
      }
      return byteBuffer.get() & 0xFF;
    }

    @Override
    public int read(final byte[] bytes, final int offset, final int length) throws IOException {
      if (length == 0) {
        return 0;
      }
      final int count = Math.min(byteBuffer.remaining(), length);
      if (count == 0) {
        return -1;
      }
      byteBuffer.get(bytes, offset, count);
      return count;
    }

    @Override
    public int available() throws IOException {
      return byteBuffer.remaining();
    }
  }

  public TWKBReader() {}

  public Geometry read(final ByteBuffer bytes) throws ParseException {
    try (ByteBufferInputStream in = new ByteBufferInputStream(bytes)) {
      try (DataInputStream input = new DataInputStream(in)) {
        return read(input);
      }
    } catch (final IOException e) {
      throw new ParseException("Error reading TWKB geometry.", e);
    }
  }

  public Geometry read(final byte[] bytes) throws ParseException {
    try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
      try (DataInputStream input = new DataInputStream(in)) {
        return read(input);
      }
    } catch (final IOException e) {
      throw new ParseException("Error reading TWKB geometry.", e);
    }
  }

  public Geometry read(final DataInput input) throws IOException {
    final byte typeAndPrecision = input.readByte();
    final byte type = (byte) (typeAndPrecision & 0x0F);
    final int basePrecision = TWKBUtils.zigZagDecode((typeAndPrecision & 0xF0) >> 4);
    final byte metadata = input.readByte();
    PrecisionReader precision;
    if ((metadata & TWKBUtils.EXTENDED_DIMENSIONS) != 0) {
      final byte extendedDimensions = input.readByte();
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

  private Point readPoint(
      final PrecisionReader precision,
      final byte metadata,
      final DataInput input) throws IOException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createPoint();
    }

    final Coordinate coordinate = precision.readPoint(input);
    return GeometryUtils.GEOMETRY_FACTORY.createPoint(coordinate);
  }

  private LineString readLineString(
      final PrecisionReader precision,
      final byte metadata,
      final DataInput input) throws IOException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createLineString();
    }

    final Coordinate[] coordinates = precision.readPointArray(input);
    return GeometryUtils.GEOMETRY_FACTORY.createLineString(coordinates);
  }

  private Polygon readPolygon(
      final PrecisionReader precision,
      final byte metadata,
      final DataInput input) throws IOException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createPolygon();
    }
    final int numRings = Varint.readUnsignedVarInt(input);
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
      final DataInput input) throws IOException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createMultiPoint();
    }
    final Coordinate[] points = precision.readPointArray(input);
    return GeometryUtils.GEOMETRY_FACTORY.createMultiPointFromCoords(points);
  }

  private MultiLineString readMultiLineString(
      final PrecisionReader precision,
      final byte metadata,
      final DataInput input) throws IOException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createMultiLineString();
    }
    final int numLines = Varint.readUnsignedVarInt(input);
    final LineString[] lines = new LineString[numLines];
    for (int i = 0; i < numLines; i++) {
      lines[i] = GeometryUtils.GEOMETRY_FACTORY.createLineString(precision.readPointArray(input));
    }
    return GeometryUtils.GEOMETRY_FACTORY.createMultiLineString(lines);
  }

  private MultiPolygon readMultiPolygon(
      final PrecisionReader precision,
      final byte metadata,
      final DataInput input) throws IOException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createMultiPolygon();
    }
    final int numPolygons = Varint.readUnsignedVarInt(input);
    final Polygon[] polygons = new Polygon[numPolygons];
    int numRings;
    for (int i = 0; i < numPolygons; i++) {
      numRings = Varint.readUnsignedVarInt(input);
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

  private GeometryCollection readGeometryCollection(final DataInput input, final byte metadata)
      throws IOException {
    if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
      return GeometryUtils.GEOMETRY_FACTORY.createGeometryCollection();
    }
    final int numGeometries = Varint.readUnsignedVarInt(input);
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

    public Coordinate readPoint(final DataInput input) throws IOException {
      return new Coordinate(
          (Varint.readSignedVarLong(input)) / precisionMultiplier,
          (Varint.readSignedVarLong(input)) / precisionMultiplier);
    }

    public Coordinate[] readPointArray(final DataInput input) throws IOException {
      final int numCoordinates = Varint.readUnsignedVarInt(input);
      final Coordinate[] coordinates = new Coordinate[numCoordinates];
      long lastX = 0;
      long lastY = 0;
      for (int i = 0; i < numCoordinates; i++) {
        lastX = Varint.readSignedVarLong(input) + lastX;
        lastY = Varint.readSignedVarLong(input) + lastY;
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
    public Coordinate readPoint(final DataInput input) throws IOException {
      final Coordinate coordinate = super.readPoint(input);
      if (hasZ) {
        coordinate.setZ(Varint.readSignedVarLong(input) / zPrecisionMultiplier);
      }
      if (hasM) {
        coordinate.setM(Varint.readSignedVarLong(input) / mPrecisionMultiplier);
      }
      return coordinate;
    }

    @Override
    public Coordinate[] readPointArray(final DataInput input) throws IOException {
      final int numCoordinates = Varint.readUnsignedVarInt(input);
      final Coordinate[] coordinates = new Coordinate[numCoordinates];
      long lastX = 0;
      long lastY = 0;
      long lastZ = 0;
      long lastM = 0;
      for (int i = 0; i < numCoordinates; i++) {
        lastX = Varint.readSignedVarLong(input) + lastX;
        lastY = Varint.readSignedVarLong(input) + lastY;
        coordinates[i] =
            new Coordinate((lastX) / precisionMultiplier, (lastY) / precisionMultiplier);
        if (hasZ) {
          lastZ = Varint.readSignedVarLong(input) + lastZ;
          coordinates[i].setZ((lastZ) / zPrecisionMultiplier);
        }
        if (hasM) {
          lastM = Varint.readSignedVarLong(input) + lastM;
          coordinates[i].setM((lastM) / mPrecisionMultiplier);
        }
      }
      return coordinates;
    }
  }
}
