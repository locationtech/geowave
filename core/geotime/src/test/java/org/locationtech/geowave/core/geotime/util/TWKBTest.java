/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.util;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;

public class TWKBTest {
  private static GeometryFactory factory = null;
  private static TWKBWriter writerFullPrecision = null;
  private static TWKBWriter writer3Precision = null;
  private static TWKBWriter writer0Precision = null;
  private static TWKBWriter writerNegativePrecision = null;
  private static TWKBReader reader = null;

  @BeforeClass
  public static void init() {
    factory = new GeometryFactory();
    writerFullPrecision = new TWKBWriter();
    writer3Precision = new TWKBWriter(3);
    writer0Precision = new TWKBWriter(0);
    writerNegativePrecision = new TWKBWriter(-3);
    reader = new TWKBReader();
  }

  @Test
  public void testReadWritePoint() throws ParseException {
    final Point point = factory.createPoint(new Coordinate(12.13281248321, -1518.375));
    Point expected = factory.createPoint(new Coordinate(12.1328125, -1518.375)); // maximum
    // precision is 7
    // decimal digits
    byte[] encoded = writerFullPrecision.write(point);
    Geometry decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected = factory.createPoint(new Coordinate(12.133, -1518.375));
    encoded = writer3Precision.write(point);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected = factory.createPoint(new Coordinate(12, -1518));
    encoded = writer0Precision.write(point);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected = factory.createPoint(new Coordinate(0, -2000));
    encoded = writerNegativePrecision.write(point);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    // test empty
    expected = factory.createPoint();
    encoded = writerFullPrecision.write(expected);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);
  }

  @Test
  public void testReadWriteLine() throws ParseException {
    final LineString line =
        factory.createLineString(
            new Coordinate[] {
                new Coordinate(12.13281248321, -1518.375),
                new Coordinate(15.875, -1495.38281248325),
                new Coordinate(17.2635, -1384.75)});
    LineString expected =
        factory.createLineString(
            new Coordinate[] {
                new Coordinate(12.1328125, -1518.375),
                new Coordinate(15.875, -1495.3828125),
                new Coordinate(17.2635, -1384.75)});
    byte[] encoded = writerFullPrecision.write(line);
    Geometry decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected =
        factory.createLineString(
            new Coordinate[] {
                new Coordinate(12.133, -1518.375),
                new Coordinate(15.875, -1495.383),
                new Coordinate(17.264, -1384.75)});
    encoded = writer3Precision.write(line);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected =
        factory.createLineString(
            new Coordinate[] {
                new Coordinate(12, -1518),
                new Coordinate(16, -1495),
                new Coordinate(17, -1385)});
    encoded = writer0Precision.write(line);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected =
        factory.createLineString(
            new Coordinate[] {
                new Coordinate(0, -2000),
                new Coordinate(0, -1000),
                new Coordinate(0, -1000)});
    encoded = writerNegativePrecision.write(line);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    // test empty
    expected = factory.createLineString();
    encoded = writerFullPrecision.write(expected);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);
  }

  @Test
  public void testReadWritePolygon() throws ParseException {
    final Polygon poly =
        factory.createPolygon(
            factory.createLinearRing(
                new Coordinate[] {
                    new Coordinate(12.13281248321, -1518.375),
                    new Coordinate(24.875, -1518.38281248325),
                    new Coordinate(24.2635, -1284.75),
                    new Coordinate(12.325, -1282.125),
                    new Coordinate(12.13281248321, -1518.375)}),
            new LinearRing[] {
                factory.createLinearRing(
                    new Coordinate[] {
                        new Coordinate(13.5, -1500.1),
                        new Coordinate(20.27335, -1495.3424),
                        new Coordinate(20.1275, -1350.25),
                        new Coordinate(13.875, -1348.75),
                        new Coordinate(13.5, -1500.1)}),
                factory.createLinearRing(
                    new Coordinate[] {
                        new Coordinate(13.5, -1325.195),
                        new Coordinate(20.27335, -1349.51),
                        new Coordinate(20.1275, -1450.325),
                        new Coordinate(13.5, -1325.195)})});
    Polygon expected =
        factory.createPolygon(
            factory.createLinearRing(
                new Coordinate[] {
                    new Coordinate(12.1328125, -1518.375),
                    new Coordinate(24.875, -1518.3828125),
                    new Coordinate(24.2635, -1284.75),
                    new Coordinate(12.325, -1282.125),
                    new Coordinate(12.1328125, -1518.375)}),
            new LinearRing[] {
                factory.createLinearRing(
                    new Coordinate[] {
                        new Coordinate(13.5, -1500.1),
                        new Coordinate(20.27335, -1495.3424),
                        new Coordinate(20.1275, -1350.25),
                        new Coordinate(13.875, -1348.75),
                        new Coordinate(13.5, -1500.1)}),
                factory.createLinearRing(
                    new Coordinate[] {
                        new Coordinate(13.5, -1325.195),
                        new Coordinate(20.27335, -1349.51),
                        new Coordinate(20.1275, -1450.325),
                        new Coordinate(13.5, -1325.195)})});
    byte[] encoded = writerFullPrecision.write(poly);
    Geometry decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected =
        factory.createPolygon(
            factory.createLinearRing(
                new Coordinate[] {
                    new Coordinate(12.133, -1518.375),
                    new Coordinate(24.875, -1518.383),
                    new Coordinate(24.264, -1284.75),
                    new Coordinate(12.325, -1282.125),
                    new Coordinate(12.133, -1518.375)}),
            new LinearRing[] {
                factory.createLinearRing(
                    new Coordinate[] {
                        new Coordinate(13.5, -1500.1),
                        new Coordinate(20.273, -1495.342),
                        new Coordinate(20.128, -1350.25),
                        new Coordinate(13.875, -1348.75),
                        new Coordinate(13.5, -1500.1)}),
                factory.createLinearRing(
                    new Coordinate[] {
                        new Coordinate(13.5, -1325.195),
                        new Coordinate(20.273, -1349.51),
                        new Coordinate(20.128, -1450.325),
                        new Coordinate(13.5, -1325.195)})});
    encoded = writer3Precision.write(poly);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected =
        factory.createPolygon(
            factory.createLinearRing(
                new Coordinate[] {
                    new Coordinate(12, -1518),
                    new Coordinate(25, -1518),
                    new Coordinate(24, -1285),
                    new Coordinate(12, -1282),
                    new Coordinate(12, -1518)}),
            new LinearRing[] {
                factory.createLinearRing(
                    new Coordinate[] {
                        new Coordinate(14, -1500),
                        new Coordinate(20, -1495),
                        new Coordinate(20, -1350),
                        new Coordinate(14, -1349),
                        new Coordinate(14, -1500)}),
                factory.createLinearRing(
                    new Coordinate[] {
                        new Coordinate(14, -1325),
                        new Coordinate(20, -1350),
                        new Coordinate(20, -1450),
                        new Coordinate(14, -1325)})});
    encoded = writer0Precision.write(poly);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected =
        factory.createPolygon(
            factory.createLinearRing(
                new Coordinate[] {
                    new Coordinate(0, -2000),
                    new Coordinate(0, -2000),
                    new Coordinate(0, -1000),
                    new Coordinate(0, -1000),
                    new Coordinate(0, -2000)}),
            new LinearRing[] {
                factory.createLinearRing(
                    new Coordinate[] {
                        new Coordinate(0, -2000),
                        new Coordinate(0, -1000),
                        new Coordinate(0, -1000),
                        new Coordinate(0, -1000),
                        new Coordinate(0, -2000)}),
                factory.createLinearRing(
                    new Coordinate[] {
                        new Coordinate(0, -1000),
                        new Coordinate(0, -1000),
                        new Coordinate(0, -1000),
                        new Coordinate(0, -1000)})});
    encoded = writerNegativePrecision.write(poly);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    // test empty
    expected = factory.createPolygon();
    encoded = writerFullPrecision.write(expected);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);
  }

  @Test
  public void testReadWriteMultiPoint() throws ParseException {
    final MultiPoint points =
        factory.createMultiPoint(
            new Point[] {
                factory.createPoint(new Coordinate(12.13281248321, -1518.375)),
                factory.createPoint(new Coordinate(15.875, -1495.38281248325)),
                factory.createPoint(new Coordinate(17.2635, -1384.75))});
    MultiPoint expected =
        factory.createMultiPoint(
            new Point[] {
                factory.createPoint(new Coordinate(12.1328125, -1518.375)),
                factory.createPoint(new Coordinate(15.875, -1495.3828125)),
                factory.createPoint(new Coordinate(17.2635, -1384.75))});
    byte[] encoded = writerFullPrecision.write(points);
    Geometry decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected =
        factory.createMultiPoint(
            new Point[] {
                factory.createPoint(new Coordinate(12.133, -1518.375)),
                factory.createPoint(new Coordinate(15.875, -1495.383)),
                factory.createPoint(new Coordinate(17.264, -1384.75))});
    encoded = writer3Precision.write(points);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected =
        factory.createMultiPoint(
            new Point[] {
                factory.createPoint(new Coordinate(12, -1518)),
                factory.createPoint(new Coordinate(16, -1495)),
                factory.createPoint(new Coordinate(17, -1385))});
    encoded = writer0Precision.write(points);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected =
        factory.createMultiPoint(
            new Point[] {
                factory.createPoint(new Coordinate(0, -2000)),
                factory.createPoint(new Coordinate(0, -1000)),
                factory.createPoint(new Coordinate(0, -1000))});
    encoded = writerNegativePrecision.write(points);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    // test empty
    expected = factory.createMultiPoint();
    encoded = writerFullPrecision.write(expected);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);
  }

  @Test
  public void testReadWriteMultiLineString() throws ParseException {
    final MultiLineString line =
        factory.createMultiLineString(
            new LineString[] {
                factory.createLineString(
                    new Coordinate[] {
                        new Coordinate(13.5, -1500.1),
                        new Coordinate(20.273, -1495.342)}),
                factory.createLineString(
                    new Coordinate[] {
                        new Coordinate(12.13281248321, -1518.375),
                        new Coordinate(15.875, -1495.38281248325),
                        new Coordinate(17.2635, -1384.75)}),
                factory.createLineString(
                    new Coordinate[] {
                        new Coordinate(13.5, -1325.195),
                        new Coordinate(20.27335, -1349.51),
                        new Coordinate(20.1275, -1450.325)})});
    MultiLineString expected =
        factory.createMultiLineString(
            new LineString[] {
                factory.createLineString(
                    new Coordinate[] {
                        new Coordinate(13.5, -1500.1),
                        new Coordinate(20.273, -1495.342)}),
                factory.createLineString(
                    new Coordinate[] {
                        new Coordinate(12.1328125, -1518.375),
                        new Coordinate(15.875, -1495.3828125),
                        new Coordinate(17.2635, -1384.75)}),
                factory.createLineString(
                    new Coordinate[] {
                        new Coordinate(13.5, -1325.195),
                        new Coordinate(20.27335, -1349.51),
                        new Coordinate(20.1275, -1450.325)})});
    byte[] encoded = writerFullPrecision.write(line);
    Geometry decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected =
        factory.createMultiLineString(
            new LineString[] {
                factory.createLineString(
                    new Coordinate[] {
                        new Coordinate(13.5, -1500.1),
                        new Coordinate(20.273, -1495.342)}),
                factory.createLineString(
                    new Coordinate[] {
                        new Coordinate(12.133, -1518.375),
                        new Coordinate(15.875, -1495.383),
                        new Coordinate(17.264, -1384.75)}),
                factory.createLineString(
                    new Coordinate[] {
                        new Coordinate(13.5, -1325.195),
                        new Coordinate(20.273, -1349.51),
                        new Coordinate(20.128, -1450.325)})});
    encoded = writer3Precision.write(line);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected =
        factory.createMultiLineString(
            new LineString[] {
                factory.createLineString(
                    new Coordinate[] {new Coordinate(14, -1500), new Coordinate(20, -1495)}),
                factory.createLineString(
                    new Coordinate[] {
                        new Coordinate(12, -1518),
                        new Coordinate(16, -1495),
                        new Coordinate(17, -1385)}),
                factory.createLineString(
                    new Coordinate[] {
                        new Coordinate(14, -1325),
                        new Coordinate(20, -1350),
                        new Coordinate(20, -1450)})});
    encoded = writer0Precision.write(line);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected =
        factory.createMultiLineString(
            new LineString[] {
                factory.createLineString(
                    new Coordinate[] {new Coordinate(0, -2000), new Coordinate(0, -1000)}),
                factory.createLineString(
                    new Coordinate[] {
                        new Coordinate(0, -2000),
                        new Coordinate(0, -1000),
                        new Coordinate(0, -1000)}),
                factory.createLineString(
                    new Coordinate[] {
                        new Coordinate(0, -1000),
                        new Coordinate(0, -1000),
                        new Coordinate(0, -1000)})});
    encoded = writerNegativePrecision.write(line);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    // test empty
    expected = factory.createMultiLineString();
    encoded = writerFullPrecision.write(expected);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);
  }

  @Test
  public void testReadWriteMultiPolygon() throws ParseException {
    final MultiPolygon multiPoly =
        factory.createMultiPolygon(
            new Polygon[] {
                factory.createPolygon(
                    factory.createLinearRing(
                        new Coordinate[] {
                            new Coordinate(12.13281248321, -1518.375),
                            new Coordinate(24.875, -1518.38281248325),
                            new Coordinate(24.2635, -1284.75),
                            new Coordinate(12.325, -1282.125),
                            new Coordinate(12.13281248321, -1518.375)}),
                    new LinearRing[] {
                        factory.createLinearRing(
                            new Coordinate[] {
                                new Coordinate(13.5, -1500.1),
                                new Coordinate(20.27335, -1495.3424),
                                new Coordinate(20.1275, -1350.25),
                                new Coordinate(13.875, -1348.75),
                                new Coordinate(13.5, -1500.1)}),
                        factory.createLinearRing(
                            new Coordinate[] {
                                new Coordinate(13.5, -1325.195),
                                new Coordinate(20.27335, -1349.51),
                                new Coordinate(20.1275, -1450.325),
                                new Coordinate(13.5, -1325.195)})}),
                factory.createPolygon(
                    factory.createLinearRing(
                        new Coordinate[] {
                            new Coordinate(1513.5, -0.1),
                            new Coordinate(1520.27335, -95.3424),
                            new Coordinate(1520.1275, -50.25),
                            new Coordinate(1513.875, -48.75),
                            new Coordinate(1513.5, -0.1)})),
                factory.createPolygon()});
    MultiPolygon expected =
        factory.createMultiPolygon(
            new Polygon[] {
                factory.createPolygon(
                    factory.createLinearRing(
                        new Coordinate[] {
                            new Coordinate(12.1328125, -1518.375),
                            new Coordinate(24.875, -1518.3828125),
                            new Coordinate(24.2635, -1284.75),
                            new Coordinate(12.325, -1282.125),
                            new Coordinate(12.1328125, -1518.375)}),
                    new LinearRing[] {
                        factory.createLinearRing(
                            new Coordinate[] {
                                new Coordinate(13.5, -1500.1),
                                new Coordinate(20.27335, -1495.3424),
                                new Coordinate(20.1275, -1350.25),
                                new Coordinate(13.875, -1348.75),
                                new Coordinate(13.5, -1500.1)}),
                        factory.createLinearRing(
                            new Coordinate[] {
                                new Coordinate(13.5, -1325.195),
                                new Coordinate(20.27335, -1349.51),
                                new Coordinate(20.1275, -1450.325),
                                new Coordinate(13.5, -1325.195)})}),
                factory.createPolygon(
                    factory.createLinearRing(
                        new Coordinate[] {
                            new Coordinate(1513.5, -0.1),
                            new Coordinate(1520.27335, -95.3424),
                            new Coordinate(1520.1275, -50.25),
                            new Coordinate(1513.875, -48.75),
                            new Coordinate(1513.5, -0.1)})),
                factory.createPolygon()});
    byte[] encoded = writerFullPrecision.write(multiPoly);
    Geometry decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected =
        factory.createMultiPolygon(
            new Polygon[] {
                factory.createPolygon(
                    factory.createLinearRing(
                        new Coordinate[] {
                            new Coordinate(12.133, -1518.375),
                            new Coordinate(24.875, -1518.383),
                            new Coordinate(24.264, -1284.75),
                            new Coordinate(12.325, -1282.125),
                            new Coordinate(12.133, -1518.375)}),
                    new LinearRing[] {
                        factory.createLinearRing(
                            new Coordinate[] {
                                new Coordinate(13.5, -1500.1),
                                new Coordinate(20.273, -1495.342),
                                new Coordinate(20.128, -1350.25),
                                new Coordinate(13.875, -1348.75),
                                new Coordinate(13.5, -1500.1)}),
                        factory.createLinearRing(
                            new Coordinate[] {
                                new Coordinate(13.5, -1325.195),
                                new Coordinate(20.273, -1349.51),
                                new Coordinate(20.128, -1450.325),
                                new Coordinate(13.5, -1325.195)})}),
                factory.createPolygon(
                    factory.createLinearRing(
                        new Coordinate[] {
                            new Coordinate(1513.5, -0.1),
                            new Coordinate(1520.273, -95.342),
                            new Coordinate(1520.128, -50.25),
                            new Coordinate(1513.875, -48.75),
                            new Coordinate(1513.5, -0.1)})),
                factory.createPolygon()});
    encoded = writer3Precision.write(multiPoly);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected =
        factory.createMultiPolygon(
            new Polygon[] {
                factory.createPolygon(
                    factory.createLinearRing(
                        new Coordinate[] {
                            new Coordinate(12, -1518),
                            new Coordinate(25, -1518),
                            new Coordinate(24, -1285),
                            new Coordinate(12, -1282),
                            new Coordinate(12, -1518)}),
                    new LinearRing[] {
                        factory.createLinearRing(
                            new Coordinate[] {
                                new Coordinate(14, -1500),
                                new Coordinate(20, -1495),
                                new Coordinate(20, -1350),
                                new Coordinate(14, -1349),
                                new Coordinate(14, -1500)}),
                        factory.createLinearRing(
                            new Coordinate[] {
                                new Coordinate(14, -1325),
                                new Coordinate(20, -1350),
                                new Coordinate(20, -1450),
                                new Coordinate(14, -1325)})}),
                factory.createPolygon(
                    factory.createLinearRing(
                        new Coordinate[] {
                            new Coordinate(1514, 0),
                            new Coordinate(1520, -95),
                            new Coordinate(1520, -50),
                            new Coordinate(1514, -49),
                            new Coordinate(1514, 0)})),
                factory.createPolygon()});
    encoded = writer0Precision.write(multiPoly);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected =
        factory.createMultiPolygon(
            new Polygon[] {
                factory.createPolygon(
                    factory.createLinearRing(
                        new Coordinate[] {
                            new Coordinate(0, -2000),
                            new Coordinate(0, -2000),
                            new Coordinate(0, -1000),
                            new Coordinate(0, -1000),
                            new Coordinate(0, -2000)}),
                    new LinearRing[] {
                        factory.createLinearRing(
                            new Coordinate[] {
                                new Coordinate(0, -2000),
                                new Coordinate(0, -1000),
                                new Coordinate(0, -1000),
                                new Coordinate(0, -1000),
                                new Coordinate(0, -2000)}),
                        factory.createLinearRing(
                            new Coordinate[] {
                                new Coordinate(0, -1000),
                                new Coordinate(0, -1000),
                                new Coordinate(0, -1000),
                                new Coordinate(0, -1000)})}),
                factory.createPolygon(
                    factory.createLinearRing(
                        new Coordinate[] {
                            new Coordinate(2000, 0),
                            new Coordinate(2000, 0),
                            new Coordinate(2000, 0),
                            new Coordinate(2000, 0),
                            new Coordinate(2000, 0)})),
                factory.createPolygon()});
    encoded = writerNegativePrecision.write(multiPoly);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    // test empty
    expected = factory.createMultiPolygon();
    encoded = writerFullPrecision.write(expected);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);
  }

  @Test
  public void testReadWriteGeometryCollection() throws ParseException {
    final GeometryCollection geoms =
        factory.createGeometryCollection(
            new Geometry[] {
                factory.createPolygon(
                    factory.createLinearRing(
                        new Coordinate[] {
                            new Coordinate(12.13281248321, -1518.375),
                            new Coordinate(24.875, -1518.38281248325),
                            new Coordinate(24.2635, -1284.75),
                            new Coordinate(12.325, -1282.125),
                            new Coordinate(12.13281248321, -1518.375)}),
                    new LinearRing[] {
                        factory.createLinearRing(
                            new Coordinate[] {
                                new Coordinate(13.5, -1500.1),
                                new Coordinate(20.27335, -1495.3424),
                                new Coordinate(20.1275, -1350.25),
                                new Coordinate(13.875, -1348.75),
                                new Coordinate(13.5, -1500.1)}),
                        factory.createLinearRing(
                            new Coordinate[] {
                                new Coordinate(13.5, -1325.195),
                                new Coordinate(20.27335, -1349.51),
                                new Coordinate(20.1275, -1450.325),
                                new Coordinate(13.5, -1325.195)})}),
                factory.createLineString(
                    new Coordinate[] {
                        new Coordinate(1513.5, -0.1),
                        new Coordinate(1520.27335, -95.3424),
                        new Coordinate(1520.1275, -50.25),
                        new Coordinate(1513.875, -48.75),
                        new Coordinate(1513.5, -0.1)}),
                factory.createPoint(new Coordinate(12.34, 18.1)),
                factory.createPoint(),
                factory.createLineString(),
                factory.createPolygon()});
    GeometryCollection expected =
        factory.createGeometryCollection(
            new Geometry[] {
                factory.createPolygon(
                    factory.createLinearRing(
                        new Coordinate[] {
                            new Coordinate(12.1328125, -1518.375),
                            new Coordinate(24.875, -1518.3828125),
                            new Coordinate(24.2635, -1284.75),
                            new Coordinate(12.325, -1282.125),
                            new Coordinate(12.1328125, -1518.375)}),
                    new LinearRing[] {
                        factory.createLinearRing(
                            new Coordinate[] {
                                new Coordinate(13.5, -1500.1),
                                new Coordinate(20.27335, -1495.3424),
                                new Coordinate(20.1275, -1350.25),
                                new Coordinate(13.875, -1348.75),
                                new Coordinate(13.5, -1500.1)}),
                        factory.createLinearRing(
                            new Coordinate[] {
                                new Coordinate(13.5, -1325.195),
                                new Coordinate(20.27335, -1349.51),
                                new Coordinate(20.1275, -1450.325),
                                new Coordinate(13.5, -1325.195)})}),
                factory.createLineString(
                    new Coordinate[] {
                        new Coordinate(1513.5, -0.1),
                        new Coordinate(1520.27335, -95.3424),
                        new Coordinate(1520.1275, -50.25),
                        new Coordinate(1513.875, -48.75),
                        new Coordinate(1513.5, -0.1)}),
                factory.createPoint(new Coordinate(12.34, 18.1)),
                factory.createPoint(),
                factory.createLineString(),
                factory.createPolygon()});
    byte[] encoded = writerFullPrecision.write(geoms);
    Geometry decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected =
        factory.createGeometryCollection(
            new Geometry[] {
                factory.createPolygon(
                    factory.createLinearRing(
                        new Coordinate[] {
                            new Coordinate(12.133, -1518.375),
                            new Coordinate(24.875, -1518.383),
                            new Coordinate(24.264, -1284.75),
                            new Coordinate(12.325, -1282.125),
                            new Coordinate(12.133, -1518.375)}),
                    new LinearRing[] {
                        factory.createLinearRing(
                            new Coordinate[] {
                                new Coordinate(13.5, -1500.1),
                                new Coordinate(20.273, -1495.342),
                                new Coordinate(20.128, -1350.25),
                                new Coordinate(13.875, -1348.75),
                                new Coordinate(13.5, -1500.1)}),
                        factory.createLinearRing(
                            new Coordinate[] {
                                new Coordinate(13.5, -1325.195),
                                new Coordinate(20.273, -1349.51),
                                new Coordinate(20.128, -1450.325),
                                new Coordinate(13.5, -1325.195)})}),
                factory.createLineString(
                    new Coordinate[] {
                        new Coordinate(1513.5, -0.1),
                        new Coordinate(1520.273, -95.342),
                        new Coordinate(1520.128, -50.25),
                        new Coordinate(1513.875, -48.75),
                        new Coordinate(1513.5, -0.1)}),
                factory.createPoint(new Coordinate(12.34, 18.1)),
                factory.createPoint(),
                factory.createLineString(),
                factory.createPolygon()});
    encoded = writer3Precision.write(geoms);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected =
        factory.createGeometryCollection(
            new Geometry[] {
                factory.createPolygon(
                    factory.createLinearRing(
                        new Coordinate[] {
                            new Coordinate(12, -1518),
                            new Coordinate(25, -1518),
                            new Coordinate(24, -1285),
                            new Coordinate(12, -1282),
                            new Coordinate(12, -1518)}),
                    new LinearRing[] {
                        factory.createLinearRing(
                            new Coordinate[] {
                                new Coordinate(14, -1500),
                                new Coordinate(20, -1495),
                                new Coordinate(20, -1350),
                                new Coordinate(14, -1349),
                                new Coordinate(14, -1500)}),
                        factory.createLinearRing(
                            new Coordinate[] {
                                new Coordinate(14, -1325),
                                new Coordinate(20, -1350),
                                new Coordinate(20, -1450),
                                new Coordinate(14, -1325)})}),
                factory.createLineString(
                    new Coordinate[] {
                        new Coordinate(1514, 0),
                        new Coordinate(1520, -95),
                        new Coordinate(1520, -50),
                        new Coordinate(1514, -49),
                        new Coordinate(1514, 0)}),
                factory.createPoint(new Coordinate(12, 18)),
                factory.createPoint(),
                factory.createLineString(),
                factory.createPolygon()});
    encoded = writer0Precision.write(geoms);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    expected =
        factory.createGeometryCollection(
            new Geometry[] {
                factory.createPolygon(
                    factory.createLinearRing(
                        new Coordinate[] {
                            new Coordinate(0, -2000),
                            new Coordinate(0, -2000),
                            new Coordinate(0, -1000),
                            new Coordinate(0, -1000),
                            new Coordinate(0, -2000)}),
                    new LinearRing[] {
                        factory.createLinearRing(
                            new Coordinate[] {
                                new Coordinate(0, -2000),
                                new Coordinate(0, -1000),
                                new Coordinate(0, -1000),
                                new Coordinate(0, -1000),
                                new Coordinate(0, -2000)}),
                        factory.createLinearRing(
                            new Coordinate[] {
                                new Coordinate(0, -1000),
                                new Coordinate(0, -1000),
                                new Coordinate(0, -1000),
                                new Coordinate(0, -1000)})}),
                factory.createLineString(
                    new Coordinate[] {
                        new Coordinate(2000, 0),
                        new Coordinate(2000, 0),
                        new Coordinate(2000, 0),
                        new Coordinate(2000, 0),
                        new Coordinate(2000, 0)}),
                factory.createPoint(new Coordinate(0, 0)),
                factory.createPoint(),
                factory.createLineString(),
                factory.createPolygon()});
    encoded = writerNegativePrecision.write(geoms);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);

    // test empty
    expected = factory.createMultiPolygon();
    encoded = writerFullPrecision.write(expected);
    decoded = reader.read(encoded);
    Assert.assertEquals(expected, decoded);
  }
}
