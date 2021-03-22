/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.query.ExplicitSpatialQuery;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.datastore.accumulo.AccumuloDataStore;
import org.locationtech.geowave.datastore.accumulo.AccumuloDataStoreStatsTest.TestGeometry;
import org.locationtech.geowave.datastore.accumulo.AccumuloDataStoreStatsTest.TestGeometryAdapter;
import org.locationtech.geowave.datastore.accumulo.config.AccumuloOptions;
import org.locationtech.geowave.datastore.accumulo.operations.AccumuloOperations;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

public class AccumuloRangeQueryTest {
  private DataStore mockDataStore;
  private Index index;
  private DataTypeAdapter<TestGeometry> adapter;
  private final GeometryFactory factory = new GeometryFactory();
  private final TestGeometry testdata =
      new TestGeometry(
          factory.createPolygon(
              new Coordinate[] {
                  new Coordinate(1.025, 1.032),
                  new Coordinate(1.026, 1.032),
                  new Coordinate(1.026, 1.033),
                  new Coordinate(1.025, 1.032)}),
          "test_shape_1");

  @Before
  public void ingestGeometries() throws AccumuloException, AccumuloSecurityException, IOException {
    final MockInstance mockInstance = new MockInstance();
    final Connector mockConnector =
        mockInstance.getConnector("root", new PasswordToken(new byte[0]));

    final AccumuloOptions options = new AccumuloOptions();
    mockDataStore = new AccumuloDataStore(new AccumuloOperations(mockConnector, options), options);

    index = new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());
    adapter = new TestGeometryAdapter();
    mockDataStore.addType(adapter, index);
    try (Writer writer = mockDataStore.createWriter(adapter.getTypeName())) {
      writer.write(testdata);
    }
  }

  @Test
  public void testIntersection() {
    final Geometry testGeo =
        factory.createPolygon(
            new Coordinate[] {
                new Coordinate(1.0249, 1.0319),
                new Coordinate(1.0261, 1.0319),
                new Coordinate(1.0261, 1.0323),
                new Coordinate(1.0249, 1.0319)});
    final QueryConstraints intersectQuery = new ExplicitSpatialQuery(testGeo);
    Assert.assertTrue(testdata.geom.intersects(testGeo));
    final CloseableIterator<TestGeometry> resultOfIntersect =
        (CloseableIterator) mockDataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).constraints(intersectQuery).build());
    Assert.assertTrue(resultOfIntersect.hasNext());
  }

  @Test
  public void largeQuery() {
    final Geometry largeGeo = createPolygon(50000);
    final QueryConstraints largeQuery = new ExplicitSpatialQuery(largeGeo);
    final CloseableIterator itr =
        mockDataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).constraints(largeQuery).build());
    int numfeats = 0;
    while (itr.hasNext()) {
      itr.next();
      numfeats++;
    }
    Assert.assertEquals(numfeats, 1);
  }

  /**
   * Verifies equality for interning is still working as expected (topologically), as the the
   * largeQuery() test has a dependency on this;
   *
   * @throws ParseException
   */
  @Test
  public void testInterning() throws ParseException {
    final Geometry g =
        GeometryUtils.GEOMETRY_FACTORY.createPolygon(
            new Coordinate[] {
                new Coordinate(0, 0),
                new Coordinate(1, 0),
                new Coordinate(1, 1),
                new Coordinate(0, 1),
                new Coordinate(0, 0)});
    final Geometry gNewInstance =
        GeometryUtils.GEOMETRY_FACTORY.createPolygon(
            new Coordinate[] {
                new Coordinate(0, 0),
                new Coordinate(1, 0),
                new Coordinate(1, 1),
                new Coordinate(0, 1),
                new Coordinate(0, 0)});
    final WKBWriter wkbWriter = new WKBWriter();
    final byte[] b = wkbWriter.write(g);
    final byte[] b2 = new byte[b.length];
    System.arraycopy(b, 0, b2, 0, b.length);
    final WKBReader wkbReader = new WKBReader();
    final Geometry gSerialized = wkbReader.read(b);
    final Geometry gSerializedArrayCopy = wkbReader.read(b2);

    Assert.assertEquals(g, gNewInstance);
    Assert.assertEquals(g, gSerializedArrayCopy);
    Assert.assertEquals(gSerialized, gSerializedArrayCopy);
    Assert.assertEquals(gSerialized, gSerializedArrayCopy);
  }

  @Test
  public void testMiss() {
    final QueryConstraints intersectQuery =
        new ExplicitSpatialQuery(
            factory.createPolygon(
                new Coordinate[] {
                    new Coordinate(1.0247, 1.0319),
                    new Coordinate(1.0249, 1.0319),
                    new Coordinate(1.0249, 1.0323),
                    new Coordinate(1.0247, 1.0319)}));
    final CloseableIterator<TestGeometry> resultOfIntersect =
        (CloseableIterator) mockDataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).constraints(intersectQuery).build());
    Assert.assertFalse(resultOfIntersect.hasNext());
  }

  @Test
  public void testEncompass() {
    final QueryConstraints encompassQuery =
        new ExplicitSpatialQuery(
            factory.createPolygon(
                new Coordinate[] {
                    new Coordinate(1.0249, 1.0319),
                    new Coordinate(1.0261, 1.0319),
                    new Coordinate(1.0261, 1.0331),
                    new Coordinate(1.0249, 1.0319)}));
    final CloseableIterator<TestGeometry> resultOfIntersect =
        (CloseableIterator) mockDataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).constraints(encompassQuery).build());
    Assert.assertTrue(resultOfIntersect.hasNext());
    final TestGeometry geom1 = resultOfIntersect.next();
    Assert.assertEquals("test_shape_1", geom1.id);
  }

  private static Polygon createPolygon(final int numPoints) {
    final double centerX = 4;
    final double centerY = 12;
    final int maxRadius = 80;

    final List<Coordinate> coords = new ArrayList<>();
    final Random rand = new Random(8675309l);

    final double increment = (double) 360 / numPoints;

    for (double theta = 0; theta <= 360; theta += increment) {
      final double radius = (rand.nextDouble() * maxRadius) + 0.1;
      final double rad = (theta * Math.PI) / 180.0;
      final double x = centerX + (radius * Math.sin(rad));
      final double y = centerY + (radius * Math.cos(rad));
      coords.add(new Coordinate(x, y));
    }
    coords.add(coords.get(0));
    return GeometryUtils.GEOMETRY_FACTORY.createPolygon(
        coords.toArray(new Coordinate[coords.size()]));
  }

  protected DataTypeAdapter<TestGeometry> createGeometryAdapter() {
    return new TestGeometryAdapter();
  }

}
