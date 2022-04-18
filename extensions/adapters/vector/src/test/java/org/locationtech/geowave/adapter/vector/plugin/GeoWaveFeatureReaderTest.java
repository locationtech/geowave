/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.DelegatingFeatureReader;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.feature.SchemaException;
import org.geotools.feature.visitor.MaxVisitor;
import org.geotools.feature.visitor.MinVisitor;
import org.geotools.filter.FilterFactoryImpl;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.BaseDataStoreTest;
import org.locationtech.geowave.adapter.vector.util.DateUtilities;
import org.locationtech.geowave.core.geotime.index.api.SpatialIndexBuilder;
import org.locationtech.geowave.core.geotime.index.api.SpatialTemporalIndexBuilder;
import org.locationtech.geowave.core.store.index.AttributeDimensionalityTypeProvider;
import org.locationtech.geowave.core.store.index.AttributeIndexOptions;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class GeoWaveFeatureReaderTest extends BaseDataStoreTest {
  DataStore dataStore;
  SimpleFeatureType schema;
  SimpleFeatureType type;
  final GeometryFactory factory = new GeometryFactory(new PrecisionModel(PrecisionModel.FIXED));
  Query query = null;
  List<String> fids = new ArrayList<>();
  List<String> pids = new ArrayList<>();
  Date stime, mtime, etime;

  @Before
  public void setup() throws SchemaException, CQLException, Exception {
    dataStore = createDataStore();
    type =
        DataUtilities.createType(
            "GeoWaveFeatureReaderTest",
            "geometry:Geometry:srid=4326,start:Date,end:Date,pop:java.lang.Long,pid:String");
    ((GeoWaveGTDataStore) dataStore).getDataStore().addIndex(
        new SpatialIndexBuilder().createIndex());
    ((GeoWaveGTDataStore) dataStore).getDataStore().addIndex(
        new SpatialTemporalIndexBuilder().createIndex());
    dataStore.createSchema(type);
    ((GeoWaveGTDataStore) dataStore).getDataStore().addIndex(
        type.getTypeName(),
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ((GeoWaveGTDataStore) dataStore).getDataStore(),
            new AttributeIndexOptions(type.getTypeName(), "pop")));

    stime = DateUtilities.parseISO("2005-05-15T20:32:56Z");
    mtime = DateUtilities.parseISO("2005-05-20T20:32:56Z");
    etime = DateUtilities.parseISO("2005-05-25T20:32:56Z");

    final Transaction transaction1 = new DefaultTransaction();
    final FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
        dataStore.getFeatureWriter(type.getTypeName(), transaction1);
    assertFalse(writer.hasNext());
    SimpleFeature newFeature = writer.next();
    newFeature.setAttribute("pop", Long.valueOf(100));
    newFeature.setAttribute("pid", "a" + UUID.randomUUID().toString());
    newFeature.setAttribute("start", stime);
    newFeature.setAttribute("end", mtime);
    newFeature.setAttribute("geometry", factory.createPoint(new Coordinate(27.25, 41.25)));
    fids.add(newFeature.getID());
    pids.add(newFeature.getAttribute("pid").toString());
    writer.write();
    newFeature = writer.next();
    newFeature.setAttribute("pop", Long.valueOf(101));
    newFeature.setAttribute("pid", "b" + UUID.randomUUID().toString());
    newFeature.setAttribute("start", mtime);
    newFeature.setAttribute("end", etime);
    newFeature.setAttribute("geometry", factory.createPoint(new Coordinate(28.25, 41.25)));
    fids.add(newFeature.getID());
    pids.add(newFeature.getAttribute("pid").toString());
    writer.write();
    writer.close();
    transaction1.commit();
    transaction1.close();

    query =
        new Query(
            "GeoWaveFeatureReaderTest",
            ECQL.toFilter("IN ('" + fids.get(0) + "')"),
            new String[] {"geometry", "pid"});
  }

  @Test
  public void testFID()
      throws IllegalArgumentException, NoSuchElementException, IOException, CQLException {
    final FeatureReader<SimpleFeatureType, SimpleFeature> reader =
        dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
    int count = 0;
    while (reader.hasNext()) {
      final SimpleFeature feature = reader.next();
      assertTrue(fids.contains(feature.getID()));
      count++;
    }
    assertTrue(count > 0);
  }

  @Test
  public void testAttributeIndex() throws CQLException, IOException {
    final Query ecqlQuery =
        new Query(
            "GeoWaveFeatureReaderTest",
            ECQL.toFilter("pop > 100"),
            new String[] {"geometry", "pid", "pop"});

    FeatureReader<SimpleFeatureType, SimpleFeature> reader =
        dataStore.getFeatureReader(ecqlQuery, Transaction.AUTO_COMMIT);
    int count = 0;
    while (reader.hasNext()) {
      final SimpleFeature feature = reader.next();
      assertEquals(fids.get(1), feature.getID());
      count++;
    }
    reader.close();
    assertEquals(1, count);

    final Query cqlQuery =
        new Query(
            "GeoWaveFeatureReaderTest",
            CQL.toFilter("pop >= 100"),
            new String[] {"geometry", "pid", "pop"});

    reader = dataStore.getFeatureReader(cqlQuery, Transaction.AUTO_COMMIT);
    count = 0;
    while (reader.hasNext()) {
      final SimpleFeature feature = reader.next();
      assertTrue(fids.contains(feature.getID()));
      count++;
    }
    reader.close();
    assertEquals(2, count);
  }

  @Test
  public void testTemporal()
      throws IllegalArgumentException, NoSuchElementException, IOException, CQLException {
    // This tests performs both CQL and ECQL queries on a time-based attribute because different
    // geometry visitors are used to extract the geometry portion of the query. Under normal
    // circumstances this is fine except for when there is no geometry constraint specified. Using
    // CQL will result in a default geometry with infinite area. ECQL results in a null geometry.
    // This test checks both code paths to ensure there are no unintended errors.
    final Query ecqlQuery =
        new Query(
            "GeoWaveFeatureReaderTest",
            ECQL.toFilter("start AFTER 2005-05-16T20:32:56Z"),
            new String[] {"geometry", "pid"});

    FeatureReader<SimpleFeatureType, SimpleFeature> reader =
        dataStore.getFeatureReader(ecqlQuery, Transaction.AUTO_COMMIT);
    int count = 0;
    while (reader.hasNext()) {
      final SimpleFeature feature = reader.next();
      assertTrue(fids.contains(feature.getID()));
      count++;
    }
    reader.close();
    assertEquals(1, count);

    final Query cqlQuery =
        new Query(
            "GeoWaveFeatureReaderTest",
            CQL.toFilter("start >= '2005-05-16 20:32:56+0000'"),
            new String[] {"geometry", "pid"});

    reader = dataStore.getFeatureReader(cqlQuery, Transaction.AUTO_COMMIT);
    count = 0;
    while (reader.hasNext()) {
      final SimpleFeature feature = reader.next();
      assertTrue(fids.contains(feature.getID()));
      count++;
    }
    reader.close();
    assertEquals(1, count);
  }

  @Test
  public void testSmallBBOX() throws IllegalArgumentException, NoSuchElementException, IOException {
    final FilterFactoryImpl factory = new FilterFactoryImpl();
    final Query query =
        new Query(
            "GeoWaveFeatureReaderTest",
            factory.bbox("geometry", 28, 41, 28.5, 41.5, "EPSG:4326"),
            new String[] {"geometry", "pid"});

    final FeatureReader<SimpleFeatureType, SimpleFeature> reader =
        dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
    int count = 0;
    while (reader.hasNext()) {
      final SimpleFeature feature = reader.next();
      assertTrue(fids.contains(feature.getID()));
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testBBOX() throws IllegalArgumentException, NoSuchElementException, IOException {
    final FilterFactoryImpl factory = new FilterFactoryImpl();
    final Query query =
        new Query(
            "GeoWaveFeatureReaderTest",
            factory.bbox("geometry", -180, -90, 180, 90, "EPSG:4326"),
            new String[] {"geometry", "pid"});

    final FeatureReader<SimpleFeatureType, SimpleFeature> reader =
        dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
    int count = 0;
    while (reader.hasNext()) {
      final SimpleFeature feature = reader.next();
      assertTrue(fids.contains(feature.getID()));
      count++;
    }
    assertTrue(count > 0);
  }

  @Test
  public void testRangeIndex()
      throws IllegalArgumentException, NoSuchElementException, IOException {
    final FeatureReader<SimpleFeatureType, SimpleFeature> reader =
        dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
    int count = 0;
    while (reader.hasNext()) {
      final SimpleFeature feature = reader.next();
      assertTrue(fids.contains(feature.getID()));
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testLike()
      throws IllegalArgumentException, NoSuchElementException, IOException, CQLException {
    System.out.println(pids);
    final Query query =
        new Query(
            "GeoWaveFeatureReaderTest",
            ECQL.toFilter("pid like '" + pids.get(0).substring(0, 1) + "%'"),
            new String[] {"geometry", "pid"});
    final FeatureReader<SimpleFeatureType, SimpleFeature> reader =
        dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
    int count = 0;
    while (reader.hasNext()) {
      final SimpleFeature feature = reader.next();
      assertTrue(fids.contains(feature.getID()));
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testMax() throws IllegalArgumentException, NoSuchElementException, IOException {
    final FeatureReader<SimpleFeatureType, SimpleFeature> reader =
        dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
    final MaxVisitor visitor = new MaxVisitor("start", type);
    unwrapDelegatingFeatureReader(reader).getFeatureCollection().accepts(visitor, null);
    assertTrue(visitor.getMax().equals(mtime));
  }

  @Test
  public void testMin() throws IllegalArgumentException, NoSuchElementException, IOException {
    final FeatureReader<SimpleFeatureType, SimpleFeature> reader =
        dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
    final MinVisitor visitor = new MinVisitor("start", type);
    unwrapDelegatingFeatureReader(reader).getFeatureCollection().accepts(visitor, null);
    assertTrue(visitor.getMin().equals(stime));
  }

  private GeoWaveFeatureReader unwrapDelegatingFeatureReader(
      final FeatureReader<SimpleFeatureType, SimpleFeature> reader) {
    // GeoTools uses decorator pattern to wrap FeatureReaders
    // we need to get down to the inner GeoWaveFeatureReader
    FeatureReader<SimpleFeatureType, SimpleFeature> currReader = reader;
    while (!(currReader instanceof GeoWaveFeatureReader)) {
      currReader =
          ((DelegatingFeatureReader<SimpleFeatureType, SimpleFeature>) currReader).getDelegate();
    }
    return (GeoWaveFeatureReader) currReader;
  }
}
