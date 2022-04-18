/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalOptions;
import org.locationtech.geowave.core.geotime.index.TemporalDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.TemporalOptions;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.AttributeDimensionalityTypeProvider;
import org.locationtech.geowave.core.store.index.AttributeIndexOptions;
import org.locationtech.geowave.core.store.query.gwql.Result;
import org.locationtech.geowave.core.store.query.gwql.ResultSet;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveBasicVectorIT;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jersey.repackaged.com.google.common.collect.Iterators;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveQueryLanguageIT extends AbstractGeoWaveBasicVectorIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveQueryLanguageIT.class);
  private static final String TYPE_NAME = "testType";
  private static final String GEOM = "geom";
  private static final String ALT = "alt";
  private static final String POLY = "poly";
  private static final String TIMESTAMP = "Timestamp";
  private static final String LATITUDE = "Latitude";
  private static final String LONGITUDE = "Longitude";
  private static final String INTEGER = "Integer";
  private static final String ID = "ID";
  private static final String COMMENT = "Comment";
  private static final int TOTAL_FEATURES = 128; // Must be power of 2 for tests to pass
  private static final long ONE_DAY_MILLIS = 1000 * 60 * 60 * 24;

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB,
          GeoWaveStoreType.FILESYSTEM})
  protected DataStorePluginOptions dataStore;

  private SimpleFeatureBuilder featureBuilder;

  private static long startMillis;

  @BeforeClass
  public static void reportTestStart() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("----------------------------------");
    LOGGER.warn("*                                *");
    LOGGER.warn("* RUNNING GeoWaveQueryLanguageIT *");
    LOGGER.warn("*                                *");
    LOGGER.warn("----------------------------------");
  }

  @AfterClass
  public static void reportTestFinish() {
    LOGGER.warn("-----------------------------------");
    LOGGER.warn("*                                 *");
    LOGGER.warn("* FINISHED GeoWaveQueryLanguageIT *");
    LOGGER.warn(
        "*          "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.           *");
    LOGGER.warn("*                                 *");
    LOGGER.warn("-----------------------------------");
  }

  @After
  public void cleanupWorkspace() {
    TestUtils.deleteAll(dataStore);
  }

  private DataTypeAdapter<SimpleFeature> createDataAdapter() {

    final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
    final AttributeTypeBuilder ab = new AttributeTypeBuilder();

    builder.setName(TYPE_NAME);

    builder.add(ab.binding(Geometry.class).nillable(false).buildDescriptor(GEOM));
    builder.add(ab.binding(Date.class).nillable(true).buildDescriptor(TIMESTAMP));
    builder.add(ab.binding(Double.class).nillable(false).buildDescriptor(LATITUDE));
    builder.add(ab.binding(Double.class).nillable(false).buildDescriptor(LONGITUDE));
    builder.add(ab.binding(Integer.class).nillable(true).buildDescriptor(INTEGER));
    builder.add(ab.binding(String.class).nillable(true).buildDescriptor(ID));
    builder.add(ab.binding(String.class).nillable(true).buildDescriptor(COMMENT));
    builder.add(ab.binding(Point.class).nillable(true).buildDescriptor(ALT));
    builder.add(ab.binding(Polygon.class).nillable(true).buildDescriptor(POLY));
    builder.setDefaultGeometry(GEOM);

    final SimpleFeatureType featureType = builder.buildFeatureType();
    featureBuilder = new SimpleFeatureBuilder(featureType);

    final SimpleFeatureType sft = featureType;
    final GeotoolsFeatureDataAdapter<SimpleFeature> fda = SimpleIngest.createDataAdapter(sft);
    return fda;
  }

  private final String[] comment = new String[] {"AlphA", "Bravo", "Charlie", null};

  // Each default geometry lies along the line from -64, -64 to 63, 63, while the alternate
  // geometry lies along the line of -64, 64 to 63, -63. This ensures that the alternate geometry
  // lies in different quadrants of the coordinate system.
  private void ingestData(final DataStore dataStore) {
    try (Writer<Object> writer = dataStore.createWriter(TYPE_NAME)) {
      for (int i = 0; i < TOTAL_FEATURES; i++) {
        final double coordinate = i - (TOTAL_FEATURES / 2);
        featureBuilder.set(
            GEOM,
            GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(coordinate, coordinate)));
        featureBuilder.set(TIMESTAMP, new Date(i * ONE_DAY_MILLIS));
        featureBuilder.set(LATITUDE, coordinate);
        featureBuilder.set(LONGITUDE, coordinate);
        featureBuilder.set(INTEGER, (int) coordinate);
        featureBuilder.set(ID, Long.toHexString((long) (coordinate * 1000)));
        featureBuilder.set(COMMENT, comment[i % 4]);
        featureBuilder.set(
            ALT,
            (i % 2) == 1 ? GeometryUtils.GEOMETRY_FACTORY.createPoint(
                new Coordinate(coordinate, -coordinate)) : null);
        featureBuilder.set(
            POLY,
            GeometryUtils.GEOMETRY_FACTORY.createPolygon(
                new Coordinate[] {
                    new Coordinate(coordinate - 1, coordinate - 1),
                    new Coordinate(coordinate - 1, coordinate + 1),
                    new Coordinate(coordinate + 1, coordinate + 1),
                    new Coordinate(coordinate + 1, coordinate - 1),
                    new Coordinate(coordinate - 1, coordinate - 1)}));
        writer.write(featureBuilder.buildFeature(Integer.toString(i)));
      }
    }
  }

  @Test
  public void testSelectionQueries() {
    final DataStore ds = dataStore.createDataStore();

    final DataTypeAdapter<SimpleFeature> adapter = createDataAdapter();

    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    final Index spatialTemporalIndex =
        SpatialTemporalDimensionalityTypeProvider.createIndexFromOptions(
            new SpatialTemporalOptions());
    ds.addType(adapter, spatialIndex, spatialTemporalIndex);

    // Ingest data
    ingestData(ds);

    /////////////////////////////////////////////////////
    // BBOX on non-indexed geometry
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT * FROM %s WHERE BBOX(%s, -64.5, 0.5, 0.5, 64.5)",
                TYPE_NAME,
                ALT))) {
      assertEquals(9, results.columnCount());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // BBOX on Alternate Geometry and starts with on
    // comment
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT * FROM %s WHERE BBOX(%s, -64.5, -32.5, 32.5, 64.5) AND strStartsWith(%s, 'b', true)",
                TYPE_NAME,
                ALT,
                COMMENT))) {
      int count = 0;
      final int commentColumn = results.columnIndex(COMMENT);
      while (results.hasNext()) {
        final Result result = results.next();
        assertEquals("Bravo", result.columnValue(commentColumn));
        count++;
      }
      // 1/4 of entries match the comment predicate, but only 3/4 of those match the bounding box
      assertEquals(Math.round(TOTAL_FEATURES / 8 * 1.5), count);
    }

    /////////////////////////////////////////////////////
    // Constrain latitude and longitude
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT * FROM %s WHERE %s > 5 AND %s < 10 AND %s >= 7",
                TYPE_NAME,
                LATITUDE,
                LATITUDE,
                LONGITUDE))) {
      assertEquals(9, results.columnCount());
      assertEquals(3, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Longitude is an exact range
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT * FROM %s WHERE %s < -31.5 OR %s > 31.5",
                TYPE_NAME,
                LONGITUDE,
                LONGITUDE))) {
      assertEquals(9, results.columnCount());
      assertEquals(TOTAL_FEATURES / 2 + 1, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // BBOX on indexed geometry
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT * FROM %s WHERE BBOX(%s, 0.5, 0.5, 10.5, 10.5)",
                TYPE_NAME,
                GEOM))) {
      assertEquals(9, results.columnCount());
      assertEquals(10, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Spatial-temporal query
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT * FROM %s WHERE BBOX(%s, 0.5, 0.5, 30.5, 30.5) AND %s BEFORE %d",
                TYPE_NAME,
                GEOM,
                TIMESTAMP,
                66 * ONE_DAY_MILLIS + 1))) {
      assertEquals(9, results.columnCount());
      assertEquals(2, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Temporal query
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT * FROM %s WHERE %s BEFORE %d",
                TYPE_NAME,
                TIMESTAMP,
                66 * ONE_DAY_MILLIS + 1))) {
      assertEquals(9, results.columnCount());
      assertEquals(67, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // ID ends with and INTEGER between
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT * FROM %s WHERE strEndsWith(%s, '0') AND %s BETWEEN 10 AND 20",
                TYPE_NAME,
                ID,
                INTEGER))) {
      assertEquals(9, results.columnCount());
      assertEquals(6, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // ID is more constraining
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT * FROM %s WHERE strEndsWith(%s, 'a0') AND %s BETWEEN 0 AND 40",
                TYPE_NAME,
                ID,
                INTEGER))) {
      assertEquals(9, results.columnCount());
      assertEquals(2, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // BBOX on 2 geometries
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT * FROM %s WHERE BBOX(%s, -30.5, -30.5, 30.5, 30.5) AND BBOX(%s, -30.5, -30.5, 30.5, 30.5)",
                TYPE_NAME,
                GEOM,
                ALT))) {
      assertEquals(9, results.columnCount());
      assertEquals(30, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Constrain integer and latitude
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT * FROM %s WHERE %s < -60 AND %s < 5",
                TYPE_NAME,
                INTEGER,
                LATITUDE))) {
      assertEquals(9, results.columnCount());
      assertEquals(4, Iterators.size(results));
    }
  }

  @Test
  public void testTextExpressionQueries() {
    final DataStore ds = dataStore.createDataStore();

    final DataTypeAdapter<SimpleFeature> adapter = createDataAdapter();

    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    ds.addType(adapter, spatialIndex);
    final Index commentIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, COMMENT));
    final Index idIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, ID));

    ds.addIndex(TYPE_NAME, commentIndex, idIndex);

    // Ingest data
    ingestData(ds);

    /////////////////////////////////////////////////////
    // Starts With
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s FROM %s WHERE strStartsWith(%s, 'Br')",
                COMMENT,
                TYPE_NAME,
                COMMENT))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Starts With (ignore case)
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s FROM %s WHERE strStartsWith(%s, 'br', true)",
                COMMENT,
                TYPE_NAME,
                COMMENT))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Ends With
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s FROM %s WHERE strEndsWith(%s, 'phA')",
                COMMENT,
                TYPE_NAME,
                COMMENT))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Ends With (ignore case)
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s FROM %s WHERE strEndsWith(%s, 'pha', true)",
                COMMENT,
                TYPE_NAME,
                COMMENT))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Contains
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s FROM %s WHERE strContains(%s, 'lph')",
                COMMENT,
                TYPE_NAME,
                COMMENT))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Contains (ignore case)
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s FROM %s WHERE strContains(%s, 'al', true)",
                COMMENT,
                TYPE_NAME,
                COMMENT))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Between
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s FROM %s WHERE %s BETWEEN 'A' AND 'C'",
                COMMENT,
                TYPE_NAME,
                COMMENT))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Greater Than
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(String.format("SELECT %s FROM %s WHERE %s > 'B'", COMMENT, TYPE_NAME, COMMENT))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Less Than
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(String.format("SELECT %s FROM %s WHERE %s < 'B'", COMMENT, TYPE_NAME, COMMENT))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Greater Than Or Equal To
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(String.format("SELECT %s FROM %s WHERE %s >= 'B'", COMMENT, TYPE_NAME, COMMENT))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Less Than Or Equal To
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(String.format("SELECT %s FROM %s WHERE %s <= 'B'", COMMENT, TYPE_NAME, COMMENT))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(results));
    }
  }

  @Test
  public void testNumericExpressionQueries() {
    final DataStore ds = dataStore.createDataStore();

    final DataTypeAdapter<SimpleFeature> adapter = createDataAdapter();

    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    ds.addType(adapter, spatialIndex);
    final Index integerIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, INTEGER));
    final Index latitudeIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, LATITUDE));
    final Index longitudeIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, LONGITUDE));

    ds.addIndex(TYPE_NAME, integerIndex, latitudeIndex, longitudeIndex);

    // Ingest data
    ingestData(ds);

    /////////////////////////////////////////////////////
    // Greater Than
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(String.format("SELECT %s FROM %s WHERE %s > 0", INTEGER, TYPE_NAME, INTEGER))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES / 2 - 1, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Less Than
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(String.format("SELECT %s FROM %s WHERE %s < 0", LATITUDE, TYPE_NAME, LATITUDE))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Greater Than Or Equal To
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(String.format("SELECT %s FROM %s WHERE %s >= 0", INTEGER, TYPE_NAME, INTEGER))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Less Than
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format("SELECT %s FROM %s WHERE %s <= 0", LONGITUDE, TYPE_NAME, LONGITUDE))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES / 2 + 1, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Equal To
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(String.format("SELECT %s FROM %s WHERE %s = 12", INTEGER, TYPE_NAME, INTEGER))) {
      assertEquals(1, results.columnCount());
      assertEquals(1, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Not Equal To
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s FROM %s WHERE %s <> 12 AND %s <> 8",
                INTEGER,
                TYPE_NAME,
                INTEGER,
                INTEGER))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES - 2, Iterators.size(results));
    }
  }

  @Test
  public void testTemporalExpressionQueriesTemporalIndex() {
    final DataStore ds = dataStore.createDataStore();

    final DataTypeAdapter<SimpleFeature> adapter = createDataAdapter();

    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    final Index temporalIndex =
        TemporalDimensionalityTypeProvider.createIndexFromOptions(new TemporalOptions());
    ds.addType(adapter, spatialIndex, temporalIndex);

    // Ingest data
    ingestData(ds);

    /////////////////////////////////////////////////////
    // After
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s FROM %s WHERE %s AFTER %d",
                TIMESTAMP,
                TYPE_NAME,
                TIMESTAMP,
                ONE_DAY_MILLIS * (TOTAL_FEATURES / 2)))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES / 2 - 1, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Before
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s FROM %s WHERE %s BEFORE %d",
                TIMESTAMP,
                TYPE_NAME,
                TIMESTAMP,
                ONE_DAY_MILLIS * (TOTAL_FEATURES / 2)))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // During or After
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s FROM %s WHERE %s DURING_OR_AFTER %d",
                TIMESTAMP,
                TYPE_NAME,
                TIMESTAMP,
                ONE_DAY_MILLIS * (TOTAL_FEATURES / 2)))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Before or During
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s FROM %s WHERE %s BEFORE_OR_DURING %d",
                TIMESTAMP,
                TYPE_NAME,
                TIMESTAMP,
                ONE_DAY_MILLIS * (TOTAL_FEATURES / 2)))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES / 2 + 1, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // During
    /////////////////////////////////////////////////////
    final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s FROM %s WHERE %s DURING '%s/%s'",
                TIMESTAMP,
                TYPE_NAME,
                TIMESTAMP,
                format.format(new Date(ONE_DAY_MILLIS * 5)),
                format.format(new Date(ONE_DAY_MILLIS * 10))))) {
      assertEquals(1, results.columnCount());
      assertEquals(5, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Between
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s FROM %s WHERE %s BETWEEN %d AND %d",
                TIMESTAMP,
                TYPE_NAME,
                TIMESTAMP,
                ONE_DAY_MILLIS * 5,
                ONE_DAY_MILLIS * 10))) {
      assertEquals(1, results.columnCount());
      assertEquals(6, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Contains (inverse of During)
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s FROM %s WHERE TCONTAINS('%s/%s', %s)",
                TIMESTAMP,
                TYPE_NAME,
                format.format(new Date(ONE_DAY_MILLIS * 5)),
                format.format(new Date(ONE_DAY_MILLIS * 10)),
                TIMESTAMP))) {
      assertEquals(1, results.columnCount());
      assertEquals(5, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Overlaps
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s FROM %s WHERE TOVERLAPS(%s, '%s/%s')",
                TIMESTAMP,
                TYPE_NAME,
                TIMESTAMP,
                format.format(new Date(ONE_DAY_MILLIS * 5)),
                format.format(new Date(ONE_DAY_MILLIS * 10))))) {
      assertEquals(1, results.columnCount());
      assertEquals(5, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Equal To
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s FROM %s WHERE %s = %d",
                TIMESTAMP,
                TYPE_NAME,
                TIMESTAMP,
                ONE_DAY_MILLIS * 12))) {
      assertEquals(1, results.columnCount());
      assertEquals(1, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Not Equal To
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s FROM %s WHERE %s <> %d AND %s <> %d",
                TIMESTAMP,
                TYPE_NAME,
                TIMESTAMP,
                ONE_DAY_MILLIS * 12,
                TIMESTAMP,
                ONE_DAY_MILLIS * 8))) {
      assertEquals(1, results.columnCount());
      assertEquals(TOTAL_FEATURES - 2, Iterators.size(results));
    }
  }

  @Test
  public void testSpatialExpressionQueries() {
    final DataStore ds = dataStore.createDataStore();

    final DataTypeAdapter<SimpleFeature> adapter = createDataAdapter();

    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    ds.addType(adapter, spatialIndex);
    final Index altIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, ALT));
    final Index polyIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, POLY));

    ds.addIndex(TYPE_NAME, altIndex, polyIndex);

    final String boxPoly =
        GeometryUtils.GEOMETRY_FACTORY.createPolygon(
            new Coordinate[] {
                new Coordinate(-20.5, -20.5),
                new Coordinate(-20.5, 20.5),
                new Coordinate(20.5, 20.5),
                new Coordinate(20.5, -20.5),
                new Coordinate(-20.5, -20.5)}).toText();

    final String boxPoly2 =
        GeometryUtils.GEOMETRY_FACTORY.createPolygon(
            new Coordinate[] {
                new Coordinate(-20, -20),
                new Coordinate(-20, 20),
                new Coordinate(20, 20),
                new Coordinate(20, -20),
                new Coordinate(-20, -20)}).toText();

    // Large diagonal line
    final String line =
        GeometryUtils.GEOMETRY_FACTORY.createLineString(
            new Coordinate[] {new Coordinate(-20.5, -20.5), new Coordinate(20.5, 20.5)}).toText();

    // Ingest data
    ingestData(ds);

    /////////////////////////////////////////////////////
    // Basic BBOX
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s, %s, %s FROM %s WHERE BBOX(%s, 0.5, 0.5, 64.5, 64.5)",
                GEOM,
                POLY,
                ALT,
                TYPE_NAME,
                GEOM))) {
      assertEquals(3, results.columnCount());
      assertEquals(TOTAL_FEATURES / 2 - 1, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Loose BBOX
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s, %s, %s FROM %s WHERE BBOXLOOSE(%s, 0.5, 0.5, 64.5, 64.5)",
                GEOM,
                POLY,
                ALT,
                TYPE_NAME,
                GEOM))) {
      assertEquals(3, results.columnCount());
      assertEquals(TOTAL_FEATURES / 2 - 1, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Intersects
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s, %s, %s FROM %s WHERE INTERSECTS(%s, '%s')",
                GEOM,
                POLY,
                ALT,
                TYPE_NAME,
                ALT,
                boxPoly))) {
      assertEquals(3, results.columnCount());
      assertEquals(20, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Loose Intersects
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s, %s, %s FROM %s WHERE INTERSECTSLOOSE(%s, '%s')",
                GEOM,
                POLY,
                ALT,
                TYPE_NAME,
                ALT,
                boxPoly))) {
      assertEquals(3, results.columnCount());
      assertEquals(20, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Disjoint
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s, %s, %s FROM %s WHERE DISJOINT(%s, '%s')",
                GEOM,
                POLY,
                ALT,
                TYPE_NAME,
                GEOM,
                boxPoly))) {
      assertEquals(3, results.columnCount());
      assertEquals(TOTAL_FEATURES - 41, Iterators.size(results));
    }


    /////////////////////////////////////////////////////
    // Loose Disjoint
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s, %s, %s FROM %s WHERE DISJOINTLOOSE(%s, '%s')",
                GEOM,
                POLY,
                ALT,
                TYPE_NAME,
                GEOM,
                boxPoly))) {
      assertEquals(3, results.columnCount());
      assertEquals(TOTAL_FEATURES - 41, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Crosses
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s, %s, %s FROM %s WHERE CROSSES(%s, '%s')",
                GEOM,
                POLY,
                ALT,
                TYPE_NAME,
                POLY,
                line))) {
      assertEquals(3, results.columnCount());
      assertEquals(43, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Overlaps
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s, %s, %s FROM %s WHERE OVERLAPS(%s, '%s')",
                GEOM,
                POLY,
                ALT,
                TYPE_NAME,
                POLY,
                boxPoly))) {
      assertEquals(3, results.columnCount());
      assertEquals(4, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Contains
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s, %s, %s FROM %s WHERE CONTAINS(%s, '%s')",
                GEOM,
                POLY,
                ALT,
                TYPE_NAME,
                GEOM,
                boxPoly))) {
      assertEquals(3, results.columnCount());
      assertFalse(results.hasNext());
    }

    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s, %s, %s FROM %s WHERE CONTAINS('%s', %s)",
                GEOM,
                POLY,
                ALT,
                TYPE_NAME,
                boxPoly,
                GEOM))) {
      assertEquals(3, results.columnCount());
      assertEquals(41, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Touches
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s, %s, %s FROM %s WHERE TOUCHES(%s, '%s')",
                GEOM,
                POLY,
                ALT,
                TYPE_NAME,
                POLY,
                boxPoly2))) {
      assertEquals(3, results.columnCount());
      assertEquals(2, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // Within
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s, %s, %s FROM %s WHERE WITHIN(%s, '%s')",
                GEOM,
                POLY,
                ALT,
                TYPE_NAME,
                GEOM,
                boxPoly))) {
      assertEquals(3, results.columnCount());
      assertEquals(41, Iterators.size(results));
    }

    /////////////////////////////////////////////////////
    // EqualTo
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s, %s, %s FROM %s WHERE %s = 'POINT(1 1)'",
                GEOM,
                POLY,
                ALT,
                TYPE_NAME,
                GEOM))) {
      assertEquals(3, results.columnCount());
      assertEquals(1, Iterators.size(results));
    }

    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s, %s, %s FROM %s WHERE 'POINT(1 1)'::geometry = %s",
                GEOM,
                POLY,
                ALT,
                TYPE_NAME,
                GEOM))) {
      assertEquals(3, results.columnCount());
      assertEquals(1, Iterators.size(results));
    }


    /////////////////////////////////////////////////////
    // NotEqualTo
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT %s, %s, %s FROM %s WHERE %s <> 'POINT(1 1)'",
                GEOM,
                POLY,
                ALT,
                TYPE_NAME,
                GEOM))) {
      assertEquals(3, results.columnCount());
      assertEquals(TOTAL_FEATURES - 1, Iterators.size(results));
    }
  }

  @Test
  public void testAggregations() {
    final DataStore ds = dataStore.createDataStore();

    final DataTypeAdapter<SimpleFeature> adapter = createDataAdapter();

    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    ds.addType(adapter, spatialIndex);

    final Index latitudeIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, LATITUDE));

    ds.addIndex(TYPE_NAME, latitudeIndex);

    // Ingest data
    ingestData(ds);

    /////////////////////////////////////////////////////
    // BBOX ALT with No Filter
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(String.format("SELECT BBOX(%s) FROM %s", ALT, TYPE_NAME))) {
      assertEquals(1, results.columnCount());
      assertTrue(results.hasNext());
      assertTrue(Envelope.class.isAssignableFrom(results.columnType(0)));
      assertEquals(new Envelope(-63, 63, -63, 63), results.next().columnValue(0));
      assertFalse(results.hasNext());
    }

    /////////////////////////////////////////////////////
    // BBOX ALT with latitude filter
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(String.format("SELECT BBOX(%s) FROM %s WHERE %s > 0", ALT, TYPE_NAME, LATITUDE))) {
      assertEquals(1, results.columnCount());
      assertTrue(results.hasNext());
      assertTrue(Envelope.class.isAssignableFrom(results.columnType(0)));
      assertEquals(new Envelope(1, 63, -63, -1), results.next().columnValue(0));
      assertFalse(results.hasNext());
    }

    /////////////////////////////////////////////////////
    // COUNT ALT with No Filter
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(String.format("SELECT COUNT(%s) FROM %s", ALT, TYPE_NAME))) {
      assertEquals(1, results.columnCount());
      assertTrue(results.hasNext());
      assertTrue(Long.class.isAssignableFrom(results.columnType(0)));
      assertEquals(TOTAL_FEATURES / 2, ((Long) results.next().columnValue(0)).intValue());
      assertFalse(results.hasNext());
    }

    /////////////////////////////////////////////////////
    // COUNT ALT with latitude filter
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format("SELECT COUNT(%s) FROM %s WHERE %s > 0", ALT, TYPE_NAME, LATITUDE))) {
      assertEquals(1, results.columnCount());
      assertTrue(results.hasNext());
      assertTrue(Long.class.isAssignableFrom(results.columnType(0)));
      assertEquals(TOTAL_FEATURES / 4, ((Long) results.next().columnValue(0)).intValue());
      assertFalse(results.hasNext());
    }

    /////////////////////////////////////////////////////
    // SUM INTEGER with no filter
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(String.format("SELECT SUM(%s) FROM %s", INTEGER, TYPE_NAME, LATITUDE))) {
      assertEquals(1, results.columnCount());
      assertTrue(results.hasNext());
      assertTrue(BigDecimal.class.isAssignableFrom(results.columnType(0)));
      assertEquals(-64, ((BigDecimal) results.next().columnValue(0)).intValue());
      assertFalse(results.hasNext());
    }

    /////////////////////////////////////////////////////
    // SUM INTEGER with latitude filter
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format("SELECT SUM(%s) FROM %s WHERE %s > 0", INTEGER, TYPE_NAME, LATITUDE))) {
      assertEquals(1, results.columnCount());
      assertTrue(results.hasNext());
      assertTrue(BigDecimal.class.isAssignableFrom(results.columnType(0)));
      int expected = 0;
      for (int i = 1; i < TOTAL_FEATURES / 2; i++) {
        expected += i;
      }
      assertEquals(expected, ((BigDecimal) results.next().columnValue(0)).intValue());
      assertFalse(results.hasNext());
    }

    /////////////////////////////////////////////////////
    // MIN INTEGER with no filter
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(String.format("SELECT MIN(%s) FROM %s", INTEGER, TYPE_NAME, LATITUDE))) {
      assertEquals(1, results.columnCount());
      assertTrue(results.hasNext());
      assertTrue(BigDecimal.class.isAssignableFrom(results.columnType(0)));
      assertEquals(-64, ((BigDecimal) results.next().columnValue(0)).intValue());
      assertFalse(results.hasNext());
    }

    /////////////////////////////////////////////////////
    // MIN INTEGER with latitude filter
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format("SELECT MIN(%s) FROM %s WHERE %s > 0", INTEGER, TYPE_NAME, LATITUDE))) {
      assertEquals(1, results.columnCount());
      assertTrue(results.hasNext());
      assertTrue(BigDecimal.class.isAssignableFrom(results.columnType(0)));
      assertEquals(1, ((BigDecimal) results.next().columnValue(0)).intValue());
      assertFalse(results.hasNext());
    }

    /////////////////////////////////////////////////////
    // MAX INTEGER with no filter
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(String.format("SELECT MAX(%s) FROM %s", INTEGER, TYPE_NAME, LATITUDE))) {
      assertEquals(1, results.columnCount());
      assertTrue(results.hasNext());
      assertTrue(BigDecimal.class.isAssignableFrom(results.columnType(0)));
      assertEquals(63, ((BigDecimal) results.next().columnValue(0)).intValue());
      assertFalse(results.hasNext());
    }

    /////////////////////////////////////////////////////
    // MAX INTEGER with latitude filter
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format("SELECT MAX(%s) FROM %s WHERE %s < 0", INTEGER, TYPE_NAME, LATITUDE))) {
      assertEquals(1, results.columnCount());
      assertTrue(results.hasNext());
      assertTrue(BigDecimal.class.isAssignableFrom(results.columnType(0)));
      assertEquals(-1, ((BigDecimal) results.next().columnValue(0)).intValue());
      assertFalse(results.hasNext());
    }

    /////////////////////////////////////////////////////
    // Composite aggregation with no filter
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT BBOX(%s), MIN(%s), MAX(%s), SUM(%s) FROM %s",
                ALT,
                INTEGER,
                INTEGER,
                INTEGER,
                TYPE_NAME,
                LATITUDE))) {
      assertEquals(4, results.columnCount());
      assertTrue(results.hasNext());
      assertTrue(Envelope.class.isAssignableFrom(results.columnType(0)));
      assertTrue(BigDecimal.class.isAssignableFrom(results.columnType(1)));
      assertTrue(BigDecimal.class.isAssignableFrom(results.columnType(2)));
      assertTrue(BigDecimal.class.isAssignableFrom(results.columnType(3)));
      final Result next = results.next();
      assertEquals(new Envelope(-63, 63, -63, 63), next.columnValue(0));
      assertEquals(-64, ((BigDecimal) next.columnValue(1)).intValue());
      assertEquals(63, ((BigDecimal) next.columnValue(2)).intValue());
      assertEquals(-64, ((BigDecimal) next.columnValue(3)).intValue());
      assertFalse(results.hasNext());
    }

    /////////////////////////////////////////////////////
    // Composite aggregation with latitude filter
    /////////////////////////////////////////////////////
    try (final ResultSet results =
        ds.query(
            String.format(
                "SELECT BBOX(%s), MIN(%s), MAX(%s), SUM(%s) FROM %s WHERE %s > 0",
                ALT,
                INTEGER,
                INTEGER,
                INTEGER,
                TYPE_NAME,
                LATITUDE))) {
      assertEquals(4, results.columnCount());
      assertTrue(results.hasNext());
      assertTrue(Envelope.class.isAssignableFrom(results.columnType(0)));
      assertTrue(BigDecimal.class.isAssignableFrom(results.columnType(1)));
      assertTrue(BigDecimal.class.isAssignableFrom(results.columnType(2)));
      assertTrue(BigDecimal.class.isAssignableFrom(results.columnType(3)));
      final Result next = results.next();
      assertEquals(new Envelope(1, 63, -63, -1), next.columnValue(0));
      assertEquals(1, ((BigDecimal) next.columnValue(1)).intValue());
      assertEquals(63, ((BigDecimal) next.columnValue(2)).intValue());
      int expected = 0;
      for (int i = 1; i < TOTAL_FEATURES / 2; i++) {
        expected += i;
      }
      assertEquals(expected, ((BigDecimal) next.columnValue(3)).intValue());
      assertFalse(results.hasNext());
    }
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStore;
  }
}
