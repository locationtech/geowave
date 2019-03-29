/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.util.FeatureTranslatingIterator;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.query.ExplicitSpatialQuery;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveIT;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
public class AttributesSubsetQueryIT extends AbstractGeoWaveIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(AttributesSubsetQueryIT.class);

  private static SimpleFeatureType simpleFeatureType;
  private static FeatureDataAdapter dataAdapter;

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB})
  protected DataStorePluginOptions dataStore;

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStore;
  }

  // constants for attributes of SimpleFeatureType
  private static final String CITY_ATTRIBUTE = "city";
  private static final String STATE_ATTRIBUTE = "state";
  private static final String POPULATION_ATTRIBUTE = "population";
  private static final String LAND_AREA_ATTRIBUTE = "landArea";
  private static final String GEOMETRY_ATTRIBUTE = "geometry";

  private static Index index =
      new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());

  private static final Collection<String> ALL_ATTRIBUTES =
      Arrays.asList(
          CITY_ATTRIBUTE,
          STATE_ATTRIBUTE,
          POPULATION_ATTRIBUTE,
          LAND_AREA_ATTRIBUTE,
          GEOMETRY_ATTRIBUTE);

  // points used to construct bounding box for queries
  private static final Coordinate GUADALAJARA = new Coordinate(-103.3500, 20.6667);
  private static final Coordinate ATLANTA = new Coordinate(-84.3900, 33.7550);

  private final QueryConstraints spatialQuery =
      new ExplicitSpatialQuery(
          GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(GUADALAJARA, ATLANTA)));

  private static long startMillis;

  @BeforeClass
  public static void setupData() throws IOException {
    simpleFeatureType = getSimpleFeatureType();

    dataAdapter = new FeatureDataAdapter(simpleFeatureType);

    dataAdapter.init(index);

    startMillis = System.currentTimeMillis();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*    RUNNING AttributesSubsetQueryIT    *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @AfterClass
  public static void reportTest() {
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*   FINISHED AttributesSubsetQueryIT    *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @Test
  public void testNoFiltering() throws IOException {

    final CloseableIterator<SimpleFeature> results =
        (CloseableIterator) dataStore.createDataStore().query(
            QueryBuilder.newBuilder().addTypeName(dataAdapter.getTypeName()).indexName(
                TestUtils.DEFAULT_SPATIAL_INDEX.getName()).constraints(spatialQuery).build());

    // query expects to match 3 cities from Texas, which should each contain
    // non-null values for each SimpleFeature attribute
    verifyResults(results, 3, ALL_ATTRIBUTES);
  }

  @Test
  public void testServerSideFiltering() throws IOException {

    QueryBuilder<?, ?> bldr =
        QueryBuilder.newBuilder().addTypeName(dataAdapter.getTypeName()).indexName(
            TestUtils.DEFAULT_SPATIAL_INDEX.getName()).subsetFields(
                dataAdapter.getTypeName(),
                CITY_ATTRIBUTE,
                GEOMETRY_ATTRIBUTE);

    CloseableIterator<SimpleFeature> results =
        (CloseableIterator<SimpleFeature>) dataStore.createDataStore().query(
            bldr.constraints(spatialQuery).build());

    // query expects to match 3 cities from Texas, which should each contain
    // non-null values for a subset of attributes (city) and nulls for the
    // rest
    List<String> expectedAttributes = Arrays.asList(CITY_ATTRIBUTE, GEOMETRY_ATTRIBUTE); // always
    // included
    verifyResults(results, 3, expectedAttributes);
    bldr =
        QueryBuilder.newBuilder().addTypeName(dataAdapter.getTypeName()).indexName(
            TestUtils.DEFAULT_SPATIAL_INDEX.getName()).subsetFields(
                dataAdapter.getTypeName(),
                GEOMETRY_ATTRIBUTE);
    // now try just geometry
    results =
        (CloseableIterator<SimpleFeature>) dataStore.createDataStore().query(
            bldr.constraints(spatialQuery).build());

    // query expects to match 3 cities from Texas, which should each contain
    // non-null values for geometry and null values for all other attributes
    expectedAttributes = Arrays.asList(GEOMETRY_ATTRIBUTE); // always
    // included
    verifyResults(results, 3, expectedAttributes);
  }

  @Test
  public void testClientSideFiltering() throws IOException {

    final List<String> attributesSubset = Arrays.asList(CITY_ATTRIBUTE, POPULATION_ATTRIBUTE);

    final CloseableIterator<SimpleFeature> results =
        (CloseableIterator) dataStore.createDataStore().query(
            QueryBuilder.newBuilder().addTypeName(dataAdapter.getTypeName()).indexName(
                TestUtils.DEFAULT_SPATIAL_INDEX.getName()).constraints(spatialQuery).build());

    // query expects to match 3 cities from Texas, which should each contain
    // non-null values for a subset of attributes (city, population) and
    // nulls for the rest
    verifyResults(
        // performs filtering client side
        new FeatureTranslatingIterator(simpleFeatureType, attributesSubset, results),
        3,
        attributesSubset);
  }

  private void verifyResults(
      final CloseableIterator<SimpleFeature> results,
      final int numExpectedResults,
      final Collection<String> attributesExpected) throws IOException {

    int numResults = 0;
    SimpleFeature currentFeature;
    Object currentAttributeValue;

    while (results.hasNext()) {

      currentFeature = results.next();
      numResults++;

      for (final String currentAttribute : ALL_ATTRIBUTES) {

        currentAttributeValue = currentFeature.getAttribute(currentAttribute);

        if (attributesExpected.contains(currentAttribute)) {
          Assert.assertNotNull(
              "Expected non-null " + currentAttribute + " value!",
              currentAttributeValue);
        } else {
          Assert.assertNull("Expected null " + currentAttribute + " value!", currentAttributeValue);
        }
      }
    }

    results.close();

    Assert.assertEquals("Unexpected number of query results", numExpectedResults, numResults);
  }

  private static SimpleFeatureType getSimpleFeatureType() {

    SimpleFeatureType type = null;

    try {
      type =
          DataUtilities.createType(
              "testCityData",
              CITY_ATTRIBUTE
                  + ":String,"
                  + STATE_ATTRIBUTE
                  + ":String,"
                  + POPULATION_ATTRIBUTE
                  + ":Double,"
                  + LAND_AREA_ATTRIBUTE
                  + ":Double,"
                  + GEOMETRY_ATTRIBUTE
                  + ":Geometry");
    } catch (final SchemaException e) {
      LOGGER.error("Unable to create SimpleFeatureType", e);
    }

    return type;
  }

  @Before
  public void ingestSampleData() throws IOException {

    LOGGER.info("Ingesting canned data...");
    final DataStore store = dataStore.createDataStore();
    store.addType(dataAdapter, TestUtils.DEFAULT_SPATIAL_INDEX);
    try (Writer writer = store.createWriter(dataAdapter.getTypeName())) {
      for (final SimpleFeature sf : buildCityDataSet()) {
        writer.write(sf);
      }
    }
    LOGGER.info("Ingest complete.");
  }

  @After
  public void deleteSampleData() throws IOException {

    LOGGER.info("Deleting canned data...");
    TestUtils.deleteAll(dataStore);
    LOGGER.info("Delete complete.");
  }

  private static List<SimpleFeature> buildCityDataSet() {

    final List<SimpleFeature> points = new ArrayList<>();

    // http://en.wikipedia.org/wiki/List_of_United_States_cities_by_population
    points.add(
        buildSimpleFeature(
            "New York",
            "New York",
            8405837,
            302.6,
            new Coordinate(-73.9385, 40.6643)));
    points.add(
        buildSimpleFeature(
            "Los Angeles",
            "California",
            3884307,
            468.7,
            new Coordinate(-118.4108, 34.0194)));
    points.add(
        buildSimpleFeature(
            "Chicago",
            "Illinois",
            2718782,
            227.6,
            new Coordinate(-87.6818, 41.8376)));
    points.add(
        buildSimpleFeature("Houston", "Texas", 2195914, 599.6, new Coordinate(-95.3863, 29.7805)));
    points.add(
        buildSimpleFeature(
            "Philadelphia",
            "Pennsylvania",
            1553165,
            134.1,
            new Coordinate(-75.1333, 40.0094)));
    points.add(
        buildSimpleFeature(
            "Phoenix",
            "Arizona",
            1513367,
            516.7,
            new Coordinate(-112.088, 33.5722)));
    points.add(
        buildSimpleFeature(
            "San Antonio",
            "Texas",
            1409019,
            460.9,
            new Coordinate(-98.5251, 29.4724)));
    points.add(
        buildSimpleFeature(
            "San Diego",
            "California",
            1355896,
            325.2,
            new Coordinate(-117.135, 32.8153)));
    points.add(
        buildSimpleFeature("Dallas", "Texas", 1257676, 340.5, new Coordinate(-96.7967, 32.7757)));
    points.add(
        buildSimpleFeature(
            "San Jose",
            "California",
            998537,
            176.5,
            new Coordinate(-121.8193, 37.2969)));

    return points;
  }

  private static SimpleFeature buildSimpleFeature(
      final String city,
      final String state,
      final double population,
      final double landArea,
      final Coordinate coordinate) {

    final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(simpleFeatureType);

    builder.set(CITY_ATTRIBUTE, city);
    builder.set(STATE_ATTRIBUTE, state);
    builder.set(POPULATION_ATTRIBUTE, population);
    builder.set(LAND_AREA_ATTRIBUTE, landArea);
    builder.set(GEOMETRY_ATTRIBUTE, GeometryUtils.GEOMETRY_FACTORY.createPoint(coordinate));

    return builder.buildFeature(UUID.randomUUID().toString());
  }
}
