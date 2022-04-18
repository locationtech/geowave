/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.basic;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.index.VectorTextIndexEntryConverter;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.text.EnumIndexStrategy;
import org.locationtech.geowave.core.index.text.EnumSearch;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.CustomIndex;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Streams;

@RunWith(GeoWaveITRunner.class)
@GeoWaveTestStore(
    value = {
        GeoWaveStoreType.ACCUMULO,
        GeoWaveStoreType.BIGTABLE,
        GeoWaveStoreType.HBASE,
        GeoWaveStoreType.CASSANDRA,
        GeoWaveStoreType.DYNAMODB,
        GeoWaveStoreType.KUDU,
        GeoWaveStoreType.REDIS,
        GeoWaveStoreType.ROCKSDB,
        GeoWaveStoreType.FILESYSTEM})
public class GeoWaveEnumIndexIT extends AbstractGeoWaveIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveEnumIndexIT.class);
  private static long startMillis;
  protected static final String TEST_STATE_CAPITALS_RESOURCE_PATH =
      TestUtils.TEST_RESOURCE_PACKAGE + "/query/stateCapitals.csv";
  private static String TEST_ENUM_INDEX_NAME = "TestEnumIdx";
  protected DataStorePluginOptions dataStoreOptions;
  private static final String TYPE_NAME = "capitals";
  private static final String TIMEZONE_ATTR_NAME = "timezone";

  @BeforeClass
  public static void startTimer() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*    RUNNING GeoWaveEnumIndexIT       *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @AfterClass
  public static void reportTest() {
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*      FINISHED GeoWaveEnumIndexIT      *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                  *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStoreOptions;
  }

  private static SimpleFeatureType initType() {
    final SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();
    b.setName(TYPE_NAME);
    b.add("geom", Point.class);
    b.add("state", String.class);
    b.add("city", String.class);
    b.add("year", Integer.class);
    b.add("area", Float.class);
    b.add("population", Integer.class);
    b.add("notes", String.class);
    b.add(TIMEZONE_ATTR_NAME, String.class);
    return b.buildFeatureType();
  }

  private void ingest(final String attrNameToIndex, final boolean includeSpatial)
      throws IOException {
    final DataStore ds = dataStoreOptions.createDataStore();
    final SimpleFeatureType type = initType();

    ds.addType(
        new FeatureDataAdapter(type),
        createEnumIndex(
            new String[] {"Eastern", "Central", "Mountain", "Pacific"},
            type.indexOf(attrNameToIndex)));
    if (includeSpatial) {
      ds.addIndex(TYPE_NAME, DimensionalityType.SPATIAL.getDefaultIndices());
    }
    try (
        InputStreamReader reader =
            new InputStreamReader(
                GeoWaveTextIndexIT.class.getClassLoader().getResourceAsStream(
                    TEST_STATE_CAPITALS_RESOURCE_PATH));
        Writer<SimpleFeature> w = ds.createWriter(TYPE_NAME)) {
      Streams.stream(
          CSVFormat.DEFAULT.withHeader(
              "state",
              "city",
              "lon",
              "lat",
              "year",
              "area",
              "population",
              "notes").parse(reader)).map(r -> toFeature(r, type)).forEach(w::write);
    }
  }

  private static SimpleFeature toFeature(final CSVRecord r, final SimpleFeatureType t) {
    final SimpleFeatureBuilder b = new SimpleFeatureBuilder(t);
    final double longitude = Double.parseDouble(r.get("lon"));
    b.add(
        GeometryUtils.GEOMETRY_FACTORY.createPoint(
            new Coordinate(longitude, Double.parseDouble(r.get("lat")))));
    b.add(r.get("state"));
    b.add(r.get("city"));
    b.add(Integer.parseInt(r.get("year")));
    b.add(Double.parseDouble(r.get("area")));
    b.add(Integer.parseInt(r.get("population")));
    b.add(r.get("notes"));
    b.add(getTimeZone(longitude));
    return b.buildFeature(UUID.randomUUID().toString());
  }

  private static String getTimeZone(final double longitude) {
    if (longitude < -117) {
      return "Pacific";
    } else if (longitude < -101) {
      return "Mountain";
    } else if (longitude < -86.2) {
      return "Central";
    }
    return "Eastern";
  }

  private static Index createEnumIndex(final String[] enumValues, final int attributeIndex) {
    return new CustomIndex<>(
        new EnumIndexStrategy<>(new VectorTextIndexEntryConverter(attributeIndex), enumValues),
        TEST_ENUM_INDEX_NAME);
  }

  @Test
  public void testEnumIndex() throws IOException {
    assertResults(false);
  }

  @Test
  public void testEnumIndexWithSpatial() throws IOException {
    assertResults(true);
  }

  private void assertResults(final boolean includeSpatial) throws IOException {
    ingest(TIMEZONE_ATTR_NAME, includeSpatial);

    getResults(new EnumSearch("Pacific"));
    getResults(new EnumSearch("Mountain"));
    getResults(new EnumSearch("Central"));
    getResults(new EnumSearch("Eastern"));
    // intentionally make sure a bogus term just returns no results
    getResults(new EnumSearch("Atlantic"));
  }

  private void getResults(final EnumSearch search) {
    final DataStore ds = dataStoreOptions.createDataStore();
    final Set<String> expectedResults = new HashSet<>();
    final QueryBuilder queryBldr = QueryBuilder.newBuilder().addTypeName(TYPE_NAME);
    // query everything and apply manual filtering
    final AtomicInteger everythingResults = new AtomicInteger(0);
    try (final CloseableIterator<SimpleFeature> it = ds.query((Query) queryBldr.build())) {
      it.forEachRemaining(f -> {
        final String timezone = f.getAttribute(TIMEZONE_ATTR_NAME).toString();
        if (search.getSearchTerm().equals(timezone)) {
          expectedResults.add(f.getID());
        }
        everythingResults.incrementAndGet();
      });
    }
    // ensure that the full set of results exceeds the expected results (ie. that we successfully
    // queried everything in the previous step)
    Assert.assertTrue(everythingResults.get() > expectedResults.size());
    LOGGER.info("Expecting '" + expectedResults.size() + "' in timezone " + search.getSearchTerm());
    queryBldr.indexName(TEST_ENUM_INDEX_NAME);
    try (final CloseableIterator<SimpleFeature> it =
        ds.query(
            (Query) queryBldr.constraints(
                queryBldr.constraintsFactory().customConstraints(search)).build())) {
      it.forEachRemaining(f -> {
        final String timezone = f.getAttribute(TIMEZONE_ATTR_NAME).toString();
        Assert.assertEquals(search.getSearchTerm(), timezone);
        expectedResults.remove(f.getID());
      });
    }
    Assert.assertTrue(expectedResults.isEmpty());
  }

}
