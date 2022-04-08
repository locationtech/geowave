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
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
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
import org.locationtech.geowave.core.index.text.CaseSensitivity;
import org.locationtech.geowave.core.index.text.TextIndexStrategy;
import org.locationtech.geowave.core.index.text.TextSearch;
import org.locationtech.geowave.core.index.text.TextSearchType;
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
public class GeoWaveTextIndexIT extends AbstractGeoWaveIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveTextIndexIT.class);
  private static long startMillis;
  protected static final String TEST_STATE_CAPITALS_RESOURCE_PATH =
      TestUtils.TEST_RESOURCE_PACKAGE + "/query/stateCapitals.csv";
  private static String TEST_TEXT_INDEX_NAME = "TestTextIdx";
  protected DataStorePluginOptions dataStoreOptions;
  private static final String TYPE_NAME = "capitals";
  private static final String CITY_ATTR_NAME = "city";
  private static final String NOTES_ATTR_NAME = "notes";

  @BeforeClass
  public static void startTimer() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*    RUNNING GeoWaveTextIndexIT       *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @AfterClass
  public static void reportTest() {
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*      FINISHED GeoWaveTextIndexIT      *");
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
    b.add(CITY_ATTR_NAME, String.class);
    b.add("year", Integer.class);
    b.add("area", Float.class);
    b.add("population", Integer.class);
    b.add(NOTES_ATTR_NAME, String.class);
    return b.buildFeatureType();
  }

  private void ingest(
      final String attrNameToIndex,
      final EnumSet<TextSearchType> supportedSearchTypes,
      final EnumSet<CaseSensitivity> supportedCaseSensitivity,
      final int nCharacterGrams,
      final boolean includeSpatial) throws IOException {
    final DataStore ds = dataStoreOptions.createDataStore();
    final SimpleFeatureType type = initType();

    ds.addType(
        new FeatureDataAdapter(type),
        createTextIndex(
            supportedSearchTypes,
            supportedCaseSensitivity,
            nCharacterGrams,
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
    b.add(
        GeometryUtils.GEOMETRY_FACTORY.createPoint(
            new Coordinate(Double.parseDouble(r.get("lon")), Double.parseDouble(r.get("lat")))));
    b.add(r.get("state"));
    b.add(r.get("city"));
    b.add(Integer.parseInt(r.get("year")));
    b.add(Double.parseDouble(r.get("area")));
    b.add(Integer.parseInt(r.get("population")));
    b.add(r.get("notes"));
    return b.buildFeature(UUID.randomUUID().toString());
  }

  private static Index createTextIndex(
      final EnumSet<TextSearchType> supportedSearchTypes,
      final EnumSet<CaseSensitivity> supportedCaseSensitivity,
      final int nCharacterGrams,
      final int attributeIndex) {
    return new CustomIndex<>(
        new TextIndexStrategy<>(
            supportedSearchTypes,
            supportedCaseSensitivity,
            nCharacterGrams,
            new VectorTextIndexEntryConverter(attributeIndex)),
        TEST_TEXT_INDEX_NAME);
  }

  @Test
  public void testAllIndexTypes() throws IOException {
    assertResults(
        EnumSet.allOf(TextSearchType.class),
        EnumSet.allOf(CaseSensitivity.class),
        3,
        false);
  }

  @Test
  public void testAllIndexTypesWithSpatial() throws IOException {
    assertResults(
        EnumSet.allOf(TextSearchType.class),
        EnumSet.allOf(CaseSensitivity.class),
        4,
        true);
  }

  @Test
  public void testOnlyCaseSensitive() throws IOException {
    assertResults(
        EnumSet.allOf(TextSearchType.class),
        EnumSet.of(CaseSensitivity.CASE_SENSITIVE),
        5,
        false);
  }

  @Test
  public void testOnlyCaseInSensitive() throws IOException {
    assertResults(
        EnumSet.allOf(TextSearchType.class),
        EnumSet.of(CaseSensitivity.CASE_INSENSITIVE),
        5,
        false);
  }

  @Test
  public void testOnlyCaseInSensitiveAndContains() throws IOException {
    assertResults(
        EnumSet.of(TextSearchType.CONTAINS),
        EnumSet.of(CaseSensitivity.CASE_INSENSITIVE),
        5,
        false);
  }

  @Test
  public void testOnlyCaseSensitiveAndContains() throws IOException {
    assertResults(
        EnumSet.of(TextSearchType.CONTAINS),
        EnumSet.of(CaseSensitivity.CASE_SENSITIVE),
        1,
        false);
  }

  @Test
  public void testOnlyCaseSensitiveAndBeginsAndEndsWith() throws IOException {
    assertResults(
        EnumSet.of(TextSearchType.BEGINS_WITH, TextSearchType.ENDS_WITH),
        EnumSet.of(CaseSensitivity.CASE_INSENSITIVE),
        5,
        false);
  }

  @Test
  public void testOnlyExactMatchAndEndsWith() throws IOException {
    assertResults(
        EnumSet.of(TextSearchType.EXACT_MATCH, TextSearchType.ENDS_WITH),
        EnumSet.allOf(CaseSensitivity.class),
        5,
        false);
  }

  @Test
  public void testOnlyExactMatchAndStartsWithCaseSensitive() throws IOException {
    assertResults(
        EnumSet.of(TextSearchType.EXACT_MATCH, TextSearchType.BEGINS_WITH),
        EnumSet.of(CaseSensitivity.CASE_SENSITIVE),
        5,
        false);
  }

  private void assertResults(
      final EnumSet<TextSearchType> supportedSearchTypes,
      final EnumSet<CaseSensitivity> supportedCaseSensitivity,
      final int nCharacterGrams,
      final boolean includeSpatial) throws IOException {
    ingest(CITY_ATTR_NAME, supportedSearchTypes, supportedCaseSensitivity, 3, includeSpatial);
    // start by exercising various "begins with" searches
    Set<String> results;
    if (supportedSearchTypes.contains(TextSearchType.BEGINS_WITH)
        && supportedCaseSensitivity.contains(CaseSensitivity.CASE_SENSITIVE)) {
      results =
          getResults(
              new TextSearch(TextSearchType.BEGINS_WITH, CaseSensitivity.CASE_SENSITIVE, "C"));

      // there are 6 capitals beginning with "C"
      Assert.assertEquals(6, results.size());
      for (final String r : results) {
        Assert.assertTrue(r.startsWith("C"));
      }
    }
    // next make sure it works case insensitive

    if (supportedSearchTypes.contains(TextSearchType.BEGINS_WITH)
        && supportedCaseSensitivity.contains(CaseSensitivity.CASE_INSENSITIVE)) {
      results =
          getResults(
              new TextSearch(
                  TextSearchType.BEGINS_WITH,
                  CaseSensitivity.CASE_INSENSITIVE,
                  "caRson c"));
      Assert.assertEquals(1, results.size());
      Assert.assertTrue(results.iterator().next().startsWith("Carson C"));
    }
    // next make sure it doesn't return results for lower case when case sensitive

    if (supportedSearchTypes.contains(TextSearchType.BEGINS_WITH)
        && supportedCaseSensitivity.contains(CaseSensitivity.CASE_SENSITIVE)) {
      results =
          getResults(
              new TextSearch(TextSearchType.BEGINS_WITH, CaseSensitivity.CASE_SENSITIVE, "c"));
      Assert.assertEquals(0, results.size());
    }
    // now move on to exercising some "ends with" constraints

    if (supportedSearchTypes.contains(TextSearchType.ENDS_WITH)
        && supportedCaseSensitivity.contains(CaseSensitivity.CASE_SENSITIVE)) {
      results =
          getResults(
              new TextSearch(TextSearchType.ENDS_WITH, CaseSensitivity.CASE_SENSITIVE, " City"));
      // there are 4 capitals that end with " City"
      Assert.assertEquals(4, results.size());
      for (final String r : results) {
        Assert.assertTrue(r.endsWith(" City"));
      }
    }

    if (supportedSearchTypes.contains(TextSearchType.ENDS_WITH)
        && supportedCaseSensitivity.contains(CaseSensitivity.CASE_INSENSITIVE)) {
      // just make sure it respects case sensitivity
      results =
          getResults(
              new TextSearch(TextSearchType.ENDS_WITH, CaseSensitivity.CASE_INSENSITIVE, " CiTy"));
      // there are 4 capitals that end with " City"
      Assert.assertEquals(4, results.size());
      for (final String r : results) {
        Assert.assertTrue(r.endsWith(" City"));
      }
    }

    if (supportedSearchTypes.contains(TextSearchType.CONTAINS)
        && supportedCaseSensitivity.contains(CaseSensitivity.CASE_SENSITIVE)) {
      // now move on to exercising some "contains" constraints
      results =
          getResults(new TextSearch(TextSearchType.CONTAINS, CaseSensitivity.CASE_SENSITIVE, "nt"));
      // there are 7 capitals that contain "nt"
      Assert.assertEquals(7, results.size());
      for (final String r : results) {
        Assert.assertTrue(r.contains("nt"));
      }
    }

    if (supportedSearchTypes.contains(TextSearchType.CONTAINS)
        && supportedCaseSensitivity.contains(CaseSensitivity.CASE_INSENSITIVE)) {
      results =
          getResults(
              new TextSearch(TextSearchType.CONTAINS, CaseSensitivity.CASE_INSENSITIVE, "Nt"));
      Assert.assertEquals(7, results.size());
      for (final String r : results) {
        Assert.assertTrue(r.contains("nt"));
      }
    }

    if (supportedSearchTypes.contains(TextSearchType.CONTAINS)
        && supportedCaseSensitivity.contains(CaseSensitivity.CASE_SENSITIVE)) {
      results =
          getResults(new TextSearch(TextSearchType.CONTAINS, CaseSensitivity.CASE_SENSITIVE, "Nt"));
      Assert.assertEquals(0, results.size());
    }

    if (supportedSearchTypes.contains(TextSearchType.CONTAINS)
        && supportedCaseSensitivity.contains(CaseSensitivity.CASE_INSENSITIVE)) {
      results =
          getResults(
              new TextSearch(TextSearchType.CONTAINS, CaseSensitivity.CASE_INSENSITIVE, " Cit"));
      // there are 4 capitals that contain with " Cit"
      Assert.assertEquals(4, results.size());
      for (final String r : results) {
        Assert.assertTrue(r.contains(" Cit"));
      }
    }

    if (supportedSearchTypes.contains(TextSearchType.CONTAINS)
        && supportedCaseSensitivity.contains(CaseSensitivity.CASE_INSENSITIVE)) {
      results =
          getResults(
              new TextSearch(TextSearchType.CONTAINS, CaseSensitivity.CASE_INSENSITIVE, " CitY"));
      // there are 4 capitals that contain with " City"
      Assert.assertEquals(4, results.size());
      for (final String r : results) {
        Assert.assertTrue(r.contains(" City"));
      }
    }

    if (supportedSearchTypes.contains(TextSearchType.CONTAINS)
        && supportedCaseSensitivity.contains(CaseSensitivity.CASE_SENSITIVE)) {
      results =
          getResults(new TextSearch(TextSearchType.CONTAINS, CaseSensitivity.CASE_SENSITIVE, "C"));
      // there are 10 capitals that contain "C"
      Assert.assertEquals(9, results.size());
      for (final String r : results) {
        Assert.assertTrue(r.contains("C"));
      }
    }

    if (supportedSearchTypes.contains(TextSearchType.CONTAINS)
        && supportedCaseSensitivity.contains(CaseSensitivity.CASE_SENSITIVE)) {
      results =
          getResults(
              new TextSearch(TextSearchType.CONTAINS, CaseSensitivity.CASE_SENSITIVE, "ciT"));
      Assert.assertEquals(0, results.size());
    }

    if (supportedSearchTypes.contains(TextSearchType.EXACT_MATCH)
        && supportedCaseSensitivity.contains(CaseSensitivity.CASE_SENSITIVE)) {
      results =
          getResults(
              new TextSearch(
                  TextSearchType.EXACT_MATCH,
                  CaseSensitivity.CASE_SENSITIVE,
                  "Salt Lake City"));
      Assert.assertEquals(1, results.size());
      Assert.assertTrue(results.iterator().next().equals("Salt Lake City"));
      results =
          getResults(
              new TextSearch(
                  TextSearchType.EXACT_MATCH,
                  CaseSensitivity.CASE_SENSITIVE,
                  "Salt Lake Cit"));
      Assert.assertEquals(0, results.size());
    }

    if (supportedSearchTypes.contains(TextSearchType.EXACT_MATCH)
        && supportedCaseSensitivity.contains(CaseSensitivity.CASE_INSENSITIVE)) {
      results =
          getResults(
              new TextSearch(
                  TextSearchType.EXACT_MATCH,
                  CaseSensitivity.CASE_INSENSITIVE,
                  "salt Lake city"));
      Assert.assertEquals(1, results.size());
      Assert.assertTrue(results.iterator().next().equals("Salt Lake City"));
    }

    if (supportedSearchTypes.contains(TextSearchType.EXACT_MATCH)
        && supportedCaseSensitivity.contains(CaseSensitivity.CASE_SENSITIVE)) {
      results =
          getResults(
              new TextSearch(
                  TextSearchType.EXACT_MATCH,
                  CaseSensitivity.CASE_SENSITIVE,
                  "salt Lake city"));
      Assert.assertEquals(0, results.size());
    }
  }

  private Set<String> getResults(final TextSearch search) {
    final DataStore ds = dataStoreOptions.createDataStore();
    final Set<String> results = new HashSet<>();
    final QueryBuilder queryBldr =
        QueryBuilder.newBuilder().addTypeName(TYPE_NAME).indexName(TEST_TEXT_INDEX_NAME);
    try (final CloseableIterator<SimpleFeature> it =
        ds.query(
            (Query) queryBldr.constraints(
                queryBldr.constraintsFactory().customConstraints(search)).build())) {
      it.forEachRemaining(f -> {
        final String cityName = f.getAttribute(CITY_ATTR_NAME).toString();
        Assert.assertFalse(results.contains(cityName));
        results.add(cityName);
      });
    }
    return results;
  }

}
