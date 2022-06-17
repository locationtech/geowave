/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.basic;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.function.IntPredicate;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.geotime.index.api.SpatialIndexBuilder;
import org.locationtech.geowave.core.geotime.index.api.SpatialTemporalIndexBuilder;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.CustomIndexStrategy;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.StatisticQueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.CustomIndex;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jersey.repackaged.com.google.common.collect.Iterators;

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
public class GeoWaveCustomIndexIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveCustomIndexIT.class);
  private static long startMillis;
  private static String TEST_ENUM_INDEX_NAME = "TestEnumIdx";
  protected DataStorePluginOptions dataStoreOptions;

  private static enum TestEnum {
    A(i -> (i % 2) == 0), B(i -> (i % 3) == 0), C(i -> (i % 5) == 0), NOT_A(i -> (i + 1) % 2 == 0);

    private final IntPredicate ingestLogic;

    private TestEnum(final IntPredicate ingestLogic) {
      this.ingestLogic = ingestLogic;
    }

    public boolean test(final int value) {
      return ingestLogic.test(value);
    }
  }

  @BeforeClass
  public static void startTimer() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*    RUNNING GeoWaveCustomIndexIT       *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @AfterClass
  public static void reportTest() {
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*      FINISHED GeoWaveCustomIndexIT    *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                  *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  public void ingest(boolean addSpatialTemporal) {
    final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();
    final GeotoolsFeatureDataAdapter fda = SimpleIngest.createDataAdapter(sft);
    final List<SimpleFeature> features =
        getGriddedTemporalFeaturesWithEnumString(new SimpleFeatureBuilder(sft), 6001);
    TestUtils.deleteAll(dataStoreOptions);
    final DataStore dataStore = dataStoreOptions.createDataStore();
    if (addSpatialTemporal) {
      dataStore.addType(
          fda,
          new SpatialIndexBuilder().createIndex(),
          new SpatialTemporalIndexBuilder().createIndex(),
          getTestEnumIndex());
    } else {
      dataStore.addType(fda, getTestEnumIndex());
    }
    try (Writer<SimpleFeature> writer = dataStore.createWriter(sft.getTypeName())) {
      features.stream().forEach(f -> writer.write(f));
    }
  }

  @After
  public void cleanup() {
    TestUtils.deleteAll(dataStoreOptions);
  }

  @Test
  public void testCustomIndexingWithSpatialTemporal() {
    ingest(true);
    testQueries(true);
    testDeleteByCustomIndex(true);

  }


  @Test
  public void testCustomIndexingAsOnlyIndex() {
    ingest(false);
    testQueries(false);
    testDeleteByCustomIndex(false);
  }

  private void testQueries(boolean spatialTemporal) {
    final DataStore dataStore = dataStoreOptions.createDataStore();
    final VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();
    final Calendar cal = Calendar.getInstance();
    cal.set(1996, Calendar.JUNE, 15, 1, 1, 1);
    final Date startQueryTime = cal.getTime();
    cal.set(1996, Calendar.JUNE, 16, 1, 1, 1);
    Assert.assertEquals(
        513L,
        (long) dataStore.aggregateStatistics(
            StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).build()).getValue());
    final Date endQueryTime = cal.getTime();
    if (spatialTemporal) {
      // if spatial/temporal indexing exists explicitly set the appropriate one
      bldr.indexName(new SpatialIndexBuilder().createIndex().getName());
    }
    try (CloseableIterator<SimpleFeature> it =
        dataStore.query(
            bldr.constraints(
                bldr.constraintsFactory().spatialTemporalConstraints().spatialConstraints(
                    GeometryUtils.GEOMETRY_FACTORY.toGeometry(
                        new Envelope(0, 2, 0, 2))).build()).build())) {
      Assert.assertEquals(27, Iterators.size(it));
    }
    if (spatialTemporal) {
      // if spatial/temporal indexing exists explicitly set the appropriate one
      bldr.indexName(new SpatialTemporalIndexBuilder().createIndex().getName());
    }
    try (CloseableIterator<SimpleFeature> it =
        dataStore.query(
            bldr.constraints(
                bldr.constraintsFactory().spatialTemporalConstraints().spatialConstraints(
                    GeometryUtils.GEOMETRY_FACTORY.toGeometry(
                        new Envelope(0, 2, 0, 2))).addTimeRange(
                            startQueryTime,
                            endQueryTime).build()).build())) {
      Assert.assertEquals(9, Iterators.size(it));
    }
    try (CloseableIterator<SimpleFeature> it =
        dataStore.query(
            bldr.constraints(
                bldr.constraintsFactory().customConstraints(
                    new TestEnumConstraints(TestEnum.A))).indexName(
                        TEST_ENUM_INDEX_NAME).build())) {
      Assert.assertEquals(513 / 2, Iterators.size(it));
    }
    try (CloseableIterator<SimpleFeature> it =
        dataStore.query(
            bldr.constraints(
                bldr.constraintsFactory().customConstraints(
                    new TestEnumConstraints(TestEnum.B))).indexName(
                        TEST_ENUM_INDEX_NAME).build())) {
      Assert.assertEquals(513 / 3, Iterators.size(it));
    }
    try (CloseableIterator<SimpleFeature> it =
        dataStore.query(
            bldr.constraints(
                bldr.constraintsFactory().customConstraints(
                    new TestEnumConstraints(TestEnum.C))).indexName(
                        TEST_ENUM_INDEX_NAME).build())) {
      Assert.assertEquals(513 / 5, Iterators.size(it));
    }
  }

  private void testDeleteByCustomIndex(boolean spatialIndex) {
    final DataStore dataStore = dataStoreOptions.createDataStore();
    final VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();
    dataStore.delete(
        bldr.constraints(
            bldr.constraintsFactory().customConstraints(
                new TestEnumConstraints(TestEnum.C))).indexName(TEST_ENUM_INDEX_NAME).build());
    try (CloseableIterator<SimpleFeature> it =
        dataStore.query(
            bldr.constraints(
                bldr.constraintsFactory().customConstraints(
                    new TestEnumConstraints(TestEnum.C))).indexName(
                        TEST_ENUM_INDEX_NAME).build())) {
      Assert.assertEquals(0, Iterators.size(it));
    }
    try (CloseableIterator<SimpleFeature> it =
        dataStore.query(
            bldr.constraints(
                bldr.constraintsFactory().customConstraints(
                    new TestEnumConstraints(TestEnum.A))).indexName(
                        TEST_ENUM_INDEX_NAME).build())) {
      // Subtract out the number of features that have A + C
      Assert.assertEquals(513 / 2 - 513 / 10, Iterators.size(it));
    }
    dataStore.delete(
        bldr.constraints(
            bldr.constraintsFactory().customConstraints(
                new TestEnumConstraints(TestEnum.B))).indexName(TEST_ENUM_INDEX_NAME).build());
    try (CloseableIterator<SimpleFeature> it =
        dataStore.query(
            bldr.constraints(
                bldr.constraintsFactory().customConstraints(
                    new TestEnumConstraints(TestEnum.B))).indexName(
                        TEST_ENUM_INDEX_NAME).build())) {
      Assert.assertEquals(0, Iterators.size(it));
    }
    try (CloseableIterator<SimpleFeature> it =
        dataStore.query(
            bldr.constraints(
                bldr.constraintsFactory().customConstraints(
                    new TestEnumConstraints(TestEnum.A))).indexName(
                        TEST_ENUM_INDEX_NAME).build())) {
      // Subtract out the number of features that have A + C and A + B, but add back in features
      // that were A + B + C so they aren't double subtracted.
      Assert.assertEquals(513 / 2 - 513 / 10 - 513 / 6 + 513 / 30, Iterators.size(it));
    }
    if (spatialIndex) {
      try (CloseableIterator<SimpleFeature> it =
          dataStore.query(
              bldr.constraints(
                  bldr.constraintsFactory().spatialTemporalConstraints().spatialConstraints(
                      GeometryUtils.GEOMETRY_FACTORY.toGeometry(
                          new Envelope(0, 2, 0, 2))).build()).build())) {
        // Number of features with A and NOT_A only
        Assert.assertEquals(15, Iterators.size(it));
      }
    }
    dataStore.delete(
        bldr.constraints(
            bldr.constraintsFactory().customConstraints(
                new TestEnumConstraints(TestEnum.A))).indexName(TEST_ENUM_INDEX_NAME).build());
    try (CloseableIterator<SimpleFeature> it =
        dataStore.query(
            bldr.constraints(
                bldr.constraintsFactory().customConstraints(
                    new TestEnumConstraints(TestEnum.A))).indexName(
                        TEST_ENUM_INDEX_NAME).build())) {
      Assert.assertEquals(0, Iterators.size(it));
    }
    if (spatialIndex) {
      try (CloseableIterator<SimpleFeature> it =
          dataStore.query(
              bldr.constraints(
                  bldr.constraintsFactory().spatialTemporalConstraints().spatialConstraints(
                      GeometryUtils.GEOMETRY_FACTORY.toGeometry(
                          new Envelope(0, 2, 0, 2))).build()).build())) {
        // Number of features with NOT_A only
        Assert.assertEquals(9, Iterators.size(it));
      }
    }
  }

  private static CustomIndex<SimpleFeature, TestEnumConstraints> getTestEnumIndex() {
    return new CustomIndex<>(new TestEnumIndexStrategy(), TEST_ENUM_INDEX_NAME);
  }

  public static class TestEnumIndexStrategy implements
      CustomIndexStrategy<SimpleFeature, TestEnumConstraints> {

    @Override
    public InsertionIds getInsertionIds(final SimpleFeature entry) {
      final String testEnums = (String) entry.getAttribute("Comment");
      if (testEnums != null) {
        final String[] testEnumsArray = testEnums.split(",");
        final List<byte[]> insertionIdsList = new ArrayList<>(testEnumsArray.length);
        for (final String testEnum : testEnumsArray) {
          insertionIdsList.add(StringUtils.stringToBinary(testEnum));
        }
        return new InsertionIds(insertionIdsList);
      }
      return new InsertionIds();
    }

    @Override
    public QueryRanges getQueryRanges(final TestEnumConstraints constraints) {
      final byte[] sortKey = StringUtils.stringToBinary(constraints.testEnum.toString());
      return new QueryRanges(new ByteArrayRange(sortKey, sortKey));
    }

    @Override
    public byte[] toBinary() {
      return new byte[0];
    }

    @Override
    public void fromBinary(final byte[] bytes) {}

    @Override
    public Class<TestEnumConstraints> getConstraintsClass() {
      return TestEnumConstraints.class;
    }

  }
  public static class TestEnumConstraints implements Persistable {
    private TestEnum testEnum;

    public TestEnumConstraints() {}

    public TestEnumConstraints(final TestEnum testEnum) {
      this.testEnum = testEnum;
    }

    @Override
    public byte[] toBinary() {
      return StringUtils.stringToBinary(testEnum.toString());
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      testEnum = TestEnum.valueOf(StringUtils.stringFromBinary(bytes));

    }

  }

  private static List<SimpleFeature> getGriddedTemporalFeaturesWithEnumString(
      final SimpleFeatureBuilder pointBuilder,
      final int firstFeatureId) {

    int featureId = firstFeatureId;
    final Calendar cal = Calendar.getInstance();
    cal.set(1996, Calendar.JUNE, 15, 0, 0, 0);
    final Date[] dates = new Date[3];
    dates[0] = cal.getTime();
    cal.set(1996, Calendar.JUNE, 16, 0, 0, 0);
    dates[1] = cal.getTime();
    cal.set(1996, Calendar.JUNE, 17, 0, 0, 0);
    dates[2] = cal.getTime();
    // put 3 points on each grid location with different temporal attributes
    final List<SimpleFeature> feats = new ArrayList<>();
    for (int longitude = -9; longitude <= 9; longitude++) {
      for (int latitude = -4; latitude <= 4; latitude++) {
        for (int date = 0; date < dates.length; date++) {
          pointBuilder.set(
              "geometry",
              GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(longitude, latitude)));
          pointBuilder.set("TimeStamp", dates[date]);
          pointBuilder.set("Latitude", latitude);
          pointBuilder.set("Longitude", longitude);
          pointBuilder.set("Comment", getCommentValue(featureId));
          // Note since trajectoryID and comment are marked as
          // nillable we
          // don't need to set them (they default to null).

          final SimpleFeature sft = pointBuilder.buildFeature(String.valueOf(featureId));
          feats.add(sft);
          featureId++;
        }
      }
    }
    return feats;
  }

  private static String getCommentValue(final int id) {
    final List<String> enums = new ArrayList<>();
    for (final TestEnum e : TestEnum.values()) {
      if (e.test(id)) {
        enums.add(e.toString());
      }
    }
    if (!enums.isEmpty()) {
      // add as comma-delimited string
      return String.join(",", enums);
    }
    return null;
  }
}
