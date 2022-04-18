/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.secondary;

import java.io.IOException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.GeoWaveVisibilityIT;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
public class VisibilitySecondaryIndexIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(VisibilitySecondaryIndexIT.class);

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB,
          GeoWaveStoreType.FILESYSTEM},
      options = {"enableSecondaryIndexing=true", "enableVisibility=true"})
  protected DataStorePluginOptions dataStoreOptions;
  private static final String testName = "VisibilitySecondaryIndexIT";
  private static long startMillis;

  private static final int TOTAL_FEATURES_FOR_VISIBILITY_TEST = 400;

  @BeforeClass
  public static void startTimer() {
    startMillis = System.currentTimeMillis();
    TestUtils.printStartOfTest(LOGGER, testName);
  }

  @AfterClass
  public static void reportTest() {
    TestUtils.printEndOfTest(LOGGER, testName, startMillis);
  }

  @After
  public void deleteAll() {
    TestUtils.deleteAll(dataStoreOptions);
  }

  @Test
  public void testVisibility() {
    GeoWaveVisibilityIT.testIngestAndQueryVisibilityFields(
        dataStoreOptions,
        getFeatureVisWriter(),
        (differingVisibilities) -> Assert.assertEquals(
            "No entries should have differing visibility",
            0,
            differingVisibilities.getValue().longValue()),
        (storeAndStatsStore, internalAdapterIdAndSpatial) -> {
          try {
            testQuery(
                storeAndStatsStore.getLeft(),
                storeAndStatsStore.getRight(),
                internalAdapterIdAndSpatial.getLeft(),
                internalAdapterIdAndSpatial.getRight());
          } catch (final IOException e) {
            LOGGER.warn("Unable to test visibility query", e);
            Assert.fail(e.getMessage());
          }
        },
        TOTAL_FEATURES_FOR_VISIBILITY_TEST);
  }

  private static void testQuery(
      final DataStore store,
      final DataStatisticsStore statsStore,
      final short internalAdapterId,
      final boolean spatial) throws IOException {
    // you have to at least be able to see the geometry field which is wide
    // open for exactly (total_Features / 4)
    testQuery(
        store,
        statsStore,
        internalAdapterId,
        new String[] {},
        spatial,
        (TOTAL_FEATURES_FOR_VISIBILITY_TEST) / 4);

    for (final String auth : new String[] {"a", "b", "c"}) {
      testQuery(
          store,
          statsStore,
          internalAdapterId,
          new String[] {auth},
          spatial,
          (2 * TOTAL_FEATURES_FOR_VISIBILITY_TEST) / 4);
    }

    // order shouldn't matter, but let's make sure here
    for (final String[] auths : new String[][] {
        new String[] {"a", "b"},
        new String[] {"b", "a"},
        new String[] {"a", "c"},
        new String[] {"c", "a"},
        new String[] {"b", "c"},
        new String[] {"c", "b"}}) {
      testQuery(
          store,
          statsStore,
          internalAdapterId,
          auths,
          spatial,
          (3 * TOTAL_FEATURES_FOR_VISIBILITY_TEST) / 4);
    }

    testQuery(
        store,
        statsStore,
        internalAdapterId,
        new String[] {"a", "b", "c"},
        spatial,
        TOTAL_FEATURES_FOR_VISIBILITY_TEST);
  }

  private static void testQuery(
      final DataStore store,
      final DataStatisticsStore statsStore,
      final short internalAdapterId,
      final String[] auths,
      final boolean spatial,
      final int expectedResultCount) throws IOException {
    // this doesn't use mixed visibilities so all attributes should be non-null
    GeoWaveVisibilityIT.testQuery(
        store,
        statsStore,
        internalAdapterId,
        auths,
        spatial,
        expectedResultCount,
        expectedResultCount * 4);
  }

  private VisibilityHandler getFeatureVisWriter() {
    return new TestSecondaryIndexFieldVisibilityHandler();
  }

  public static class TestSecondaryIndexFieldVisibilityHandler implements VisibilityHandler {

    @Override
    public byte[] toBinary() {
      return new byte[0];
    }

    @Override
    public void fromBinary(byte[] bytes) {}

    @Override
    public <T> String getVisibility(DataTypeAdapter<T> adapter, T entry, String fieldName) {
      final int fieldValueInt = Integer.parseInt(((SimpleFeature) entry).getID());
      // make them all the same because secondary indexing does not support mixed
      // visibilities

      // make some no bytes, some a, some
      // b, and some c
      final int switchValue = fieldValueInt % 4;
      switch (switchValue) {
        case 0:
          return "a";

        case 1:
          return "b";

        case 2:
          return "c";

        case 3:
        default:
          return "";
      }
    }

  }

}
