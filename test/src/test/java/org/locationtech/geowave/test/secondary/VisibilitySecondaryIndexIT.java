/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.secondary;

import java.io.IOException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.data.VisibilityWriter;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;
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
          GeoWaveStoreType.ROCKSDB},
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

  @Test
  public void testVisibility() {
    GeoWaveVisibilityIT.testIngestAndQueryVisibilityFields(
        dataStoreOptions,
        getFeatureVisWriter(),
        (differingVisibilities) -> Assert.assertEquals(
            "No entries should have differing visibility",
            0,
            differingVisibilities.getEntriesWithDifferingFieldVisibilities()),
        (store, statsStore, internalAdapterId, spatial) -> {
          try {
            testQuery(store, statsStore, internalAdapterId, spatial);
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

  private VisibilityWriter<SimpleFeature> getFeatureVisWriter() {
    return new VisibilityWriter<SimpleFeature>() {
      @Override
      public FieldVisibilityHandler<SimpleFeature, Object> getFieldVisibilityHandler(
          final String fieldId) {
        return new FieldVisibilityHandler<SimpleFeature, Object>() {

          @Override
          public byte[] getVisibility(
              final SimpleFeature rowValue,
              final String fieldId,
              final Object fieldValue) {
            final int fieldValueInt = Integer.parseInt(rowValue.getID());
            // make them all the same because secondary indexing does not support mixed
            // visibilities

            // make some no bytes, some a, some
            // b, and some c
            final int switchValue = fieldValueInt % 4;
            switch (switchValue) {
              case 0:
                return new ByteArray("a").getBytes();

              case 1:
                return new ByteArray("b").getBytes();

              case 2:
                return new ByteArray("c").getBytes();

              case 3:
              default:
                return new byte[] {};
            }
          }
        };
      }
    };
  }

}
