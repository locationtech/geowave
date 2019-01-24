/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.query;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.function.BiConsumer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.config.ConfigUtils;
import org.locationtech.geowave.core.store.data.VisibilityWriter;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveBasicVectorIT;
import org.locationtech.geowave.test.basic.GeoWaveVisibilityIT;
import org.locationtech.geowave.test.mapreduce.BasicMapReduceIT;
import org.locationtech.geowave.test.spark.SparkTestEnvironment;
import org.locationtech.geowave.test.spark.SparkUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.aol.cyclops.util.function.TriConsumer;

@RunWith(GeoWaveITRunner.class)
@Environments({Environment.MAP_REDUCE, Environment.SPARK})
public class SecondaryIndexIT extends AbstractGeoWaveBasicVectorIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(SecondaryIndexIT.class);

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB},
      options = {"enableSecondaryIndex=true", "enableVisibility=true"})
  protected DataStorePluginOptions dataStoreOptions;

  private static final int TOTAL_FEATURES_FOR_VISIBILITY_TEST = 400;

  @Test
  public void testLocalIngestAndQuerySpatial() throws Exception {
    testIngestAndQuery(DimensionalityType.SPATIAL, false);
  }

  @Test
  public void testLocalIngestAndQuerySpatialTemporal() throws Exception {
    testIngestAndQuery(DimensionalityType.SPATIAL_TEMPORAL, false);
  }

  @Test
  public void testLocalIngestAndQuerySpatialAndSpatialTemporal() throws Exception {
    testIngestAndQuery(DimensionalityType.ALL, false);
  }

  @Test
  public void testDistributedIngestAndQuerySpatial() throws Exception {
    testIngestAndQuery(DimensionalityType.SPATIAL, true);
  }

  @Test
  public void testDistributedIngestAndQuerySpatialTemporal() throws Exception {
    testIngestAndQuery(DimensionalityType.SPATIAL_TEMPORAL, true);
  }

  @Test
  public void testDistributedIngestAndQuerySpatialAndSpatialTemporal() throws Exception {
    testIngestAndQuery(DimensionalityType.ALL, true);
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

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStoreOptions;
  }

  private void testIngestAndQuery(
      final DimensionalityType dimensionality,
      final boolean distributed) throws Exception {
    // ingest both lines and points
    if (distributed) {
      testIngestAndQuery(dimensionality, (d, f) -> {
        try {
          testMapReduceExportAndReingest(dataStoreOptions, d, f);
        } catch (final Exception e) {
          LOGGER.warn("Unable to ingest map-reduce", e);
          Assert.fail(e.getMessage());
        }
      },
          (input, expected, description) -> SparkUtils.verifyQuery(
              dataStoreOptions,
              SparkTestEnvironment.getInstance().getDefaultSession().sparkContext(),
              input,
              expected,
              description,
              null,
              false),
          (dimensionalityType, urls) -> {
            // no-op on verify stats because the "expected" stats that are calculated are off by an
            // epsilon (ie. problem with the test, not the actual results)
          });
    } else {
      testIngestAndQuery(dimensionality, (d, f) -> {
        try {
          TestUtils.testLocalIngest(dataStoreOptions, dimensionality, f, 1);
        } catch (final Exception e) {
          LOGGER.warn("Unable to ingest locally", e);
          Assert.fail(e.getMessage());
        }
      }, (input, expected, description) -> {
        try {
          testQuery(input, expected, description);
        } catch (final Exception e) {
          LOGGER.warn("Unable to query locally", e);
          Assert.fail(e.getMessage());
        }
      }, (dimensionalityType, urls) -> testStats(urls, false, dimensionality.getDefaultIndices()));
    }
  }

  private static void testMapReduceExportAndReingest(
      final DataStorePluginOptions dataStoreOptions,
      final DimensionalityType dimensionality,
      final String file) throws Exception {
    final Map<String, String> configOptions = dataStoreOptions.getOptionsAsMap();
    final StoreFactoryOptions options =
        ConfigUtils.populateOptionsFromList(
            dataStoreOptions.getFactoryFamily().getDataStoreFactory().createOptionsInstance(),
            configOptions);
    options.setGeoWaveNamespace(dataStoreOptions.getGeoWaveNamespace() + "_tmp");

    final DataStorePluginOptions tempStore = new DataStorePluginOptions(options);
    TestUtils.testLocalIngest(tempStore, dimensionality, file, 1);
    BasicMapReduceIT.testMapReduceExportAndReingest(tempStore, dataStoreOptions, dimensionality);
  }

  private void testIngestAndQuery(
      final DimensionalityType dimensionality,
      final BiConsumer<DimensionalityType, String> ingestFunction,
      final TriConsumer<URL, URL[], String> queryFunction,
      final BiConsumer<DimensionalityType, URL[]> verifyStats) throws Exception {
    ingestFunction.accept(dimensionality, HAIL_SHAPEFILE_FILE);
    ingestFunction.accept(dimensionality, TORNADO_TRACKS_SHAPEFILE_FILE);
    queryFunction.accept(
        new File(TEST_BOX_FILTER_FILE).toURI().toURL(),
        new URL[] {
            new File(HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE).toURI().toURL(),
            new File(TORNADO_TRACKS_EXPECTED_BOX_FILTER_RESULTS_FILE).toURI().toURL()},
        "bounding box");
    queryFunction.accept(
        new File(TEST_POLYGON_FILTER_FILE).toURI().toURL(),
        new URL[] {
            new File(HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE).toURI().toURL(),
            new File(TORNADO_TRACKS_EXPECTED_POLYGON_FILTER_RESULTS_FILE).toURI().toURL()},
        "polygon constraint");
    queryFunction.accept(
        new File(TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
        new URL[] {
            new File(HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL(),
            new File(TORNADO_TRACKS_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()},
        "bounding box and time range");
    queryFunction.accept(
        new File(TEST_POLYGON_TEMPORAL_FILTER_FILE).toURI().toURL(),
        new URL[] {
            new File(HAIL_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL(),
            new File(TORNADO_TRACKS_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()},
        "polygon constraint and time range");
    final URL[] urls =
        new URL[] {
            new File(HAIL_SHAPEFILE_FILE).toURI().toURL(),
            new File(TORNADO_TRACKS_SHAPEFILE_FILE).toURI().toURL()};
    verifyStats.accept(dimensionality, urls);
    testSpatialTemporalLocalExportAndReingestWithCQL(
        new File(TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
        1,
        false,
        dimensionality);
    testDeleteDataId(
        new File(TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
        dimensionality.getDefaultIndices()[0]);
    testDeleteCQL(CQL_DELETE_STR, null);

    testDeleteByBasicQuery(new File(TEST_POLYGON_TEMPORAL_FILTER_FILE).toURI().toURL(), null);
    testDeleteByBasicQuery(new File(TEST_POLYGON_FILTER_FILE).toURI().toURL(), null);
    TestUtils.deleteAll(dataStoreOptions);
  }
}
