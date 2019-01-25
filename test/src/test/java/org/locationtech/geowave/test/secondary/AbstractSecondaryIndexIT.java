/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.secondary;

import java.io.File;
import java.net.URL;
import java.util.Map;
import java.util.function.BiConsumer;
import org.junit.Assert;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.config.ConfigUtils;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveBasicVectorIT;
import org.locationtech.geowave.test.mapreduce.BasicMapReduceIT;
import org.locationtech.geowave.test.spark.SparkTestEnvironment;
import org.locationtech.geowave.test.spark.SparkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.aol.cyclops.util.function.TriConsumer;

abstract public class AbstractSecondaryIndexIT extends AbstractGeoWaveBasicVectorIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSecondaryIndexIT.class);

  protected void testIngestAndQuery(
      final DimensionalityType dimensionality,
      final boolean distributed) throws Exception {
    // ingest both lines and points
    if (distributed) {
      testIngestAndQuery(dimensionality, (d, f) -> {
        try {
          testMapReduceExportAndReingest(getDataStorePluginOptions(), d, f);
        } catch (final Exception e) {
          LOGGER.warn("Unable to ingest map-reduce", e);
          Assert.fail(e.getMessage());
        }
      },
          (input, expected, description) -> SparkUtils.verifyQuery(
              getDataStorePluginOptions(),
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
          TestUtils.testLocalIngest(getDataStorePluginOptions(), dimensionality, f, 1);
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
    TestUtils.deleteAll(getDataStorePluginOptions());
  }
}
