/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.spark;

import java.net.URL;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.junit.Assert;
import org.locationtech.geowave.analytic.spark.GeoWaveRDD;
import org.locationtech.geowave.analytic.spark.GeoWaveRDDLoader;
import org.locationtech.geowave.analytic.spark.RDDOptions;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.ExpectedResults;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(SparkUtils.class);
  private static final int DEFAULT_SPLITS_FOR_COUNT = 10;

  public static void verifyQuery(
      final DataStorePluginOptions dataStore,
      final SparkContext context,
      final URL filterFile,
      final URL[] expectedResultsFiles,
      final String name,
      final Pair<String, String> optimalCqlQueryGeometryAndTimeFields,
      final boolean useDuring) {
    verifyQuery(
        dataStore,
        context,
        filterFile,
        expectedResultsFiles,
        name,
        null,
        optimalCqlQueryGeometryAndTimeFields,
        useDuring);
  }

  public static void verifyQuery(
      final DataStorePluginOptions dataStore,
      final SparkContext context,
      final URL filterFile,
      final URL[] expectedResultsFiles,
      final String name,
      final CoordinateReferenceSystem crsTransform,
      final Pair<String, String> optimalCqlQueryGeometryAndTimeFields,
      final boolean useDuring) {
    try {
      // get expected results
      final ExpectedResults expectedResults = TestUtils.getExpectedResults(expectedResultsFiles);

      QueryConstraints query;
      if (crsTransform != null) {
        final SimpleFeature feature = TestUtils.resourceToFeature(filterFile);
        query =
            TestUtils.featureToQuery(
                GeometryUtils.crsTransform(
                    feature,
                    SimpleFeatureTypeBuilder.retype(feature.getFeatureType(), crsTransform),
                    CRS.findMathTransform(GeometryUtils.getDefaultCRS(), crsTransform, true)),
                null,
                GeometryUtils.getCrsCode(crsTransform),
                useDuring);

      } else {
        query =
            TestUtils.resourceToQuery(filterFile, optimalCqlQueryGeometryAndTimeFields, useDuring);
      }
      // Load RDD using spatial query (bbox)
      final RDDOptions queryOpts = new RDDOptions();
      queryOpts.setQuery(QueryBuilder.newBuilder().constraints(query).build());
      queryOpts.setMinSplits(DEFAULT_SPLITS_FOR_COUNT);
      queryOpts.setMaxSplits(DEFAULT_SPLITS_FOR_COUNT);
      final GeoWaveRDD newRDD = GeoWaveRDDLoader.loadRDD(context, dataStore, queryOpts);
      final JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = newRDD.getRawRDD();
      final long count = getCount(javaRdd, dataStore.getType());

      LOGGER.warn("DataStore loaded into RDD with " + count + " features.");

      // Verify RDD count matches expected count
      Assert.assertEquals(expectedResults.count, count);
    } catch (final Exception e) {
      e.printStackTrace();
      TestUtils.deleteAll(dataStore);
      Assert.fail("Error occurred while testing '" + name + "'");
    }
  }

  public static long getCount(
      final JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd,
      final String dataStoreType) {
    // TODO counting by key shouldn't be necessary
    // it seems like it could only occur if the RecordReaders resulting from the splits had
    // overlapping ranges
    return javaRdd.countByKey().size();
  }
}
