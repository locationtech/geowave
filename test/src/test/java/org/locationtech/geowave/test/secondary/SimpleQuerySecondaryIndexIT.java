/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.secondary;

import java.util.List;
import org.apache.commons.lang3.Range;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.simple.SimpleDoubleIndexStrategy;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.dimension.BasicNumericDimensionField;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.BasicIndexModel;
import org.locationtech.geowave.core.store.index.CustomNameIndex;
import org.locationtech.geowave.core.store.query.constraints.SimpleNumericQuery;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveIT;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
public class SimpleQuerySecondaryIndexIT extends AbstractGeoWaveIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleQuerySecondaryIndexIT.class);

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
      options = {"enableSecondaryIndexing=true"})
  protected DataStorePluginOptions dataStoreOptions;
  private static long startMillis;
  private static final String testName = "SimpleQuerySecondaryIndexIT";

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStoreOptions;
  }

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
  public void testNoSecondaryIndices() {
    final DataStore ds = dataStoreOptions.createDataStore();
    final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();
    final GeotoolsFeatureDataAdapter fda = SimpleIngest.createDataAdapter(sft);
    final List<SimpleFeature> features =
        SimpleIngest.getGriddedFeatures(new SimpleFeatureBuilder(sft), 1234);
    ds.addType(fda);
    int totalFeatures = 0;
    int ingestedFeatures = 0;
    try (Writer<SimpleFeature> writer = ds.createWriter(fda.getTypeName())) {
      for (final SimpleFeature feat : features) {
        if ((totalFeatures % 5) == 0) {
          // just write 20 percent of the grid
          writer.write(feat);
          ingestedFeatures++;
        }
        totalFeatures++;
      }
    }
    try (CloseableIterator<SimpleFeature> it =
        ds.query(
            VectorQueryBuilder.newBuilder().addTypeName(sft.getTypeName()).constraints(
                VectorQueryBuilder.newBuilder().constraintsFactory().dataIds(
                    StringUtils.stringToBinary(Integer.toString(1234)))).build())) {
      Assert.assertTrue(it.hasNext());
    }
    Assert.assertTrue(
        ds.delete(
            VectorQueryBuilder.newBuilder().addTypeName(sft.getTypeName()).constraints(
                VectorQueryBuilder.newBuilder().constraintsFactory().dataIds(
                    StringUtils.stringToBinary(Integer.toString(1234)))).build()));
    try (CloseableIterator<SimpleFeature> it =
        ds.query(
            VectorQueryBuilder.newBuilder().addTypeName(sft.getTypeName()).constraints(
                VectorQueryBuilder.newBuilder().constraintsFactory().dataIds(
                    StringUtils.stringToBinary(Integer.toString(1234)))).build())) {
      Assert.assertFalse(it.hasNext());
    }
    Assert.assertTrue(
        ds.delete(
            VectorQueryBuilder.newBuilder().addTypeName(sft.getTypeName()).constraints(
                VectorQueryBuilder.newBuilder().constraintsFactory().dataIds(
                    StringUtils.stringToBinary(Integer.toString(1239)))).build()));
    try (CloseableIterator<SimpleFeature> it =
        ds.query(
            VectorQueryBuilder.newBuilder().addTypeName(sft.getTypeName()).constraints(
                VectorQueryBuilder.newBuilder().constraintsFactory().dataIds(
                    StringUtils.stringToBinary(Integer.toString(1239)))).build())) {
      Assert.assertFalse(it.hasNext());
    }
    try (CloseableIterator<SimpleFeature> it =
        ds.query(VectorQueryBuilder.newBuilder().addTypeName(sft.getTypeName()).build())) {
      int count = 0;
      while (it.hasNext()) {
        it.next();
        count++;
      }
      Assert.assertEquals(ingestedFeatures - 2, count);
    }

    // TODO within the datastores delete by range is not supported (the deletion logic expect Data
    // IDs to be non-null within reader params and deletions don't have logic for handling ranges

    // GEOWAVE Issue #1575 documents this
    //@formatter:off
//    try (CloseableIterator<SimpleFeature> it =
//        ds.query(
//            VectorQueryBuilder.newBuilder().addTypeName(sft.getTypeName()).constraints(
//                VectorQueryBuilder.newBuilder().constraintsFactory().dataIdsByRange(
//                    StringUtils.stringToBinary(Integer.toString(1234)),
//                    StringUtils.stringToBinary(Integer.toString(1249)))).build())) {
//      int count = 0;
//      while (it.hasNext()) {
//        it.next();
//        count++;
//      }
    //there would be 4 but 2 were already delete individually
//      Assert.assertEquals(2, count);
//    }
//    Assert.assertTrue(
//        ds.delete(
//            VectorQueryBuilder.newBuilder().addTypeName(sft.getTypeName()).constraints(
//                VectorQueryBuilder.newBuilder().constraintsFactory().dataIdsByRange(
//                    StringUtils.stringToBinary(Integer.toString(1234)),
//                    StringUtils.stringToBinary(Integer.toString(1249)))).build()));
//    try (CloseableIterator<SimpleFeature> it =
//        ds.query(
//            VectorQueryBuilder.newBuilder().addTypeName(sft.getTypeName()).constraints(
//                VectorQueryBuilder.newBuilder().constraintsFactory().dataIdsByRange(
//                    StringUtils.stringToBinary(Integer.toString(1234)),
//                    StringUtils.stringToBinary(Integer.toString(1249)))).build())) {
//      Assert.assertFalse(it.hasNext());
//    }
//    try (CloseableIterator<SimpleFeature> it =
//        ds.query(VectorQueryBuilder.newBuilder().addTypeName(sft.getTypeName()).build())) {
//      int count = 0;
//      while (it.hasNext()) {
//        it.next();
//        count++;
//      }
    //this would include 2 from individual deletion and 2 from range deletion
//      Assert.assertEquals(ingestedFeatures - 4, count);
//    }
    //@formatter:on
    ds.deleteAll();
  }

  // @Test
  public void testMultipleSecondaryIndices() {
    final DataStore ds = dataStoreOptions.createDataStore();
    final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();
    final GeotoolsFeatureDataAdapter fda = SimpleIngest.createDataAdapter(sft);
    final List<SimpleFeature> features =
        SimpleIngest.getGriddedFeatures(new SimpleFeatureBuilder(sft), 1234);
    final Index latIdx =
        new CustomNameIndex(
            new SimpleDoubleIndexStrategy(),
            new BasicIndexModel(
                new NumericDimensionField[] {
                    new BasicNumericDimensionField<>("Latitude", Double.class)}),
            "Lat_IDX");
    final Index lonIdx =
        new CustomNameIndex(
            new SimpleDoubleIndexStrategy(),
            new BasicIndexModel(
                new NumericDimensionField[] {
                    new BasicNumericDimensionField<>("Longitude", Double.class)}),
            "Lon_IDX");
    ds.addType(fda, TestUtils.DEFAULT_SPATIAL_INDEX, latIdx, lonIdx);
    int ingestedFeatures = 0;
    try (Writer<SimpleFeature> writer = ds.createWriter(fda.getTypeName())) {
      for (final SimpleFeature feat : features) {
        ingestedFeatures++;
        if ((ingestedFeatures % 5) == 0) {
          // just write 20 percent of the grid
          writer.write(feat);
        }
      }
    }
    try (CloseableIterator<SimpleFeature> it =
        ds.query(
            VectorQueryBuilder.newBuilder().indexName("Lon_IDX").addTypeName(
                sft.getTypeName()).constraints(
                    new SimpleNumericQuery(Range.between((double) 0, (double) 0))).build())) {
      int count = 0;
      while (it.hasNext()) {
        it.next();
        count++;
      }
      Assert.assertTrue(count > 1);
    }
    Assert.assertTrue(
        ds.delete(
            VectorQueryBuilder.newBuilder().indexName("Lon_IDX").addTypeName(
                sft.getTypeName()).constraints(
                    new SimpleNumericQuery(Range.between((double) 0, (double) 0))).build()));
    try (CloseableIterator<SimpleFeature> it =
        ds.query(
            VectorQueryBuilder.newBuilder().indexName("Lon_IDX").addTypeName(
                sft.getTypeName()).constraints(
                    new SimpleNumericQuery(Range.between((double) 0, (double) 0))).build())) {
      int count = 0;
      while (it.hasNext()) {
        it.next();
        count++;
      }
      Assert.assertTrue(count == 0);
    }
    try (CloseableIterator<SimpleFeature> it =
        ds.query(
            VectorQueryBuilder.newBuilder().indexName("Lon_IDX").addTypeName(
                sft.getTypeName()).constraints(
                    new SimpleNumericQuery(Range.between((double) 1, (double) 45))).build())) {
      int count = 0;
      while (it.hasNext()) {
        it.next();
        count++;
      }
      Assert.assertTrue(count > 1);
    }
    ds.deleteAll();
  }
}
