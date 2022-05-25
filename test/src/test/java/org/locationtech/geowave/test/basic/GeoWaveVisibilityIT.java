/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.raster.RasterUtils;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.ExplicitSpatialQuery;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.DataStoreProperty;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.api.AggregationQueryBuilder;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.StatisticQueryBuilder;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.data.visibility.FieldMappedVisibilityHandler;
import org.locationtech.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.core.store.statistics.binning.DataTypeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.binning.FieldValueBinningStrategy;
import org.locationtech.geowave.core.store.statistics.index.DifferingVisibilityCountStatistic;
import org.locationtech.geowave.core.store.statistics.index.DifferingVisibilityCountStatistic.DifferingVisibilityCountValue;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import jersey.repackaged.com.google.common.collect.Iterators;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveVisibilityIT extends AbstractGeoWaveIT {
  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB,
          GeoWaveStoreType.FILESYSTEM},
      options = {"enableVisibility=true", "enableSecondaryIndexing=false"})
  protected DataStorePluginOptions dataStoreOptions;

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGeoWaveIT.class);
  private static long startMillis;

  private static final int TOTAL_FEATURES = 800;

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStoreOptions;
  }

  @BeforeClass
  public static void startTimer() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*         RUNNING GeoWaveVisibilityIT   *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @AfterClass
  public static void reportTest() {
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*      FINISHED GeoWaveVisibilityIT     *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @After
  public void deleteAll() {
    TestUtils.deleteAll(dataStoreOptions);
    // dataStoreOptions.createDataStoreOperations().clearAuthorizations(null);
  }

  @Test
  public void testIngestAndQueryMixedVisibilityRasters() throws IOException {
    final String coverageName = "testMixedVisibilityRasters";
    final int maxCellSize =
        TestUtils.getTestEnvironment(dataStoreOptions.getType()).getMaxCellSize();
    final int tileSize;
    if (maxCellSize <= (64 * 1024)) {
      tileSize = 24;
    } else {
      tileSize = 64;
    }
    final double westLon = 0;
    final double eastLon = 45;
    final double southLat = 0;
    final double northLat = 45;

    ingestAndQueryMixedVisibilityRasters(
        coverageName,
        tileSize,
        westLon,
        eastLon,
        southLat,
        northLat);
  }

  @Test
  public void testComplexVisibility() throws IOException {
    internalTestComplexVisibility();
    // because this has intermittently failed in the past, lets run it several times and make sure
    // it passes regularly
    for (int i = 1; i < 5; i++) {
      deleteAll();
      internalTestComplexVisibility();
    }
  }

  public void internalTestComplexVisibility() throws IOException {
    final String coverageName = "testComplexVisibility";
    final int maxCellSize =
        TestUtils.getTestEnvironment(dataStoreOptions.getType()).getMaxCellSize();
    final int tileSize;
    if (maxCellSize <= (64 * 1024)) {
      tileSize = 24;
    } else {
      tileSize = 64;
    }
    final double westLon = 0;
    final double eastLon = 45;
    final double southLat = 0;
    final double northLat = 45;

    ingestAndQueryComplexVisibilityRasters(
        coverageName,
        tileSize,
        westLon,
        eastLon,
        southLat,
        northLat);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void ingestAndQueryComplexVisibilityRasters(
      final String coverageName,
      final int tileSize,
      final double westLon,
      final double eastLon,
      final double southLat,
      final double northLat) throws IOException {
    // Create two test rasters
    final int numBands = 8;
    final DataStore dataStore = dataStoreOptions.createDataStore();
    final RasterDataAdapter adapter =
        RasterUtils.createDataAdapterTypeDouble(
            coverageName,
            numBands,
            tileSize,
            new NoDataMergeStrategy());
    final WritableRaster raster1 = RasterUtils.createRasterTypeDouble(numBands, tileSize);
    final WritableRaster raster2 = RasterUtils.createRasterTypeDouble(numBands, tileSize);

    TestUtils.fillTestRasters(raster1, raster2, tileSize);
    dataStore.addType(adapter, TestUtils.DEFAULT_SPATIAL_INDEX);
    try (Writer writer = dataStore.createWriter(adapter.getTypeName())) {
      // Write the first raster w/ vis info
      writer.write(
          RasterUtils.createCoverageTypeDouble(
              coverageName,
              westLon,
              eastLon,
              southLat,
              northLat,
              raster1),
          getRasterVisWriter("(a&b)|c"));

      // Write the second raster w/ no vis info
      writer.write(
          RasterUtils.createCoverageTypeDouble(
              coverageName,
              westLon,
              eastLon,
              southLat,
              northLat,
              raster2));
    }

    // First, query w/ no authorizations. We should get
    // just the second raster back

    try (CloseableIterator<?> it =
        dataStore.query(QueryBuilder.newBuilder().addTypeName(coverageName).build())) {

      final GridCoverage coverage = (GridCoverage) it.next();
      final Raster raster = coverage.getRenderedImage().getData();

      Assert.assertEquals(tileSize, raster.getWidth());
      Assert.assertEquals(tileSize, raster.getHeight());

      for (int x = 0; x < tileSize; x++) {
        for (int y = 0; y < tileSize; y++) {
          for (int b = 0; b < numBands; b++) {
            final double p0 = raster.getSampleDouble(x, y, b);
            final double p1 = raster2.getSampleDouble(x, y, b);

            Assert.assertEquals("x=" + x + ",y=" + y + ",b=" + b, p0, p1, 0.0);
          }
        }
      }

      // there should be exactly one
      Assert.assertFalse(it.hasNext());
    }

    // Next, query w/ only 'a' authorization. We should get
    // just the second raster back
    try (CloseableIterator<?> it =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(coverageName).addAuthorization("a").build())) {

      final GridCoverage coverage = (GridCoverage) it.next();
      final Raster raster = coverage.getRenderedImage().getData();

      Assert.assertEquals(tileSize, raster.getWidth());
      Assert.assertEquals(tileSize, raster.getHeight());

      for (int x = 0; x < tileSize; x++) {
        for (int y = 0; y < tileSize; y++) {
          for (int b = 0; b < numBands; b++) {
            final double p0 = raster.getSampleDouble(x, y, b);
            final double p1 = raster2.getSampleDouble(x, y, b);

            Assert.assertEquals("x=" + x + ",y=" + y + ",b=" + b, p0, p1, 0.0);
          }
        }
      }

      // there should be exactly one
      Assert.assertFalse(it.hasNext());
    }

    // Next, query w/ only 'b' authorization. We should get
    // just the second raster back
    try (CloseableIterator<?> it =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(coverageName).addAuthorization("b").build())) {

      final GridCoverage coverage = (GridCoverage) it.next();
      final Raster raster = coverage.getRenderedImage().getData();

      Assert.assertEquals(tileSize, raster.getWidth());
      Assert.assertEquals(tileSize, raster.getHeight());

      for (int x = 0; x < tileSize; x++) {
        for (int y = 0; y < tileSize; y++) {
          for (int b = 0; b < numBands; b++) {
            final double p0 = raster.getSampleDouble(x, y, b);
            final double p1 = raster2.getSampleDouble(x, y, b);

            Assert.assertEquals("x=" + x + ",y=" + y + ",b=" + b, p0, p1, 0.0);
          }
        }
      }

      // there should be exactly one
      Assert.assertFalse(it.hasNext());
    }

    // Now, query w/ only "c" authorization. We should get
    // just the merged raster back

    try (CloseableIterator<?> it =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(coverageName).addAuthorization("c").build())) {

      final GridCoverage coverage = (GridCoverage) it.next();
      final Raster raster = coverage.getRenderedImage().getData();

      Assert.assertEquals(tileSize, raster.getWidth());
      Assert.assertEquals(tileSize, raster.getHeight());

      // the expected outcome is:
      // band 1,2,3,4,5,6 has every value set correctly, band 0 has every
      // even row set correctly and every odd row should be NaN, and band
      // 7 has the upper quadrant as NaN and the rest set
      for (int x = 0; x < tileSize; x++) {
        for (int y = 0; y < tileSize; y++) {
          for (int b = 1; b < 7; b++) {
            final double pExp = TestUtils.getTileValue(x, y, b, tileSize);
            final double pAct = raster.getSampleDouble(x, y, b);

            Assert.assertEquals("x=" + x + ",y=" + y + ",b=" + b, pExp, pAct, 0.0);
          }
          if ((y % 2) == 0) {
            final double pExp = TestUtils.getTileValue(x, y, 0, tileSize);
            final double pAct = raster.getSampleDouble(x, y, 0);

            Assert.assertEquals("x=" + x + ",y=" + y + ",b=0", pExp, pAct, 0.0);
          } else {
            final double pAct = raster.getSampleDouble(x, y, 0);
            Assert.assertEquals("x=" + x + ",y=" + y + ",b=0", Double.NaN, pAct, 0.0);
          }
          if ((x > ((tileSize * 3) / 4)) && (y > ((tileSize * 3) / 4))) {
            final double pAct = raster.getSampleDouble(x, y, 7);
            Assert.assertEquals("x=" + x + ",y=" + y + ",b=7", Double.NaN, pAct, 0.0);
          } else {
            final double pExp = TestUtils.getTileValue(x, y, 7, tileSize);
            final double pAct = raster.getSampleDouble(x, y, 7);
            Assert.assertEquals("x=" + x + ",y=" + y + ",b=7", pExp, pAct, 0.0);
          }
        }
      }

      // there should be exactly one
      Assert.assertFalse(it.hasNext());
    }

    // Finally, query w/ "a" and "b" authorization. We should get
    // just the merged raster back

    try (CloseableIterator<?> it =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(coverageName).addAuthorization(
                "a").addAuthorization("b").build())) {

      final GridCoverage coverage = (GridCoverage) it.next();
      final Raster raster = coverage.getRenderedImage().getData();

      Assert.assertEquals(tileSize, raster.getWidth());
      Assert.assertEquals(tileSize, raster.getHeight());

      // the expected outcome is:
      // band 1,2,3,4,5,6 has every value set correctly, band 0 has every
      // even row set correctly and every odd row should be NaN, and band
      // 7 has the upper quadrant as NaN and the rest set
      for (int x = 0; x < tileSize; x++) {
        for (int y = 0; y < tileSize; y++) {
          for (int b = 1; b < 7; b++) {
            final double pExp = TestUtils.getTileValue(x, y, b, tileSize);
            final double pAct = raster.getSampleDouble(x, y, b);

            Assert.assertEquals("x=" + x + ",y=" + y + ",b=" + b, pExp, pAct, 0.0);
          }
          if ((y % 2) == 0) {
            final double pExp = TestUtils.getTileValue(x, y, 0, tileSize);
            final double pAct = raster.getSampleDouble(x, y, 0);

            Assert.assertEquals("x=" + x + ",y=" + y + ",b=0", pExp, pAct, 0.0);
          } else {
            final double pAct = raster.getSampleDouble(x, y, 0);
            Assert.assertEquals("x=" + x + ",y=" + y + ",b=0", Double.NaN, pAct, 0.0);
          }
          if ((x > ((tileSize * 3) / 4)) && (y > ((tileSize * 3) / 4))) {
            final double pAct = raster.getSampleDouble(x, y, 7);
            Assert.assertEquals("x=" + x + ",y=" + y + ",b=7", Double.NaN, pAct, 0.0);
          } else {
            final double pExp = TestUtils.getTileValue(x, y, 7, tileSize);
            final double pAct = raster.getSampleDouble(x, y, 7);
            Assert.assertEquals("x=" + x + ",y=" + y + ",b=7", pExp, pAct, 0.0);
          }
        }
      }

      // there should be exactly one
      Assert.assertFalse(it.hasNext());
    }
  }

  private void ingestAndQueryMixedVisibilityRasters(
      final String coverageName,
      final int tileSize,
      final double westLon,
      final double eastLon,
      final double southLat,
      final double northLat) throws IOException {
    // Create two test rasters
    final int numBands = 8;
    final DataStore dataStore = dataStoreOptions.createDataStore();
    final RasterDataAdapter adapter =
        RasterUtils.createDataAdapterTypeDouble(
            coverageName,
            numBands,
            tileSize,
            new NoDataMergeStrategy());
    final WritableRaster raster1 = RasterUtils.createRasterTypeDouble(numBands, tileSize);
    final WritableRaster raster2 = RasterUtils.createRasterTypeDouble(numBands, tileSize);

    TestUtils.fillTestRasters(raster1, raster2, tileSize);
    dataStore.addType(adapter, TestUtils.DEFAULT_SPATIAL_INDEX);
    try (Writer writer = dataStore.createWriter(adapter.getTypeName())) {
      // Write the first raster w/ vis info
      writer.write(
          RasterUtils.createCoverageTypeDouble(
              coverageName,
              westLon,
              eastLon,
              southLat,
              northLat,
              raster1),
          getRasterVisWriter("a"));

      // Write the second raster w/ no vis info
      writer.write(
          RasterUtils.createCoverageTypeDouble(
              coverageName,
              westLon,
              eastLon,
              southLat,
              northLat,
              raster2));
    }

    // First, query w/ no authorizations. We should get
    // just the second raster back

    try (CloseableIterator<?> it =
        dataStore.query(QueryBuilder.newBuilder().addTypeName(coverageName).build())) {

      final GridCoverage coverage = (GridCoverage) it.next();
      final Raster raster = coverage.getRenderedImage().getData();

      Assert.assertEquals(tileSize, raster.getWidth());
      Assert.assertEquals(tileSize, raster.getHeight());

      for (int x = 0; x < tileSize; x++) {
        for (int y = 0; y < tileSize; y++) {
          for (int b = 0; b < numBands; b++) {
            final double p0 = raster.getSampleDouble(x, y, b);
            final double p1 = raster2.getSampleDouble(x, y, b);

            Assert.assertEquals("x=" + x + ",y=" + y + ",b=" + b, p0, p1, 0.0);
          }
        }
      }

      // there should be exactly one
      Assert.assertFalse(it.hasNext());
    }

    // Now, query w/ authorization. We should get
    // just the merged raster back

    try (CloseableIterator<?> it =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(coverageName).addAuthorization("a").build())) {

      final GridCoverage coverage = (GridCoverage) it.next();
      final Raster raster = coverage.getRenderedImage().getData();

      Assert.assertEquals(tileSize, raster.getWidth());
      Assert.assertEquals(tileSize, raster.getHeight());

      // the expected outcome is:
      // band 1,2,3,4,5,6 has every value set correctly, band 0 has every
      // even row set correctly and every odd row should be NaN, and band
      // 7 has the upper quadrant as NaN and the rest set
      for (int x = 0; x < tileSize; x++) {
        for (int y = 0; y < tileSize; y++) {
          for (int b = 1; b < 7; b++) {
            final double pExp = TestUtils.getTileValue(x, y, b, tileSize);
            final double pAct = raster.getSampleDouble(x, y, b);

            Assert.assertEquals("x=" + x + ",y=" + y + ",b=" + b, pExp, pAct, 0.0);
          }
          if ((y % 2) == 0) {
            final double pExp = TestUtils.getTileValue(x, y, 0, tileSize);
            final double pAct = raster.getSampleDouble(x, y, 0);

            Assert.assertEquals("x=" + x + ",y=" + y + ",b=0", pExp, pAct, 0.0);
          } else {
            final double pAct = raster.getSampleDouble(x, y, 0);
            Assert.assertEquals("x=" + x + ",y=" + y + ",b=0", Double.NaN, pAct, 0.0);
          }
          if ((x > ((tileSize * 3) / 4)) && (y > ((tileSize * 3) / 4))) {
            final double pAct = raster.getSampleDouble(x, y, 7);
            Assert.assertEquals("x=" + x + ",y=" + y + ",b=7", Double.NaN, pAct, 0.0);
          } else {
            final double pExp = TestUtils.getTileValue(x, y, 7, tileSize);
            final double pAct = raster.getSampleDouble(x, y, 7);
            Assert.assertEquals("x=" + x + ",y=" + y + ",b=7", pExp, pAct, 0.0);
          }
        }
      }

      // there should be exactly one
      Assert.assertFalse(it.hasNext());
    }
  }

  @Test
  public void testIngestAndQueryMixedVisibilityFields() throws IOException {
    internalTestIngestAndQueryMixedVisibilityFields();
    // because this has intermittently failed in the past, lets run it several times and make sure
    // it passes regularly
    for (int i = 1; i < 5; i++) {
      deleteAll();
      internalTestIngestAndQueryMixedVisibilityFields();
    }
  }

  public void internalTestIngestAndQueryMixedVisibilityFields() throws IOException {
    testIngestAndQueryVisibilityFields(
        dataStoreOptions,
        getFeatureVisWriter(),
        (differingVisibilities) -> Assert.assertEquals(
            "Exactly half the entries should have differing visibility",
            TOTAL_FEATURES / 2,
            differingVisibilities.getValue().intValue()),
        (storeAndStatsStore, internalAdapterIdAndSpatial) -> {
          try {
            testQueryMixed(
                storeAndStatsStore.getLeft(),
                storeAndStatsStore.getRight(),
                internalAdapterIdAndSpatial.getLeft(),
                internalAdapterIdAndSpatial.getRight());
          } catch (final IOException e) {
            LOGGER.warn("Unable to test visibility query", e);
            Assert.fail(e.getMessage());
          }
        },
        TOTAL_FEATURES);
  }

  public static void testIngestAndQueryVisibilityFields(
      final DataStorePluginOptions dataStoreOptions,
      final VisibilityHandler visibilityHandler,
      final Consumer<DifferingVisibilityCountValue> verifyDifferingVisibilities,
      final BiConsumer<Pair<DataStore, DataStatisticsStore>, Pair<Short, Boolean>> verifyQuery,
      final int totalFeatures) {
    // Specify visibility at the global level
    dataStoreOptions.createPropertyStore().setProperty(
        new DataStoreProperty(BaseDataStoreUtils.GLOBAL_VISIBILITY_PROPERTY, visibilityHandler));
    final SimpleFeatureBuilder bldr = new SimpleFeatureBuilder(getType());
    final FeatureDataAdapter adapter = new FeatureDataAdapter(getType());
    final DataStore store = dataStoreOptions.createDataStore();
    store.addType(adapter, TestUtils.DEFAULT_SPATIAL_INDEX);
    try (Writer<SimpleFeature> writer = store.createWriter(adapter.getTypeName())) {
      for (int i = 0; i < totalFeatures; i++) {
        bldr.set("a", Integer.toString(i));
        bldr.set("b", Integer.toString(i));
        bldr.set("c", Integer.toString(i));
        bldr.set("geometry", new GeometryFactory().createPoint(new Coordinate(0, 0)));
        writer.write(bldr.buildFeature(Integer.toString(i)), visibilityHandler);
      }
    }
    final DataStore dataStore = dataStoreOptions.createDataStore();
    final DataStatisticsStore statsStore = dataStoreOptions.createDataStatisticsStore();
    final InternalAdapterStore internalDataStore = dataStoreOptions.createInternalAdapterStore();
    final short internalAdapterId = internalDataStore.getAdapterId(adapter.getTypeName());

    final DifferingVisibilityCountValue count =
        dataStore.aggregateStatistics(
            StatisticQueryBuilder.newBuilder(
                DifferingVisibilityCountStatistic.STATS_TYPE).indexName(
                    TestUtils.DEFAULT_SPATIAL_INDEX.getName()).binConstraints(
                        BinConstraints.of(DataTypeBinningStrategy.getBin(adapter))).build());
    verifyDifferingVisibilities.accept(count);
    verifyQuery.accept(Pair.of(store, statsStore), Pair.of(internalAdapterId, false));
    verifyQuery.accept(Pair.of(store, statsStore), Pair.of(internalAdapterId, true));
  }


  @Test
  public void testMixedIndexFieldVisibility() {
    testMixedIndexFieldVisibility(
        dataStoreOptions,
        getMixedIndexFieldFeatureVisWriter(),
        TOTAL_FEATURES);
  }

  public static void testMixedIndexFieldVisibility(
      final DataStorePluginOptions dataStoreOptions,
      final VisibilityHandler visibilityHandler,
      final int totalFeatures) {
    final SimpleFeatureBuilder bldr = new SimpleFeatureBuilder(getType());
    final FeatureDataAdapter adapter = new FeatureDataAdapter(getType());
    final DataStore store = dataStoreOptions.createDataStore();
    store.addType(adapter, TestUtils.DEFAULT_SPATIAL_TEMPORAL_INDEX);

    // Specify visibility at the writer level
    try (Writer<SimpleFeature> writer =
        store.createWriter(adapter.getTypeName(), visibilityHandler)) {
      for (int i = 0; i < totalFeatures; i++) {
        bldr.set("t", new Date());
        bldr.set("a", A_FIELD_VALUES[i % 3]);
        bldr.set("b", B_FIELD_VALUES[i % 3]);
        bldr.set("c", C_FIELD_VALUES[i % 3]);
        bldr.set("geometry", new GeometryFactory().createPoint(new Coordinate(0, 0)));
        writer.write(bldr.buildFeature(Integer.toString(i)));
      }
    }
    final DataStore dataStore = dataStoreOptions.createDataStore();

    try (CloseableIterator<?> it =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).addAuthorization(
                "g").build())) {
      // Geometry and time both have their own visibility, so without providing the authorization
      // for both, nothing should be visible.
      Assert.assertFalse(it.hasNext());
    }

    try (CloseableIterator<?> it =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).addAuthorization(
                "t").build())) {
      // Geometry and time both have their own visibility, so without providing the authorization
      // for both, nothing should be visible.
      Assert.assertFalse(it.hasNext());
    }

    try (CloseableIterator<?> it =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).addAuthorization(
                "g").addAuthorization("t").build())) {
      // When the authorization for both time and geometry are provided, everything should be
      // visible
      Assert.assertTrue(it.hasNext());
      assertEquals(totalFeatures, Iterators.size(it));
    }
  }

  @Test
  public void testMixedVisibilityStatistics() throws IOException {
    testMixedVisibilityStatistics(dataStoreOptions, getFieldIDFeatureVisWriter(), TOTAL_FEATURES);
  }

  static final String[] A_FIELD_VALUES = new String[] {"A_1", "A_2", "A_3"};
  static final String[] B_FIELD_VALUES = new String[] {"B_1", "B_2", "B_3"};
  static final String[] C_FIELD_VALUES = new String[] {"C_1", "C_2", "C_3"};

  public static void testMixedVisibilityStatistics(
      final DataStorePluginOptions dataStoreOptions,
      final VisibilityHandler visibilityHandler,
      final int totalFeatures) {
    final SimpleFeatureBuilder bldr = new SimpleFeatureBuilder(getType());
    final FeatureDataAdapter adapter = new FeatureDataAdapter(getType());
    final DataStore store = dataStoreOptions.createDataStore();

    // Add some statistics
    final CountStatistic geomCount = new CountStatistic();
    geomCount.setTag("testGeom");
    geomCount.setTypeName(adapter.getTypeName());
    geomCount.setBinningStrategy(new FieldValueBinningStrategy("geometry"));

    final CountStatistic visCountC = new CountStatistic();
    visCountC.setTag("testC");
    visCountC.setTypeName(adapter.getTypeName());
    visCountC.setBinningStrategy(new FieldValueBinningStrategy("c"));

    final CountStatistic visCountAB = new CountStatistic();
    visCountAB.setTag("testAB");
    visCountAB.setTypeName(adapter.getTypeName());
    visCountAB.setBinningStrategy(new FieldValueBinningStrategy("a", "b"));

    // Specify visibility at the type level
    store.addType(
        adapter,
        visibilityHandler,
        Lists.newArrayList(geomCount, visCountC, visCountAB),
        TestUtils.DEFAULT_SPATIAL_INDEX);

    try (Writer<SimpleFeature> writer = store.createWriter(adapter.getTypeName())) {
      for (int i = 0; i < totalFeatures; i++) {
        bldr.set("a", A_FIELD_VALUES[i % 3]);
        bldr.set("b", B_FIELD_VALUES[i % 3]);
        bldr.set("c", C_FIELD_VALUES[i % 3]);
        bldr.set("geometry", new GeometryFactory().createPoint(new Coordinate(0, 0)));
        writer.write(bldr.buildFeature(Integer.toString(i)));
      }
    }

    // Since each field is only visible if you provide that field ID as an authorization, each
    // statistic should only reveal those counts if the appropriate authorization is set. Because
    // these statistics are using a field value binning strategy, the actual bins of the statistic
    // may reveal information that is not authorized to the user and should be hidden.
    final CountValue countCNoAuth =
        store.aggregateStatistics(
            StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).typeName(
                adapter.getTypeName()).tag("testC").build());
    assertEquals(0, countCNoAuth.getValue().longValue());

    // When providing the "c" auth, all values should be present
    final CountValue countCAuth =
        store.aggregateStatistics(
            StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).typeName(
                adapter.getTypeName()).tag("testC").addAuthorization("c").build());
    assertEquals(totalFeatures, countCAuth.getValue().longValue());

    // For the AB count statistic, the values should only be present if both "a" and "b"
    // authorizations are provided
    final CountValue countABNoAuth =
        store.aggregateStatistics(
            StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).typeName(
                adapter.getTypeName()).tag("testAB").build());
    assertEquals(0, countABNoAuth.getValue().longValue());

    final CountValue countABOnlyAAuth =
        store.aggregateStatistics(
            StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).typeName(
                adapter.getTypeName()).tag("testAB").addAuthorization("a").build());
    assertEquals(0, countABOnlyAAuth.getValue().longValue());

    final CountValue countABOnlyBAuth =
        store.aggregateStatistics(
            StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).typeName(
                adapter.getTypeName()).tag("testAB").addAuthorization("b").build());
    assertEquals(0, countABOnlyBAuth.getValue().longValue());

    final CountValue countABAuth =
        store.aggregateStatistics(
            StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).typeName(
                adapter.getTypeName()).tag("testAB").addAuthorization("a").addAuthorization(
                    "b").build());
    assertEquals(totalFeatures, countABAuth.getValue().longValue());

    // It should also work if additional authorizations are provided
    final CountValue countABCAuth =
        store.aggregateStatistics(
            StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).typeName(
                adapter.getTypeName()).tag("testAB").addAuthorization("a").addAuthorization(
                    "b").addAuthorization("c").build());
    assertEquals(totalFeatures, countABCAuth.getValue().longValue());

    // Since the geometry field has no visibility, no authorizations should be required
    final CountValue countGeomNoAuth =
        store.aggregateStatistics(
            StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).typeName(
                adapter.getTypeName()).tag("testGeom").build());
    assertEquals(totalFeatures, countGeomNoAuth.getValue().longValue());
  }

  private VisibilityHandler getFeatureVisWriter() {
    return new TestFieldVisibilityHandler();
  }

  public static class TestFieldVisibilityHandler implements VisibilityHandler {

    @Override
    public byte[] toBinary() {
      return new byte[0];
    }

    @Override
    public void fromBinary(final byte[] bytes) {}

    @Override
    public <T> String getVisibility(
        final DataTypeAdapter<T> adapter,
        final T entry,
        final String fieldName) {
      final boolean isGeom = fieldName.equals("geometry");
      final int fieldValueInt;
      if (isGeom) {
        fieldValueInt = Integer.parseInt(((SimpleFeature) entry).getID());
      } else {
        fieldValueInt = Integer.parseInt(adapter.getFieldValue(entry, fieldName).toString());
      }
      // just make half of them varied and
      // half of them the same
      if ((fieldValueInt % 2) == 0) {
        if (isGeom) {
          return "";
        }
        return fieldName;
      } else {
        // of the ones that are the same,
        // make some no bytes, some a, some
        // b, and some c
        final int switchValue = (fieldValueInt / 2) % 4;
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

  private VisibilityHandler getRasterVisWriter(final String visExpression) {
    return new GlobalVisibilityHandler(visExpression);
  }

  private VisibilityHandler getFieldIDFeatureVisWriter() {
    final Map<String, String> fieldVisibilities = Maps.newHashMap();
    fieldVisibilities.put("t", "t");
    fieldVisibilities.put("a", "a");
    fieldVisibilities.put("b", "b");
    fieldVisibilities.put("c", "c");
    fieldVisibilities.put("geometry", "");
    return new FieldMappedVisibilityHandler(fieldVisibilities);
  }

  private VisibilityHandler getMixedIndexFieldFeatureVisWriter() {
    final Map<String, String> fieldVisibilities = Maps.newHashMap();
    fieldVisibilities.put("t", "t");
    fieldVisibilities.put("geometry", "g");
    return new FieldMappedVisibilityHandler(fieldVisibilities);
  }


  private static void testQueryMixed(
      final DataStore store,
      final DataStatisticsStore statsStore,
      final short internalAdapterId,
      final boolean spatial) throws IOException {

    // you have to at least be able to see the geometry field which is wide
    // open for exactly (5 * total_Features / 8)
    // for other fields there is exactly
    testQuery(
        store,
        statsStore,
        internalAdapterId,
        new String[] {},
        spatial,
        (5 * TOTAL_FEATURES) / 8,
        ((TOTAL_FEATURES / 8) * 4) + (TOTAL_FEATURES / 2));

    for (final String auth : new String[] {"a", "b", "c"}) {
      testQuery(
          store,
          statsStore,
          internalAdapterId,
          new String[] {auth},
          spatial,
          (6 * TOTAL_FEATURES) / 8,
          (((2 * TOTAL_FEATURES) / 8) * 4) + ((2 * TOTAL_FEATURES) / 2));
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
          (7 * TOTAL_FEATURES) / 8,
          (((3 * TOTAL_FEATURES) / 8) * 4) + ((3 * TOTAL_FEATURES) / 2));
    }

    testQuery(
        store,
        statsStore,
        internalAdapterId,
        new String[] {"a", "b", "c"},
        spatial,
        TOTAL_FEATURES,
        TOTAL_FEATURES * 4);
  }

  public static void testQuery(
      final DataStore store,
      final DataStatisticsStore statsStore,
      final short internalAdapterId,
      final String[] auths,
      final boolean spatial,
      final int expectedResultCount,
      final int expectedNonNullFieldCount) {
    try (CloseableIterator<SimpleFeature> it =
        (CloseableIterator) store.query(
            QueryBuilder.newBuilder().setAuthorizations(auths).constraints(
                spatial
                    ? new ExplicitSpatialQuery(
                        new GeometryFactory().toGeometry(new Envelope(-1, 1, -1, 1)))
                    : null).build())) {
      int resultCount = 0;
      int nonNullFieldsCount = 0;
      while (it.hasNext()) {
        final SimpleFeature feature = it.next();
        for (int a = 0; a < feature.getAttributeCount(); a++) {
          if (feature.getAttribute(a) != null) {
            nonNullFieldsCount++;
          }
        }
        resultCount++;
      }
      Assert.assertEquals(
          "Unexpected result count for "
              + (spatial ? "spatial query" : "full table scan")
              + " with auths "
              + Arrays.toString(auths),
          expectedResultCount,
          resultCount);

      Assert.assertEquals(
          "Unexpected non-null field count for "
              + (spatial ? "spatial query" : "full table scan")
              + " with auths "
              + Arrays.toString(auths),
          expectedNonNullFieldCount,
          nonNullFieldsCount);
    }

    final Long count =
        (Long) store.aggregate(
            AggregationQueryBuilder.newBuilder().count(getType().getTypeName()).setAuthorizations(
                auths).constraints(
                    spatial
                        ? new ExplicitSpatialQuery(
                            new GeometryFactory().toGeometry(new Envelope(-1, 1, -1, 1)))
                        : null).build());
    Assert.assertEquals(
        "Unexpected aggregation result count for "
            + (spatial ? "spatial query" : "full table scan")
            + " with auths "
            + Arrays.toString(auths),
        expectedResultCount,
        count.intValue());

    final CountValue countStat =
        store.aggregateStatistics(
            StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).typeName(
                getType().getTypeName()).authorizations(auths).build());
    assertNotNull(countStat);
    Assert.assertEquals(
        "Unexpected stats result count for "
            + (spatial ? "spatial query" : "full table scan")
            + " with auths "
            + Arrays.toString(auths),
        expectedResultCount,
        countStat.getValue().intValue());
  }

  private static SimpleFeatureType getType() {
    final SimpleFeatureTypeBuilder bldr = new SimpleFeatureTypeBuilder();
    bldr.setName("testvis");
    final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();
    bldr.add(attributeTypeBuilder.binding(Date.class).buildDescriptor("t"));
    bldr.add(attributeTypeBuilder.binding(String.class).buildDescriptor("a"));
    bldr.add(attributeTypeBuilder.binding(String.class).buildDescriptor("b"));
    bldr.add(attributeTypeBuilder.binding(String.class).buildDescriptor("c"));
    bldr.add(attributeTypeBuilder.binding(Point.class).nillable(false).buildDescriptor("geometry"));
    return bldr.buildFeatureType();
  }
}
