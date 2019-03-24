/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.basic;

import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;
import org.apache.log4j.Logger;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.raster.RasterUtils;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.dimension.GeometryWrapper;
import org.locationtech.geowave.core.geotime.store.query.ExplicitSpatialQuery;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import org.locationtech.geowave.core.store.adapter.statistics.CountDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.locationtech.geowave.core.store.api.AggregationQueryBuilder;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.data.VisibilityWriter;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;
import org.locationtech.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
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
import com.aol.cyclops.util.function.QuadConsumer;

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
          GeoWaveStoreType.ROCKSDB},
      options = {"enableVisibility=true", "enableSecondaryIndexing=false"})
  protected DataStorePluginOptions dataStoreOptions;

  private static final Logger LOGGER = Logger.getLogger(AbstractGeoWaveIT.class);
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

  @Test
  public void testIngestAndQueryMixedVisibilityRasters() throws IOException {
    final String coverageName = "testMixedVisibilityRasters";
    final int tileSize = 64;
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

    TestUtils.deleteAll(dataStoreOptions);
  }

  @Test
  public void testComplexVisibility() throws IOException {
    final String coverageName = "testComplexVisibility";
    final int tileSize = 64;
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

    TestUtils.deleteAll(dataStoreOptions);
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
  public void testIngestAndQueryMixedVisibilityFields()
      throws MismatchedIndexToAdapterMapping, IOException {
    testIngestAndQueryVisibilityFields(
        dataStoreOptions,
        getFeatureVisWriter(),
        (differingVisibilities) -> Assert.assertEquals(
            "Exactly half the entries should have differing visibility",
            TOTAL_FEATURES / 2,
            differingVisibilities.getEntriesWithDifferingFieldVisibilities()),
        (store, statsStore, internalAdapterId, spatial) -> {
          try {
            testQueryMixed(store, statsStore, internalAdapterId, spatial);
          } catch (final IOException e) {
            LOGGER.warn("Unable to test visibility query", e);
            Assert.fail(e.getMessage());
          }
        },
        TOTAL_FEATURES);
  }

  public static void testIngestAndQueryVisibilityFields(
      final DataStorePluginOptions dataStoreOptions,
      final VisibilityWriter<SimpleFeature> visibilityWriter,
      final Consumer<DifferingFieldVisibilityEntryCount> verifyDifferingVisibilities,
      final QuadConsumer<DataStore, DataStatisticsStore, Short, Boolean> verifyQuery,
      int totalFeatures) {
    final SimpleFeatureBuilder bldr = new SimpleFeatureBuilder(getType());
    final FeatureDataAdapter adapter = new FeatureDataAdapter(getType());
    final DataStore store = dataStoreOptions.createDataStore();
    store.addType(adapter, TestUtils.DEFAULT_SPATIAL_INDEX);
    try (Writer writer = store.createWriter(adapter.getTypeName())) {
      for (int i = 0; i < totalFeatures; i++) {
        bldr.set("a", Integer.toString(i));
        bldr.set("b", Integer.toString(i));
        bldr.set("c", Integer.toString(i));
        bldr.set("geometry", new GeometryFactory().createPoint(new Coordinate(0, 0)));
        writer.write(bldr.buildFeature(Integer.toString(i)), visibilityWriter);
      }
    }
    final DataStatisticsStore statsStore = dataStoreOptions.createDataStatisticsStore();
    final InternalAdapterStore internalDataStore = dataStoreOptions.createInternalAdapterStore();
    final short internalAdapterId = internalDataStore.getAdapterId(adapter.getTypeName());

    final StatisticsId statsId =
        DifferingFieldVisibilityEntryCount.STATS_TYPE.newBuilder().indexName(
            TestUtils.DEFAULT_SPATIAL_INDEX.getName()).build().getId();
    try (CloseableIterator<DifferingFieldVisibilityEntryCount> differingVisibilitiesIt =
        (CloseableIterator) statsStore.getDataStatistics(
            internalAdapterId,
            statsId.getExtendedId(),
            statsId.getType())) {
      final DifferingFieldVisibilityEntryCount differingVisibilities =
          differingVisibilitiesIt.next();
      verifyDifferingVisibilities.accept(differingVisibilities);
    }
    verifyQuery.accept(store, statsStore, internalAdapterId, false);
    verifyQuery.accept(store, statsStore, internalAdapterId, true);
    TestUtils.deleteAll(dataStoreOptions);
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

            final boolean isGeom = fieldId.equals(GeometryWrapper.DEFAULT_GEOMETRY_FIELD_NAME);
            final int fieldValueInt;
            if (isGeom) {
              fieldValueInt = Integer.parseInt(rowValue.getID());
            } else {
              fieldValueInt = Integer.parseInt(fieldValue.toString());
            }
            // just make half of them varied and
            // half of them the same
            if ((fieldValueInt % 2) == 0) {
              if (isGeom) {
                return new byte[] {};
              }
              return fieldId.getBytes();
            } else {
              // of the ones that are the same,
              // make some no bytes, some a, some
              // b, and some c
              final int switchValue = (fieldValueInt / 2) % 4;
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
          }
        };
      }
    };
  }

  private VisibilityWriter<GridCoverage> getRasterVisWriter(final String visExpression) {
    return new VisibilityWriter<GridCoverage>() {
      @Override
      public FieldVisibilityHandler<GridCoverage, Object> getFieldVisibilityHandler(
          final String fieldId) {
        return new FieldVisibilityHandler<GridCoverage, Object>() {
          @Override
          public byte[] getVisibility(
              final GridCoverage rowValue,
              final String fieldId,
              final Object fieldValue) {
            return new ByteArray(visExpression).getBytes();
          }
        };
      }
    };
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
      final int expectedNonNullFieldCount) throws IOException {
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

    try (final CloseableIterator<CountDataStatistics<?>> statsIt =
        (CloseableIterator) statsStore.getDataStatistics(
            internalAdapterId,
            CountDataStatistics.STATS_TYPE,
            auths)) {
      final CountDataStatistics<?> stats = statsIt.next();
      Assert.assertEquals(
          "Unexpected stats result count for "
              + (spatial ? "spatial query" : "full table scan")
              + " with auths "
              + Arrays.toString(auths),
          expectedResultCount,
          stats.getCount());
    }
  }

  private static SimpleFeatureType getType() {
    final SimpleFeatureTypeBuilder bldr = new SimpleFeatureTypeBuilder();
    bldr.setName("testvis");
    final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();
    bldr.add(attributeTypeBuilder.binding(String.class).buildDescriptor("a"));
    bldr.add(attributeTypeBuilder.binding(String.class).buildDescriptor("b"));
    bldr.add(attributeTypeBuilder.binding(String.class).buildDescriptor("c"));
    bldr.add(attributeTypeBuilder.binding(Point.class).nillable(false).buildDescriptor("geometry"));
    return bldr.buildFeatureType();
  }
}
