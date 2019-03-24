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
import org.apache.commons.math.util.MathUtils;
import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.raster.RasterUtils;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.adapter.raster.adapter.merge.RasterTileMergeStrategy;
import org.locationtech.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.store.query.IndexOnlySpatialQuery;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.coverage.grid.GridCoverage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveBasicCustomCRSRasterIT extends AbstractGeoWaveIT {
  private static final double DOUBLE_TOLERANCE = 1E-10d;

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB})
  protected DataStorePluginOptions dataStoreOptions;

  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveBasicCustomCRSRasterIT.class);
  private static final double DELTA = MathUtils.EPSILON;
  private static long startMillis;

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStoreOptions;
  }

  @BeforeClass
  public static void startTimer() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*    RUNNING GeoWaveBasicCustomCRSRasterIT       *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @AfterClass
  public static void reportTest() {
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*      FINISHED GeoWaveBasicCustomCRSRasterIT         *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @Test
  public void testNoDataMergeStrategy() throws IOException {
    final String coverageName = "testNoDataMergeStrategy";
    final int tileSize = 64; // 256 fails on bigtable exceeding maximum
    // size, 128 fails on DynamoDB exceeding
    // maximum size
    final double westLon = 0;
    final double eastLon = SpatialDimensionalityTypeProvider.DEFAULT_UNBOUNDED_CRS_INTERVAL / 8;
    final double southLat = 0;
    final double northLat = SpatialDimensionalityTypeProvider.DEFAULT_UNBOUNDED_CRS_INTERVAL / 8;
    ingestAndQueryNoDataMergeStrategy(coverageName, tileSize, westLon, eastLon, southLat, northLat);
    TestUtils.deleteAll(dataStoreOptions);
  }

  @Test
  public void testMultipleMergeStrategies() throws IOException {
    final String noDataCoverageName = "testMultipleMergeStrategies_NoDataMergeStrategy";
    final String summingCoverageName = "testMultipleMergeStrategies_SummingMergeStrategy";
    final String sumAndAveragingCoverageName =
        "testMultipleMergeStrategies_SumAndAveragingMergeStrategy";
    final int summingNumBands = 8;
    final int summingNumRasters = 4;

    final int sumAndAveragingNumBands = 12;
    final int sumAndAveragingNumRasters = 15;
    final int noDataTileSize = 64;
    final int summingTileSize = 32;
    final int sumAndAveragingTileSize = 8;
    final double minX = 0;
    final double maxX = SpatialDimensionalityTypeProvider.DEFAULT_UNBOUNDED_CRS_INTERVAL / 2048;
    final double minY = 0;
    final double maxY = SpatialDimensionalityTypeProvider.DEFAULT_UNBOUNDED_CRS_INTERVAL / 2048;

    ingestGeneralPurpose(
        summingCoverageName,
        summingTileSize,
        minX,
        maxX,
        minY,
        maxY,
        summingNumBands,
        summingNumRasters,
        new GeoWaveBasicRasterIT.SummingMergeStrategy());

    ingestGeneralPurpose(
        sumAndAveragingCoverageName,
        sumAndAveragingTileSize,
        minX,
        maxX,
        minY,
        maxY,
        sumAndAveragingNumBands,
        sumAndAveragingNumRasters,
        new GeoWaveBasicRasterIT.SumAndAveragingMergeStrategy());

    ingestNoDataMergeStrategy(noDataCoverageName, noDataTileSize, minX, maxX, minY, maxY);

    queryGeneralPurpose(
        summingCoverageName,
        summingTileSize,
        minX,
        maxX,
        minY,
        maxY,
        summingNumBands,
        summingNumRasters,
        new GeoWaveBasicRasterIT.SummingExpectedValue());

    queryNoDataMergeStrategy(noDataCoverageName, noDataTileSize);

    queryGeneralPurpose(
        sumAndAveragingCoverageName,
        sumAndAveragingTileSize,
        minX,
        maxX,
        minY,
        maxY,
        sumAndAveragingNumBands,
        sumAndAveragingNumRasters,
        new GeoWaveBasicRasterIT.SumAndAveragingExpectedValue());

    TestUtils.deleteAll(dataStoreOptions);
  }

  private void ingestAndQueryNoDataMergeStrategy(
      final String coverageName,
      final int tileSize,
      final double minX,
      final double maxX,
      final double minY,
      final double maxY) throws IOException {
    ingestNoDataMergeStrategy(coverageName, tileSize, minX, maxX, minY, maxY);
    queryNoDataMergeStrategy(coverageName, tileSize);
  }

  private void queryNoDataMergeStrategy(final String coverageName, final int tileSize)
      throws IOException {
    final DataStore dataStore = dataStoreOptions.createDataStore();

    try (CloseableIterator<?> it =
        dataStore.query(QueryBuilder.newBuilder().addTypeName(coverageName).build())) {

      // the expected outcome is:
      // band 1,2,3,4,5,6 has every value set correctly, band 0 has every
      // even row set correctly and every odd row should be NaN, and band
      // 7 has the upper quadrant as NaN and the rest set
      final GridCoverage coverage = (GridCoverage) it.next();
      final Raster raster = coverage.getRenderedImage().getData();

      Assert.assertEquals(tileSize, raster.getWidth(), DELTA);
      Assert.assertEquals(tileSize, raster.getHeight(), DELTA);
      for (int x = 0; x < tileSize; x++) {
        for (int y = 0; y < tileSize; y++) {

          for (int b = 1; b < 7; b++) {
            Assert.assertEquals(
                "x=" + x + ",y=" + y + ",b=" + b,
                TestUtils.getTileValue(x, y, b, tileSize),
                raster.getSampleDouble(x, y, b),
                DELTA);
          }
          if ((y % 2) == 0) {
            Assert.assertEquals(
                "x=" + x + ",y=" + y + ",b=0",
                TestUtils.getTileValue(x, y, 0, tileSize),
                raster.getSampleDouble(x, y, 0),
                DELTA);
          } else {
            Assert.assertEquals(
                "x=" + x + ",y=" + y + ",b=0",
                Double.NaN,
                raster.getSampleDouble(x, y, 0),
                DELTA);
          }
          if ((x > ((tileSize * 3) / 4)) && (y > ((tileSize * 3) / 4))) {
            Assert.assertEquals(
                "x=" + x + ",y=" + y + ",b=7",
                Double.NaN,
                raster.getSampleDouble(x, y, 7),
                DELTA);
          } else {
            Assert.assertEquals(
                "x=" + x + ",y=" + y + ",b=7",
                TestUtils.getTileValue(x, y, 7, tileSize),
                raster.getSampleDouble(x, y, 7),
                DELTA);
          }
        }
      }

      // there should be exactly one
      Assert.assertFalse(it.hasNext());
    }
  }

  private void ingestNoDataMergeStrategy(
      final String coverageName,
      final int tileSize,
      final double minX,
      final double maxX,
      final double minY,
      final double maxY) throws IOException {
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
    dataStore.addType(adapter, TestUtils.createWebMercatorSpatialIndex());
    try (Writer writer = dataStore.createWriter(adapter.getTypeName())) {
      writer.write(createCoverageTypeDouble(coverageName, minX, maxX, minY, maxY, raster1));
      writer.write(createCoverageTypeDouble(coverageName, minX, maxX, minY, maxY, raster2));
    }
  }

  private static GridCoverage2D createCoverageTypeDouble(
      final String coverageName,
      final double minX,
      final double maxX,
      final double minY,
      final double maxY,
      final WritableRaster raster) {
    final GridCoverageFactory gcf = CoverageFactoryFinder.getGridCoverageFactory(null);
    final org.opengis.geometry.Envelope mapExtent =
        new ReferencedEnvelope(minX, maxX, minY, maxY, TestUtils.CUSTOM_CRS);
    return gcf.create(coverageName, raster, mapExtent);
  }

  private void ingestGeneralPurpose(
      final String coverageName,
      final int tileSize,
      final double westLon,
      final double eastLon,
      final double southLat,
      final double northLat,
      final int numBands,
      final int numRasters,
      final RasterTileMergeStrategy<?> mergeStrategy) throws IOException {

    // just ingest a number of rasters
    final DataStore dataStore = dataStoreOptions.createDataStore();
    final RasterDataAdapter basicAdapter =
        RasterUtils.createDataAdapterTypeDouble(
            coverageName,
            numBands,
            tileSize,
            new NoDataMergeStrategy());
    final RasterDataAdapter mergeStrategyOverriddenAdapter =
        new RasterDataAdapter(basicAdapter, coverageName, mergeStrategy);
    basicAdapter.getMetadata().put("test-key", "test-value");
    dataStore.addType(mergeStrategyOverriddenAdapter, TestUtils.createWebMercatorSpatialIndex());
    try (Writer writer = dataStore.createWriter(mergeStrategyOverriddenAdapter.getTypeName())) {
      for (int r = 0; r < numRasters; r++) {
        final WritableRaster raster = RasterUtils.createRasterTypeDouble(numBands, tileSize);
        for (int x = 0; x < tileSize; x++) {
          for (int y = 0; y < tileSize; y++) {
            for (int b = 0; b < numBands; b++) {
              raster.setSample(x, y, b, TestUtils.getTileValue(x, y, b, r, tileSize));
            }
          }
        }
        writer.write(
            createCoverageTypeDouble(coverageName, westLon, eastLon, southLat, northLat, raster));
      }
    }
  }

  private void queryGeneralPurpose(
      final String coverageName,
      final int tileSize,
      final double westLon,
      final double eastLon,
      final double southLat,
      final double northLat,
      final int numBands,
      final int numRasters,
      final GeoWaveBasicRasterIT.ExpectedValue expectedValue) throws IOException {
    final DataStore dataStore = dataStoreOptions.createDataStore();

    try (CloseableIterator<?> it =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(coverageName).constraints(
                new IndexOnlySpatialQuery(
                    new GeometryFactory().toGeometry(
                        new Envelope(westLon, eastLon, southLat, northLat)),
                    TestUtils.CUSTOM_CRSCODE)).build())) {
      // the expected outcome is:
      // band 1,2,3,4,5,6 has every value set correctly, band 0 has every
      // even row set correctly and every odd row should be NaN, and band
      // 7 has the upper quadrant as NaN and the rest set
      final GridCoverage coverage = (GridCoverage) it.next();
      final Raster raster = coverage.getRenderedImage().getData();

      Assert.assertEquals(tileSize, raster.getWidth());
      Assert.assertEquals(tileSize, raster.getHeight());
      for (int x = 0; x < tileSize; x++) {
        for (int y = 0; y < tileSize; y++) {
          for (int b = 0; b < numBands; b++) {
            Assert.assertEquals(
                "x=" + x + ",y=" + y + ",b=" + b,
                expectedValue.getExpectedValue(x, y, b, numRasters, tileSize),
                raster.getSampleDouble(x, y, b),
                DOUBLE_TOLERANCE);
          }
        }
      }

      // there should be exactly one
      Assert.assertFalse(it.hasNext());
    }
  }
}
