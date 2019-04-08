/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.commons.math.util.MathUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.raster.RasterUtils;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.adapter.raster.adapter.RasterTile;
import org.locationtech.geowave.adapter.raster.adapter.ServerMergeableRasterTile;
import org.locationtech.geowave.adapter.raster.adapter.merge.RasterTileMergeStrategy;
import org.locationtech.geowave.adapter.raster.adapter.merge.SimpleAbstractMergeStrategy;
import org.locationtech.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import org.locationtech.geowave.core.geotime.store.query.IndexOnlySpatialQuery;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.ReaderParamsBuilder;
import org.locationtech.geowave.core.store.operations.RowReader;
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
public class GeoWaveBasicRasterIT extends AbstractGeoWaveIT {
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

  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveBasicRasterIT.class);
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
    LOGGER.warn("*    RUNNING GeoWaveBasicRasterIT       *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @AfterClass
  public static void reportTest() {
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*      FINISHED GeoWaveBasicRasterIT         *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @Test
  public void testMergeData() throws Exception {
    final String coverageName = "testMergeData_SummingMergeStrategy";

    final int maxCellSize =
        TestUtils.getTestEnvironment(dataStoreOptions.getType()).getMaxCellSize();
    final int tileSize;
    if (maxCellSize <= 64 * 1024) {
      tileSize = 24;
    } else {
      tileSize = 32;
    }
    final double westLon = 45;
    final double eastLon = 47.8125;
    final double southLat = -47.8125;
    final double northLat = -45;
    final int numBands = 8;
    final int numRasters = 4;

    ingestGeneralPurpose(
        coverageName,
        tileSize,
        westLon,
        eastLon,
        southLat,
        northLat,
        numBands,
        numRasters,
        new SummingMergeStrategy());

    // Verify correct results
    queryGeneralPurpose(
        coverageName,
        tileSize,
        westLon,
        eastLon,
        southLat,
        northLat,
        numBands,
        numRasters,
        new SummingExpectedValue());

    final DataStoreOperations operations = dataStoreOptions.createDataStoreOperations();
    final PersistentAdapterStore adapterStore = dataStoreOptions.createAdapterStore();
    final InternalAdapterStore internalAdapterStore = dataStoreOptions.createInternalAdapterStore();
    final short[] adapterIds = new short[1];
    adapterIds[0] = internalAdapterStore.getAdapterId(coverageName);
    final ReaderParams<GeoWaveRow> params =
        new ReaderParamsBuilder<>(
            TestUtils.DEFAULT_SPATIAL_INDEX,
            adapterStore,
            internalAdapterStore,
            GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER).isClientsideRowMerging(
                true).adapterIds(adapterIds).build();
    try (RowReader<GeoWaveRow> reader = operations.createReader(params)) {
      assertTrue(reader.hasNext());

      final GeoWaveRow row = reader.next();

      // Assert that the values for the row are not merged.
      // If server side libraries are enabled, the merging will be done
      // there.
      if (!dataStoreOptions.getFactoryOptions().getStoreOptions().isServerSideLibraryEnabled()) {
        assertEquals(numRasters, row.getFieldValues().length);
      }

      assertFalse(reader.hasNext());
    }

    operations.mergeData(
        TestUtils.DEFAULT_SPATIAL_INDEX,
        adapterStore,
        internalAdapterStore,
        dataStoreOptions.createAdapterIndexMappingStore(),
        dataStoreOptions.getFactoryOptions().getStoreOptions().getMaxRangeDecomposition());

    // Make sure the row was merged
    try (RowReader<GeoWaveRow> reader = operations.createReader(params)) {
      assertTrue(reader.hasNext());

      final GeoWaveRow row = reader.next();

      // Assert that the values for the row are merged.
      assertEquals(1, row.getFieldValues().length);

      assertFalse(reader.hasNext());
    }

    // Verify results are still correct
    queryGeneralPurpose(
        coverageName,
        tileSize,
        westLon,
        eastLon,
        southLat,
        northLat,
        numBands,
        numRasters,
        new SummingExpectedValue());
    TestUtils.deleteAll(dataStoreOptions);
  }

  @Test
  public void testNoDataMergeStrategy() throws IOException {
    final String coverageName = "testNoDataMergeStrategy";
    final int maxCellSize =
        TestUtils.getTestEnvironment(dataStoreOptions.getType()).getMaxCellSize();
    final int tileSize;
    if (maxCellSize <= 64 * 1024) {
      tileSize = 24;
    } else {
      tileSize = 64; // 256 fails on bigtable exceeding maximum size
                     // 128 fails on DynamoDB exceeding maximum size
                     // 64 fails on kudu exceeding maximum size
    }
    final double westLon = 0;
    final double eastLon = 45;
    final double southLat = 0;
    final double northLat = 45;
    ingestAndQueryNoDataMergeStrategy(coverageName, tileSize, westLon, eastLon, southLat, northLat);
    TestUtils.deleteAll(dataStoreOptions);
  }

  @Test
  public void testMultipleMergeStrategies() throws IOException {
    final String noDataCoverageName = "testMultipleMergeStrategies_NoDataMergeStrategy";
    final String summingCoverageName = "testMultipleMergeStrategies_SummingMergeStrategy";
    final String sumAndAveragingCoverageName =
        "testMultipleMergeStrategies_SumAndAveragingMergeStrategy";
    final int maxCellSize =
        TestUtils.getTestEnvironment(dataStoreOptions.getType()).getMaxCellSize();

    final int summingNumBands = 8;
    final int summingNumRasters = 4;

    final int sumAndAveragingNumBands = 12;
    final int sumAndAveragingNumRasters = 15;
    final int noDataTileSize;
    final int summingTileSize;
    if (maxCellSize <= 64 * 1024) {
      noDataTileSize = 24;
      summingTileSize = 24;
    } else {
      noDataTileSize = 64;
      summingTileSize = 32;
    }
    final int sumAndAveragingTileSize = 8;
    final double westLon = 45;
    final double eastLon = 47.8125;
    final double southLat = -47.8125;
    final double northLat = -45;

    ingestGeneralPurpose(
        summingCoverageName,
        summingTileSize,
        westLon,
        eastLon,
        southLat,
        northLat,
        summingNumBands,
        summingNumRasters,
        new SummingMergeStrategy());

    ingestGeneralPurpose(
        sumAndAveragingCoverageName,
        sumAndAveragingTileSize,
        westLon,
        eastLon,
        southLat,
        northLat,
        sumAndAveragingNumBands,
        sumAndAveragingNumRasters,
        new SumAndAveragingMergeStrategy());

    ingestNoDataMergeStrategy(
        noDataCoverageName,
        noDataTileSize,
        westLon,
        eastLon,
        southLat,
        northLat);

    queryGeneralPurpose(
        summingCoverageName,
        summingTileSize,
        westLon,
        eastLon,
        southLat,
        northLat,
        summingNumBands,
        summingNumRasters,
        new SummingExpectedValue());

    queryNoDataMergeStrategy(noDataCoverageName, noDataTileSize);
    queryGeneralPurpose(
        sumAndAveragingCoverageName,
        sumAndAveragingTileSize,
        westLon,
        eastLon,
        southLat,
        northLat,
        sumAndAveragingNumBands,
        sumAndAveragingNumRasters,
        new SumAndAveragingExpectedValue());

    TestUtils.deleteAll(dataStoreOptions);
  }

  private void ingestAndQueryNoDataMergeStrategy(
      final String coverageName,
      final int tileSize,
      final double westLon,
      final double eastLon,
      final double southLat,
      final double northLat) throws IOException {
    ingestNoDataMergeStrategy(coverageName, tileSize, westLon, eastLon, southLat, northLat);
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
      final double westLon,
      final double eastLon,
      final double southLat,
      final double northLat) throws IOException {
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
      writer.write(
          RasterUtils.createCoverageTypeDouble(
              coverageName,
              westLon,
              eastLon,
              southLat,
              northLat,
              raster1));
      writer.write(
          RasterUtils.createCoverageTypeDouble(
              coverageName,
              westLon,
              eastLon,
              southLat,
              northLat,
              raster2));
    }
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
    final RasterDataAdapter adapter =
        RasterUtils.createDataAdapterTypeDouble(coverageName, numBands, tileSize, mergeStrategy);
    dataStore.addType(adapter, TestUtils.DEFAULT_SPATIAL_INDEX);
    try (Writer writer = dataStore.createWriter(adapter.getTypeName())) {
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
            RasterUtils.createCoverageTypeDouble(
                coverageName,
                westLon,
                eastLon,
                southLat,
                northLat,
                raster));
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
      final ExpectedValue expectedValue) throws IOException {
    final DataStore dataStore = dataStoreOptions.createDataStore();

    try (CloseableIterator<?> it =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(coverageName).constraints(
                new IndexOnlySpatialQuery(
                    new GeometryFactory().toGeometry(
                        new Envelope(westLon, eastLon, southLat, northLat)))).build())) {
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
                TestUtils.DOUBLE_EPSILON);
          }
        }
      }

      // there should be exactly one
      Assert.assertFalse(it.hasNext());
    }
  }

  static interface ExpectedValue {
    public double getExpectedValue(int x, int y, int b, int numRasters, int tileSize);
  }

  static class SummingExpectedValue implements ExpectedValue {
    @Override
    public double getExpectedValue(
        final int x,
        final int y,
        final int b,
        final int numRasters,
        final int tileSize) {
      double sum = 0;
      for (int r = 0; r < numRasters; r++) {
        sum += TestUtils.getTileValue(x, y, b, r, tileSize);
      }
      return sum;
    }
  }

  static class SumAndAveragingExpectedValue implements ExpectedValue {
    @Override
    public double getExpectedValue(
        final int x,
        final int y,
        final int b,
        final int numRasters,
        final int tileSize) {
      double sum = 0;
      final boolean isSum = ((b % 2) == 0);

      for (int r = 0; r < numRasters; r++) {
        sum += TestUtils.getTileValue(x, y, isSum ? b : b - 1, r, tileSize);
      }
      if (isSum) {
        return sum;
      } else {
        return sum / numRasters;
      }
    }
  }

  /** this will sum up every band */
  public static class SummingMergeStrategy extends SimpleAbstractMergeStrategy<Persistable> {

    public SummingMergeStrategy() {
      super();
    }

    @Override
    protected double getSample(
        final int x,
        final int y,
        final int b,
        final double thisSample,
        final double nextSample) {
      return thisSample + nextSample;
    }
  }

  /**
   * this will sum up every even band and place the average of the previous band in each odd band
   */
  public static class SumAndAveragingMergeStrategy implements
      RasterTileMergeStrategy<MergeCounter> {

    public SumAndAveragingMergeStrategy() {
      super();
    }

    @Override
    public void merge(
        final RasterTile<MergeCounter> thisTile,
        final RasterTile<MergeCounter> nextTile,
        final SampleModel sampleModel) {
      if (nextTile instanceof ServerMergeableRasterTile) {
        final WritableRaster nextRaster =
            Raster.createWritableRaster(sampleModel, nextTile.getDataBuffer(), null);
        final WritableRaster thisRaster =
            Raster.createWritableRaster(sampleModel, thisTile.getDataBuffer(), null);
        final MergeCounter mergeCounter = thisTile.getMetadata();
        // we're merging, this is the incremented new number of merges
        final int newNumMerges =
            mergeCounter.getNumMerges() + nextTile.getMetadata().getNumMerges() + 1;

        // we've merged 1 more tile than the total number of merges (ie.
        // if we've performed 1 merge, we've seen 2 tiles)
        final int totalTiles = newNumMerges + 1;
        final int maxX = nextRaster.getMinX() + nextRaster.getWidth();
        final int maxY = nextRaster.getMinY() + nextRaster.getHeight();
        for (int x = nextRaster.getMinX(); x < maxX; x++) {
          for (int y = nextRaster.getMinY(); y < maxY; y++) {
            for (int b = 0; (b + 1) < nextRaster.getNumBands(); b += 2) {
              final double thisSample = thisRaster.getSampleDouble(x, y, b);
              final double nextSample = nextRaster.getSampleDouble(x, y, b);

              final double sum = thisSample + nextSample;
              final double average = sum / totalTiles;
              thisRaster.setSample(x, y, b, sum);
              thisRaster.setSample(x, y, b + 1, average);
            }
          }
        }
        thisTile.setMetadata(new MergeCounter(newNumMerges));
      }
    }

    @Override
    public MergeCounter getMetadata(
        final GridCoverage tileGridCoverage,
        final RasterDataAdapter dataAdapter) {
      // initial merge counter
      return new MergeCounter();
    }

    @Override
    public byte[] toBinary() {
      return new byte[] {};
    }

    @Override
    public void fromBinary(final byte[] bytes) {}
  }

  public static class MergeCounter implements Persistable {
    private int mergeCounter = 0;

    public MergeCounter() {}

    protected MergeCounter(final int mergeCounter) {
      this.mergeCounter = mergeCounter;
    }

    public int getNumMerges() {
      return mergeCounter;
    }

    @Override
    public byte[] toBinary() {
      final ByteBuffer buf = ByteBuffer.allocate(12);
      buf.putInt(mergeCounter);
      return buf.array();
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      final ByteBuffer buf = ByteBuffer.wrap(bytes);
      mergeCounter = buf.getInt();
    }
  }
}
