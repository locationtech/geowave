/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.mapreduce;

import java.awt.Color;
import java.awt.Rectangle;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Map.Entry;
import javax.media.jai.Interpolation;
import org.apache.hadoop.util.ToolRunner;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.raster.operations.ResizeMRCommand;
import org.locationtech.geowave.adapter.raster.plugin.GeoWaveRasterConfig;
import org.locationtech.geowave.adapter.raster.plugin.GeoWaveRasterReader;
import org.locationtech.geowave.adapter.raster.util.ZipUtils;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.analytic.mapreduce.operations.KdeCommand;
import org.locationtech.geowave.analytic.spark.kde.operations.KDESparkCommand;
import org.locationtech.geowave.analytic.spark.resize.ResizeSparkCommand;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.store.AddStoreCommand;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexPluginOptions;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.annotation.NamespaceOverride;
import org.locationtech.geowave.test.spark.SparkTestEnvironment;
import org.locationtech.jts.geom.Envelope;
import org.opengis.coverage.grid.GridCoverage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
@Environments({Environment.MAP_REDUCE})
@GeoWaveTestStore({
    GeoWaveStoreType.ACCUMULO,
    GeoWaveStoreType.BIGTABLE,
    GeoWaveStoreType.HBASE,
    GeoWaveStoreType.REDIS,
    // TODO ROCKSDB can sometimes throws native exceptions (hserrpid) in the Spark section, probably
    // raster resize; should be investigated
    // GeoWaveStoreType.ROCKSDB,
    GeoWaveStoreType.FILESYSTEM})
public class CustomCRSKDERasterResizeIT {
  private static final String TEST_COVERAGE_NAME_MR_PREFIX = "TEST_COVERAGE_MR";
  private static final String TEST_COVERAGE_NAME_SPARK_PREFIX = "TEST_COVERAGE_SPARK";
  private static final String TEST_RESIZE_COVERAGE_NAME_MR_PREFIX = "TEST_RESIZE_MR";
  private static final String TEST_RESIZE_COVERAGE_NAME_SPARK_PREFIX = "TEST_RESIZE_SPARK";
  private static final String TEST_COVERAGE_NAMESPACE = "mil_nga_giat_geowave_test_coverage";
  protected static final String TEST_DATA_ZIP_RESOURCE_PATH =
      TestUtils.TEST_RESOURCE_PACKAGE + "kde-testdata.zip";
  protected static final String KDE_INPUT_DIR = TestUtils.TEST_CASE_BASE + "kde_test_case/";
  private static final String KDE_SHAPEFILE_FILE = KDE_INPUT_DIR + "kde-test.shp";
  private static final double TARGET_MIN_LON = 155.12;
  private static final double TARGET_MIN_LAT = 16.07;
  private static final double TARGET_DECIMAL_DEGREES_SIZE = 0.066;
  private static final String KDE_FEATURE_TYPE_NAME = "kde-test";
  private static final int MIN_TILE_SIZE_POWER_OF_2 = 0;
  private static final int MAX_TILE_SIZE_POWER_OF_2 = 4;
  private static final int INCREMENT = 4;
  private static final int BASE_MIN_LEVEL = 15;
  private static final int BASE_MAX_LEVEL = 16;

  @NamespaceOverride(TEST_COVERAGE_NAMESPACE)
  protected DataStorePluginOptions outputDataStorePluginOptions;

  protected DataStorePluginOptions inputDataStorePluginOptions;

  private static final Logger LOGGER = LoggerFactory.getLogger(CustomCRSKDERasterResizeIT.class);
  private static long startMillis;

  @BeforeClass
  public static void extractTestFiles() throws URISyntaxException {
    ZipUtils.unZipFile(
        new File(
            CustomCRSKDERasterResizeIT.class.getClassLoader().getResource(
                TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
        TestUtils.TEST_CASE_BASE);
    startMillis = System.currentTimeMillis();
    LOGGER.warn("-------------------------------------------------");
    LOGGER.warn("*                                               *");
    LOGGER.warn("*         RUNNING CustomCRSKDERasterResizeIT    *");
    LOGGER.warn("*                                               *");
    LOGGER.warn("-------------------------------------------------");
    try {
      SparkTestEnvironment.getInstance().tearDown();
    } catch (final Exception e) {
      LOGGER.warn("Unable to tear down default spark session", e);
    }
  }

  @AfterClass
  public static void reportTest() {
    LOGGER.warn("------------------------------------------------");
    LOGGER.warn("*                                              *");
    LOGGER.warn("*      FINISHED CustomCRSKDERasterResizeIT     *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                               *");
    LOGGER.warn("*                                              *");
    LOGGER.warn("------------------------------------------------");
  }

  @After
  public void clean() throws IOException {
    TestUtils.deleteAll(inputDataStorePluginOptions);
    TestUtils.deleteAll(outputDataStorePluginOptions);
  }

  @Test
  public void testKDEAndRasterResize() throws Exception {
    TestUtils.deleteAll(inputDataStorePluginOptions);
    TestUtils.testLocalIngest(
        inputDataStorePluginOptions,
        DimensionalityType.SPATIAL,
        "EPSG:4901",
        KDE_SHAPEFILE_FILE,
        "geotools-vector",
        1);

    final File configFile = File.createTempFile("test_export", null);
    final ManualOperationParams params = new ManualOperationParams();

    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);
    final AddStoreCommand addStore = new AddStoreCommand();
    addStore.setParameters("test-in");
    addStore.setPluginOptions(inputDataStorePluginOptions);
    addStore.execute(params);
    addStore.setParameters("raster-spatial");
    addStore.setPluginOptions(outputDataStorePluginOptions);
    addStore.execute(params);

    final String outputIndexName = "raster-spatial-idx";
    final IndexPluginOptions outputIndexOptions = new IndexPluginOptions();
    outputIndexOptions.selectPlugin("spatial");
    outputIndexOptions.setName(outputIndexName);
    ((SpatialOptions) outputIndexOptions.getDimensionalityOptions()).setCrs("EPSG:4240");

    final DataStore outputDataStore = outputDataStorePluginOptions.createDataStore();

    final Index outputIndex = outputIndexOptions.createIndex(outputDataStore);
    outputDataStore.addIndex(outputIndex);

    // use the min level to define the request boundary because it is the
    // most coarse grain
    final double decimalDegreesPerCellMinLevel = 180.0 / Math.pow(2, BASE_MIN_LEVEL);
    final double cellOriginXMinLevel = Math.round(TARGET_MIN_LON / decimalDegreesPerCellMinLevel);
    final double cellOriginYMinLevel = Math.round(TARGET_MIN_LAT / decimalDegreesPerCellMinLevel);
    final double numCellsMinLevel =
        Math.round(TARGET_DECIMAL_DEGREES_SIZE / decimalDegreesPerCellMinLevel);
    final GeneralEnvelope queryEnvelope =
        new GeneralEnvelope(
            new double[] {
                // this is exactly on a tile boundary, so there will be no
                // scaling on the tile composition/rendering
                decimalDegreesPerCellMinLevel * cellOriginXMinLevel,
                decimalDegreesPerCellMinLevel * cellOriginYMinLevel},
            new double[] {
                // these values are also on a tile boundary, to avoid
                // scaling
                decimalDegreesPerCellMinLevel * (cellOriginXMinLevel + numCellsMinLevel),
                decimalDegreesPerCellMinLevel * (cellOriginYMinLevel + numCellsMinLevel)});

    final MapReduceTestEnvironment env = MapReduceTestEnvironment.getInstance();
    final String geomField =
        ((FeatureDataAdapter) inputDataStorePluginOptions.createDataStore().getTypes()[0]).getFeatureType().getGeometryDescriptor().getLocalName();
    final Envelope cqlEnv =
        JTS.transform(
            new Envelope(155.12, 155.17, 16.07, 16.12),
            CRS.findMathTransform(CRS.decode("EPSG:4326"), CRS.decode("EPSG:4901"), true));
    final String cqlStr =
        String.format(
            "BBOX(%s, %f, %f, %f, %f)",
            geomField,
            cqlEnv.getMinX(),
            cqlEnv.getMinY(),
            cqlEnv.getMaxX(),
            cqlEnv.getMaxY());
    for (int i = MIN_TILE_SIZE_POWER_OF_2; i <= MAX_TILE_SIZE_POWER_OF_2; i += INCREMENT) {
      LOGGER.warn("running mapreduce kde: " + i);
      final String tileSizeCoverageName = TEST_COVERAGE_NAME_MR_PREFIX + i;

      final KdeCommand command = new KdeCommand();
      command.setParameters("test-in", "raster-spatial");
      command.getKdeOptions().setCqlFilter(cqlStr);
      command.getKdeOptions().setOutputIndex(outputIndexName);
      command.getKdeOptions().setFeatureType(KDE_FEATURE_TYPE_NAME);
      command.getKdeOptions().setMinLevel(BASE_MIN_LEVEL);
      command.getKdeOptions().setMaxLevel(BASE_MAX_LEVEL);
      command.getKdeOptions().setMinSplits(MapReduceTestUtils.MIN_INPUT_SPLITS);
      command.getKdeOptions().setMaxSplits(MapReduceTestUtils.MAX_INPUT_SPLITS);
      command.getKdeOptions().setCoverageName(tileSizeCoverageName);
      command.getKdeOptions().setHdfsHostPort(env.getHdfs());
      command.getKdeOptions().setJobTrackerOrResourceManHostPort(env.getJobtracker());
      command.getKdeOptions().setTileSize((int) Math.pow(2, i));

      ToolRunner.run(command.createRunner(params), new String[] {});
    }
    final int numLevels = (BASE_MAX_LEVEL - BASE_MIN_LEVEL) + 1;
    final double[][][][] initialSampleValuesPerRequestSize = new double[numLevels][][][];

    LOGGER.warn("testing mapreduce kdes");
    for (int l = 0; l < numLevels; l++) {
      initialSampleValuesPerRequestSize[l] =
          testSamplesMatch(
              TEST_COVERAGE_NAME_MR_PREFIX,
              ((MAX_TILE_SIZE_POWER_OF_2 - MIN_TILE_SIZE_POWER_OF_2) / INCREMENT) + 1,
              queryEnvelope,
              new Rectangle(
                  (int) (numCellsMinLevel * Math.pow(2, l)),
                  (int) (numCellsMinLevel * Math.pow(2, l))),
              null);
    }
    for (int i = MIN_TILE_SIZE_POWER_OF_2; i <= MAX_TILE_SIZE_POWER_OF_2; i += INCREMENT) {
      LOGGER.warn("running spark kde: " + i);
      final String tileSizeCoverageName = TEST_COVERAGE_NAME_SPARK_PREFIX + i;

      final KDESparkCommand command = new KDESparkCommand();

      // We're going to override these anyway.
      command.setParameters("test-in", "raster-spatial");

      command.getKDESparkOptions().setOutputIndex(outputIndexName);
      command.getKDESparkOptions().setCqlFilter(cqlStr);
      command.getKDESparkOptions().setTypeName(KDE_FEATURE_TYPE_NAME);
      command.getKDESparkOptions().setMinLevel(BASE_MIN_LEVEL);
      command.getKDESparkOptions().setMaxLevel(BASE_MAX_LEVEL);
      command.getKDESparkOptions().setMinSplits(MapReduceTestUtils.MIN_INPUT_SPLITS);
      command.getKDESparkOptions().setMaxSplits(MapReduceTestUtils.MAX_INPUT_SPLITS);
      command.getKDESparkOptions().setCoverageName(tileSizeCoverageName);
      command.getKDESparkOptions().setMaster("local[*]");
      command.getKDESparkOptions().setTileSize((int) Math.pow(2, i));
      command.execute(params);
    }
    LOGGER.warn("testing spark kdes");
    for (int l = 0; l < numLevels; l++) {
      testSamplesMatch(
          TEST_COVERAGE_NAME_SPARK_PREFIX,
          ((MAX_TILE_SIZE_POWER_OF_2 - MIN_TILE_SIZE_POWER_OF_2) / INCREMENT) + 1,
          queryEnvelope,
          new Rectangle(
              (int) (numCellsMinLevel * Math.pow(2, l)),
              (int) (numCellsMinLevel * Math.pow(2, l))),
          initialSampleValuesPerRequestSize[l]);
    }
    // go from the original mr KDEs to a resized version using the MR command
    for (int i = MIN_TILE_SIZE_POWER_OF_2; i <= MAX_TILE_SIZE_POWER_OF_2; i += INCREMENT) {
      LOGGER.warn("running mapreduce resize: " + i);
      final String originalTileSizeCoverageName = TEST_COVERAGE_NAME_MR_PREFIX + i;
      final String resizeTileSizeCoverageName = TEST_RESIZE_COVERAGE_NAME_MR_PREFIX + i;

      final ResizeMRCommand command = new ResizeMRCommand();

      // We're going to override these anyway.
      command.setParameters("raster-spatial", "raster-spatial");

      command.getOptions().setInputCoverageName(originalTileSizeCoverageName);
      command.getOptions().setMinSplits(MapReduceTestUtils.MIN_INPUT_SPLITS);
      command.getOptions().setMaxSplits(MapReduceTestUtils.MAX_INPUT_SPLITS);
      command.setHdfsHostPort(env.getHdfs());
      command.setJobTrackerOrResourceManHostPort(env.getJobtracker());
      command.getOptions().setOutputCoverageName(resizeTileSizeCoverageName);
      command.getOptions().setIndexName(TestUtils.createWebMercatorSpatialIndex().getName());

      // due to time considerations when running the test, downsample to
      // at most 2 powers of 2 lower
      int targetRes = (MAX_TILE_SIZE_POWER_OF_2 - i);
      if ((i - targetRes) > 2) {
        targetRes = i - 2;
      }
      command.getOptions().setOutputTileSize((int) Math.pow(2, targetRes));

      ToolRunner.run(command.createRunner(params), new String[] {});
    }
    LOGGER.warn("testing mapreduce resize");
    for (int l = 0; l < numLevels; l++) {
      testSamplesMatch(
          TEST_RESIZE_COVERAGE_NAME_MR_PREFIX,
          ((MAX_TILE_SIZE_POWER_OF_2 - MIN_TILE_SIZE_POWER_OF_2) / INCREMENT) + 1,
          queryEnvelope,
          new Rectangle(
              (int) (numCellsMinLevel * Math.pow(2, l)),
              (int) (numCellsMinLevel * Math.pow(2, l))),
          initialSampleValuesPerRequestSize[l]);
    }
    // similarly go from the original spark KDEs to a resized version using the
    // Spark command
    for (int i = MIN_TILE_SIZE_POWER_OF_2; i <= MAX_TILE_SIZE_POWER_OF_2; i += INCREMENT) {
      LOGGER.warn("running spark resize: " + i);
      final String originalTileSizeCoverageName = TEST_COVERAGE_NAME_SPARK_PREFIX + i;
      final String resizeTileSizeCoverageName = TEST_RESIZE_COVERAGE_NAME_SPARK_PREFIX + i;

      final ResizeSparkCommand command = new ResizeSparkCommand();

      // We're going to override these anyway.
      command.setParameters("raster-spatial", "raster-spatial");

      command.getOptions().setInputCoverageName(originalTileSizeCoverageName);
      command.getOptions().setMinSplits(MapReduceTestUtils.MIN_INPUT_SPLITS);
      command.getOptions().setMaxSplits(MapReduceTestUtils.MAX_INPUT_SPLITS);
      command.getOptions().setOutputCoverageName(resizeTileSizeCoverageName);
      command.getOptions().setIndexName(TestUtils.createWebMercatorSpatialIndex().getName());
      command.setMaster("local[*]");

      // due to time considerations when running the test, downsample to
      // at most 2 powers of 2 lower
      int targetRes = (MAX_TILE_SIZE_POWER_OF_2 - i);
      if ((i - targetRes) > 2) {
        targetRes = i - 2;
      }
      command.getOptions().setOutputTileSize((int) Math.pow(2, targetRes));

      command.execute(params);
    }

    LOGGER.warn("testing spark resize");
    for (int l = 0; l < numLevels; l++) {
      testSamplesMatch(
          TEST_RESIZE_COVERAGE_NAME_SPARK_PREFIX,
          ((MAX_TILE_SIZE_POWER_OF_2 - MIN_TILE_SIZE_POWER_OF_2) / INCREMENT) + 1,
          queryEnvelope,
          new Rectangle(
              (int) (numCellsMinLevel * Math.pow(2, l)),
              (int) (numCellsMinLevel * Math.pow(2, l))),
          initialSampleValuesPerRequestSize[l]);
    }
  }

  private double[][][] testSamplesMatch(
      final String coverageNamePrefix,
      final int numCoverages,
      final GeneralEnvelope queryEnvelope,
      final Rectangle pixelDimensions,
      double[][][] expectedResults) throws Exception {
    final StringBuilder str =
        new StringBuilder(StoreFactoryOptions.GEOWAVE_NAMESPACE_OPTION).append("=").append(
            TEST_COVERAGE_NAMESPACE).append(
                ";equalizeHistogramOverride=false;scaleTo8Bit=false;interpolationOverride=").append(
                    Interpolation.INTERP_NEAREST);

    str.append(";").append(GeoWaveStoreFinder.STORE_HINT_KEY).append("=").append(
        outputDataStorePluginOptions.getType());

    final Map<String, String> options = outputDataStorePluginOptions.getOptionsAsMap();

    for (final Entry<String, String> entry : options.entrySet()) {
      if (!entry.getKey().equals(StoreFactoryOptions.GEOWAVE_NAMESPACE_OPTION)) {
        str.append(";").append(entry.getKey()).append("=").append(entry.getValue());
      }
    }

    final GeoWaveRasterReader reader =
        new GeoWaveRasterReader(GeoWaveRasterConfig.readFromConfigParams(str.toString()));

    queryEnvelope.setCoordinateReferenceSystem(CRS.decode("EPSG:4166", true));
    final Raster[] rasters = new Raster[numCoverages];
    int coverageCount = 0;
    for (int i = MIN_TILE_SIZE_POWER_OF_2; i <= MAX_TILE_SIZE_POWER_OF_2; i += INCREMENT) {
      final String tileSizeCoverageName = coverageNamePrefix + i;
      final GridCoverage gridCoverage =
          reader.renderGridCoverage(
              tileSizeCoverageName,
              pixelDimensions,
              queryEnvelope,
              Color.BLACK,
              null,
              null);
      final RenderedImage image = gridCoverage.getRenderedImage();
      final Raster raster = image.getData();
      rasters[coverageCount++] = raster;
    }
    boolean atLeastOneResult = expectedResults != null;
    for (int i = 0; i < numCoverages; i++) {
      final boolean initialResults = expectedResults == null;
      if (initialResults) {
        expectedResults =
            new double[rasters[i].getWidth()][rasters[i].getHeight()][rasters[i].getNumBands()];
      } else {
        Assert.assertEquals(
            "The expected width does not match the expected width for the coverage " + i,
            expectedResults.length,
            rasters[i].getWidth());
        Assert.assertEquals(
            "The expected height does not match the expected height for the coverage " + i,
            expectedResults[0].length,
            rasters[i].getHeight());
        Assert.assertEquals(
            "The expected number of bands does not match the expected bands for the coverage " + i,
            expectedResults[0][0].length,
            rasters[i].getNumBands());
      }
      long mismatchedSamples = 0;
      for (int y = 0; y < rasters[i].getHeight(); y++) {
        for (int x = 0; x < rasters[i].getWidth(); x++) {
          for (int b = 0; b < rasters[i].getNumBands(); b++) {
            final double sample = rasters[i].getSampleDouble(x, y, b);
            if (initialResults) {
              expectedResults[x][y][b] = sample;
              if (!atLeastOneResult && (sample != 0)) {
                atLeastOneResult = true;
              }
            } else {
              if ((Double.isNaN(sample) && !Double.isNaN(expectedResults[x][y][b]))
                  || (!Double.isNaN(sample) && Double.isNaN(expectedResults[x][y][b]))
                  || (Math.abs(expectedResults[x][y][b] - sample) > TestUtils.DOUBLE_EPSILON)) {
                mismatchedSamples++;
              }
            }
          }
        }
      }
      final double percentMismatch =
          mismatchedSamples
              / (double) (rasters[i].getWidth()
                  * rasters[i].getHeight()
                  * rasters[i].getNumBands());
      Assert.assertTrue(
          (percentMismatch * 100) + "% mismatch is less than 1%",
          percentMismatch < 0.01);
    }
    Assert.assertTrue("There should be at least one value that is not black", atLeastOneResult);
    return expectedResults;
  }
}
