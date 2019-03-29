/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.landsat;

import freemarker.template.Configuration;
import freemarker.template.Template;
import it.geosolutions.jaiext.JAIExt;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.imageio.ImageIO;
import javax.media.jai.Interpolation;
import javax.media.jai.PlanarImage;
import org.apache.commons.lang.SystemUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.referencing.operation.projection.MapProjection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.raster.plugin.GeoWaveRasterConfig;
import org.locationtech.geowave.adapter.raster.plugin.GeoWaveRasterReader;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.format.landsat8.BandFeatureIterator;
import org.locationtech.geowave.format.landsat8.Landsat8BasicCommandLineOptions;
import org.locationtech.geowave.format.landsat8.Landsat8DownloadCommandLineOptions;
import org.locationtech.geowave.format.landsat8.Landsat8RasterIngestCommandLineOptions;
import org.locationtech.geowave.format.landsat8.RasterIngestRunner;
import org.locationtech.geowave.format.landsat8.SceneFeatureIterator;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
public class CustomCRSLandsatIT extends AbstractGeoWaveIT {
  private static class RasterIngestTester extends RasterIngestRunner {
    DataStorePluginOptions dataStoreOptions;

    public RasterIngestTester(
        final DataStorePluginOptions dataStoreOptions,
        final Landsat8BasicCommandLineOptions analyzeOptions,
        final Landsat8DownloadCommandLineOptions downloadOptions,
        final Landsat8RasterIngestCommandLineOptions ingestOptions,
        final List<String> parameters) {
      super(analyzeOptions, downloadOptions, ingestOptions, parameters);
      this.dataStoreOptions = dataStoreOptions;
    }

    @Override
    protected void runInternal(final OperationParams params) throws Exception {
      // TODO Auto-generated method stub
      super.runInternal(params);
    }

    @Override
    protected void processParameters(final OperationParams params) throws Exception {
      store = dataStoreOptions.createDataStore();
      dataStorePluginOptions = dataStoreOptions;
      indices = new Index[] {new SpatialIndexBuilder().setCrs("EPSG:3857").createIndex()};
      coverageNameTemplate =
          new Template(
              "name",
              new StringReader(ingestOptions.getCoverageName()),
              new Configuration());
    }
  }

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB},
      namespace = "customcrs")
  protected DataStorePluginOptions dataStoreOptions;

  private static final String CUSTOM_REFERENCE_LANDSAT_IMAGE_PATH =
      "src/test/resources/landsat/expected_custom.png";
  private static final int MIN_PATH = 198;
  private static final int MAX_PATH = 199;
  private static final int MIN_ROW = 36;
  private static final int MAX_ROW = 36;
  private static final double WEST = -2.2;
  private static final double EAST = -1.7;
  private static final double NORTH = 34.3;
  private static final double SOUTH = 33.8;

  private static final Logger LOGGER = LoggerFactory.getLogger(CustomCRSLandsatIT.class);
  private static long startMillis;

  @BeforeClass
  public static void startTimer() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*         RUNNING CustomCRSLandsatIT             *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @AfterClass
  public static void reportTest() {
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*      FINISHED CustomCRSLandsatIT               *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStoreOptions;
  }

  @Test
  public void testMosaic() throws Exception {

    // Skip this test if we're on a Mac
    org.junit.Assume.assumeTrue(isNotMac());

    JAIExt.initJAIEXT();
    MapProjection.SKIP_SANITY_CHECKS = true;
    // just use the QA band as QA is the smallest, get the best cloud cover,
    // but ensure it is before now so no recent collection affects the test
    final Landsat8BasicCommandLineOptions analyzeOptions = new Landsat8BasicCommandLineOptions();
    analyzeOptions.setCqlFilter(
        String.format(
            "BBOX(%s,%f,%f,%f,%f) AND (%s='B2' OR %s='B3' OR %s='B4' ) AND %s <= '%s' AND path >= %d AND path <= %d AND row >= %d AND row <= %d",
            SceneFeatureIterator.SHAPE_ATTRIBUTE_NAME,
            WEST,
            SOUTH,
            EAST,
            NORTH,
            BandFeatureIterator.BAND_ATTRIBUTE_NAME,
            BandFeatureIterator.BAND_ATTRIBUTE_NAME,
            BandFeatureIterator.BAND_ATTRIBUTE_NAME,
            SceneFeatureIterator.ACQUISITION_DATE_ATTRIBUTE_NAME,
            "2016-06-01T00:00:00Z",
            MIN_PATH,
            MAX_PATH,
            MIN_ROW,
            MAX_ROW));
    analyzeOptions.setNBestPerSpatial(true);
    analyzeOptions.setNBestScenes(1);
    analyzeOptions.setUseCachedScenes(true);
    final Landsat8DownloadCommandLineOptions downloadOptions =
        new Landsat8DownloadCommandLineOptions();
    final Landsat8RasterIngestCommandLineOptions ingestOptions =
        new Landsat8RasterIngestCommandLineOptions();
    ingestOptions.setRetainImages(true);
    ingestOptions.setCreatePyramid(false);
    ingestOptions.setCreateHistogram(false);
    ingestOptions.setCoverageName("test");
    ingestOptions.setTileSize(128);
    // crop to the specified bbox
    ingestOptions.setCropToSpatialConstraint(true);
    final RasterIngestTester runner =
        new RasterIngestTester(
            dataStoreOptions,
            analyzeOptions,
            downloadOptions,
            ingestOptions,
            null);
    runner.runInternal(null);

    final StringBuilder str =
        new StringBuilder(StoreFactoryOptions.GEOWAVE_NAMESPACE_OPTION).append("=").append(
            dataStoreOptions.getGeoWaveNamespace()).append(
                ";equalizeHistogramOverride=false;interpolationOverride=").append(
                    Interpolation.INTERP_NEAREST);

    str.append(";").append(GeoWaveStoreFinder.STORE_HINT_KEY).append("=").append(
        dataStoreOptions.getType());

    final Map<String, String> options = dataStoreOptions.getOptionsAsMap();

    for (final Entry<String, String> entry : options.entrySet()) {
      if (!entry.getKey().equals(StoreFactoryOptions.GEOWAVE_NAMESPACE_OPTION)) {
        str.append(";").append(entry.getKey()).append("=").append(entry.getValue());
      }
    }
    final GeneralEnvelope queryEnvelope =
        new GeneralEnvelope(new double[] {WEST, SOUTH}, new double[] {EAST, NORTH});
    queryEnvelope.setCoordinateReferenceSystem(GeometryUtils.getDefaultCRS());

    final GeoWaveRasterReader reader =
        new GeoWaveRasterReader(GeoWaveRasterConfig.readFromConfigParams(str.toString()));
    final GridCoverage2D gridCoverage =
        reader.renderGridCoverage(
            "test",
            new Rectangle(0, 0, 1024, 1024),
            queryEnvelope,
            null,
            null,
            null);
    final RenderedImage result = gridCoverage.getRenderedImage();

    final BufferedImage img = PlanarImage.wrapRenderedImage(result).getAsBufferedImage();
    final BufferedImage swappedRedBlueImg =
        new BufferedImage(img.getWidth(), img.getHeight(), BufferedImage.TYPE_INT_RGB);
    // this is something you can use an SLD channel selector to make the
    // third channel red and the first channel blue, but in this case we are
    // manually doing it, the only purpose is to make the result natural
    // colored (B2 is blue, B3 is green, and B4 is red)
    for (int y = 0; y < img.getHeight(); y++) {
      for (int x = 0; x < img.getWidth(); x++) {
        final int rgb = img.getRGB(x, y);
        // this will swap the red and blue channel/band
        swappedRedBlueImg.setRGB(
            x,
            y,
            ((rgb & 0xff00ff00) | ((rgb & 0xff0000) >> 16) | ((rgb & 0xff) << 16)));
      }
    }

    // test the result with expected, allowing for minimal error
    final BufferedImage reference = ImageIO.read(new File(CUSTOM_REFERENCE_LANDSAT_IMAGE_PATH));
    TestUtils.testTileAgainstReference(
        swappedRedBlueImg,
        reference,
        0,
        // TODO investigate lowering this and being less lenient
        0.005);
    MapProjection.SKIP_SANITY_CHECKS = false;
  }

  private static boolean isNotMac() {
    return !SystemUtils.IS_OS_MAC;
  }
}
