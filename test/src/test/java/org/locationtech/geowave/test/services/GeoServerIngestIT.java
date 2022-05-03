/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.services;

import java.awt.image.BufferedImage;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import javax.imageio.ImageIO;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic.BoundingBoxValue;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.StatisticQueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
import org.locationtech.geowave.service.client.ConfigServiceClient;
import org.locationtech.geowave.service.client.GeoServerServiceClient;
import org.locationtech.geowave.service.client.StoreServiceClient;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.jts.geom.Coordinate;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.units.indriya.AbstractSystemOfUnits;

@RunWith(GeoWaveITRunner.class)
@Environments({Environment.SERVICES})
public class GeoServerIngestIT extends BaseServiceIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeoServerIngestIT.class);
  private static GeoServerServiceClient geoServerServiceClient;
  private static ConfigServiceClient configServiceClient;
  private static StoreServiceClient storeServiceClient;
  private static final String WORKSPACE = "testomatic";
  private static final String WMS_VERSION = "1.3";
  private static final String WMS_URL_PREFIX = "/geoserver/wms";
  private static final String REFERENCE_WMS_IMAGE_PATH =
      TestUtils.isOracleJRE() ? "src/test/resources/wms/wms-grid-oraclejdk.gif"
          : "src/test/resources/wms/wms-grid.gif";

  // TODO: create a heatmap .gif using non-Oracle JRE.
  // private static final String REFERENCE_WMS_HEATMAP_NO_SB =
  // TestUtils.isOracleJRE() ? "src/test/resources/wms/wms-heatmap-no-spatial-binning.gif"
  // : "src/test/resources/wms/wms-heatmap-no-spatial-binning.gif";

  private static final String REFERENCE_WMS_HEATMAP_CNT_AGGR =
      TestUtils.isOracleJRE() ? "src/test/resources/wms/wms-heatmap-cnt-aggr-oraclejdk.gif"
          : "src/test/resources/wms/wms-heatmap-cnt-aggr-oraclejdk.gif";

  private static final String REFERENCE_WMS_HEATMAP_SUM_AGGR =
      TestUtils.isOracleJRE() ? "src/test/resources/wms/wms-heatmap-sum-aggr-oraclejdk.gif"
          : "src/test/resources/wms/wms-heatmap-sum-aggr-oraclejdk.gif";

  private static final String testName = "GeoServerIngestIT";

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          // GeoServer and this thread have different class
          // loaders so the RocksDB "singleton" instances are not shared in
          // this JVM and GeoServer, for file-based geoserver data sources, using the REST
          // "importer" will be more handy than adding a layer by referencing the local file system
          GeoWaveStoreType.ROCKSDB,
          // filesystem sporadically fails with a null response on spatial-temporal subsampling
          // (after the spatial index is removed and the services restarted)
          GeoWaveStoreType.FILESYSTEM},
      namespace = testName)
  protected DataStorePluginOptions dataStorePluginOptions;

  private static long startMillis;

  @BeforeClass
  public static void setup() {
    geoServerServiceClient = new GeoServerServiceClient(ServicesTestEnvironment.GEOWAVE_BASE_URL);

    configServiceClient = new ConfigServiceClient(ServicesTestEnvironment.GEOWAVE_BASE_URL);
    storeServiceClient = new StoreServiceClient(ServicesTestEnvironment.GEOWAVE_BASE_URL);
    startMillis = System.currentTimeMillis();
    TestUtils.printStartOfTest(LOGGER, testName);
  }

  @AfterClass
  public static void reportTest() {
    TestUtils.printEndOfTest(LOGGER, testName, startMillis);
  }

  /**
   * Create a grid of points that are temporal.
   * 
   * @param pointBuilder {SimpleFeatureBuilder} The simple feature builder.
   * @param firstFeatureId {Integer} The first feature ID.
   * @return List<SimpleFeature> A list of simple features.
   */
  private static List<SimpleFeature> getGriddedTemporalFeatures(
      final SimpleFeatureBuilder pointBuilder,
      final int firstFeatureId) {

    int featureId = firstFeatureId;
    final Calendar cal = Calendar.getInstance();
    cal.set(1996, Calendar.JUNE, 15);
    final Date[] dates = new Date[3];
    dates[0] = cal.getTime();
    cal.set(1997, Calendar.JUNE, 15);
    dates[1] = cal.getTime();
    cal.set(1998, Calendar.JUNE, 15);
    dates[2] = cal.getTime();
    // put 3 points on each grid location with different temporal attributes
    final List<SimpleFeature> feats = new ArrayList<>();
    // extremes are close to -180,180,-90,and 90 without exactly matching
    // because coordinate transforms are illegal on the boundary
    for (int longitude = -36; longitude <= 36; longitude++) {
      for (int latitude = -18; latitude <= 18; latitude++) {
        for (int date = 0; date < dates.length; date++) {
          pointBuilder.set(
              "geometry",
              GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(longitude, latitude)));
          pointBuilder.set("TimeStamp", dates[date]);
          pointBuilder.set("Latitude", latitude);
          pointBuilder.set("Longitude", longitude);

          // Create a random number for the SIZE field for sum aggregation and statistics testing
          Random rand = new Random();
          double min = 1.0;
          Double randomNum = rand.nextDouble() + min;
          randomNum = Math.round(randomNum * 100.0) / 100.0;
          pointBuilder.set("SIZE", randomNum);

          final SimpleFeature sft = pointBuilder.buildFeature(String.valueOf(featureId));
          feats.add(sft);
          featureId++;
        }
      }
    }
    return feats;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testExamplesIngest() throws Exception {
    final DataStore ds = dataStorePluginOptions.createDataStore();
    final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();

    // Use Spherical Mercator projection coordinate system to test a projected coordinate system
    final Index spatialIdx = TestUtils.createWebMercatorSpatialIndex();

    // Set the spatial temporal index
    final Index spatialTemporalIdx = TestUtils.createWebMercatorSpatialTemporalIndex();

    @SuppressWarnings("rawtypes")
    // Create data adapter
    final GeotoolsFeatureDataAdapter fda = SimpleIngest.createDataAdapter(sft);

    // Create grid of temporal points
    final List<SimpleFeature> features =
        getGriddedTemporalFeatures(new SimpleFeatureBuilder(sft), 8675309);
    LOGGER.info(
        String.format("Beginning to ingest a uniform grid of %d features", features.size()));

    // Initialize ingested features counter
    int ingestedFeatures = 0;

    // Get a subset count
    final int featuresPer5Percent = features.size() / 20;

    // Add the type to the datastore
    ds.addType(fda, spatialIdx, spatialTemporalIdx);

    // Initialize a bounding box statistic
    final BoundingBoxStatistic mercatorBounds =
        new BoundingBoxStatistic(fda.getTypeName(), sft.getGeometryDescriptor().getLocalName());

    // Set the source CRS
    mercatorBounds.setSourceCrs(
        fda.getFeatureType().getGeometryDescriptor().getCoordinateReferenceSystem());

    // Set the destination CRS
    mercatorBounds.setDestinationCrs(TestUtils.CUSTOM_CRS);

    // Set the tag
    mercatorBounds.setTag("MERCATOR_BOUNDS");

    // Add the statistic to the datastore
    ds.addStatistic(mercatorBounds);

    // Write a subset of features to the datastore
    try (@SuppressWarnings("rawtypes")
    Writer writer = ds.createWriter(fda.getTypeName())) {
      for (final SimpleFeature feat : features) {
        writer.write(feat);
        ingestedFeatures++;
        if ((ingestedFeatures % featuresPer5Percent) == 0) {
          LOGGER.info(
              String.format(
                  "Ingested %d percent of features",
                  (ingestedFeatures / featuresPer5Percent) * 5));
        }
      }
    }

    // Get the bounding box envelope
    final BoundingBoxValue env =
        ds.aggregateStatistics(
            StatisticQueryBuilder.newBuilder(BoundingBoxStatistic.STATS_TYPE).typeName(
                fda.getTypeName()).fieldName(sft.getGeometryDescriptor().getLocalName()).tag(
                    "MERCATOR_BOUNDS").build());

    // Check the status codes of various processes
    TestUtils.assertStatusCode(
        "Should Create 'testomatic' Workspace",
        201,
        geoServerServiceClient.addWorkspace("testomatic"));
    storeServiceClient.addStoreReRoute(
        dataStorePluginOptions.getGeoWaveNamespace(),
        dataStorePluginOptions.getType(),
        dataStorePluginOptions.getGeoWaveNamespace(),
        dataStorePluginOptions.getOptionsAsMap());

    TestUtils.assertStatusCode(
        "Should Add " + dataStorePluginOptions.getGeoWaveNamespace() + " Datastore",
        201,
        geoServerServiceClient.addDataStore(
            dataStorePluginOptions.getGeoWaveNamespace(),
            "testomatic",
            dataStorePluginOptions.getGeoWaveNamespace()));

    TestUtils.assertStatusCode(
        "Should Publish '" + ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE + "' Style",
        201,
        geoServerServiceClient.addStyle(
            ServicesTestEnvironment.TEST_SLD_NO_DIFFERENCE_FILE,
            ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE));

    muteLogging();
    TestUtils.assertStatusCode(
        "Should return 400, that layer was already added",
        400,
        geoServerServiceClient.addStyle(
            ServicesTestEnvironment.TEST_SLD_NO_DIFFERENCE_FILE,
            ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE));
    unmuteLogging();

    TestUtils.assertStatusCode(
        "Should Publish '" + ServicesTestEnvironment.TEST_STYLE_NAME_MINOR_SUBSAMPLE + "' Style",
        201,
        geoServerServiceClient.addStyle(
            ServicesTestEnvironment.TEST_SLD_MINOR_SUBSAMPLE_FILE,
            ServicesTestEnvironment.TEST_STYLE_NAME_MINOR_SUBSAMPLE));

    TestUtils.assertStatusCode(
        "Should Publish '" + ServicesTestEnvironment.TEST_STYLE_NAME_MAJOR_SUBSAMPLE + "' Style",
        201,
        geoServerServiceClient.addStyle(
            ServicesTestEnvironment.TEST_SLD_MAJOR_SUBSAMPLE_FILE,
            ServicesTestEnvironment.TEST_STYLE_NAME_MAJOR_SUBSAMPLE));

    TestUtils.assertStatusCode(
        "Should Publish '" + ServicesTestEnvironment.TEST_STYLE_NAME_DISTRIBUTED_RENDER + "' Style",
        201,
        geoServerServiceClient.addStyle(
            ServicesTestEnvironment.TEST_SLD_DISTRIBUTED_RENDER_FILE,
            ServicesTestEnvironment.TEST_STYLE_NAME_DISTRIBUTED_RENDER));

    // ---------------------------------HEATMAP SLD STYLE---------------------------------------
    // Test reponse code for heatmap - no spatial binning
    TestUtils.assertStatusCode(
        "Should Publish '" + ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP + "' Style",
        201,
        geoServerServiceClient.addStyle(
            ServicesTestEnvironment.TEST_SLD_HEATMAP_FILE,
            ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP));

    // Test reponse code for heatmap CNT_AGGR
    TestUtils.assertStatusCode(
        "Should Publish '" + ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_CNT_AGGR + "' Style",
        201,
        geoServerServiceClient.addStyle(
            ServicesTestEnvironment.TEST_SLD_HEATMAP_FILE_CNT_AGGR,
            ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_CNT_AGGR));

    // Test response code for heatmap SUM_AGGR
    TestUtils.assertStatusCode(
        "Should Publish '" + ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_SUM_AGGR + "' Style",
        201,
        geoServerServiceClient.addStyle(
            ServicesTestEnvironment.TEST_SLD_HEATMAP_FILE_SUM_AGGR,
            ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_SUM_AGGR));

    // Test reponse code for heatmap CNT_STATS
    TestUtils.assertStatusCode(
        "Should Publish '" + ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_CNT_STATS + "' Style",
        201,
        geoServerServiceClient.addStyle(
            ServicesTestEnvironment.TEST_SLD_HEATMAP_FILE_CNT_STATS,
            ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_CNT_STATS));

    // Test reponse code for heatmap SUM_STATS
    TestUtils.assertStatusCode(
        "Should Publish '" + ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_SUM_STATS + "' Style",
        201,
        geoServerServiceClient.addStyle(
            ServicesTestEnvironment.TEST_SLD_HEATMAP_FILE_SUM_STATS,
            ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_SUM_STATS));
    // -----------------------------------------------------------------------------------------

    TestUtils.assertStatusCode(
        "Should Publish '" + SimpleIngest.FEATURE_NAME + "' Layer",
        201,
        geoServerServiceClient.addLayer(
            dataStorePluginOptions.getGeoWaveNamespace(),
            WORKSPACE,
            null,
            null,
            "point"));

    if (!(ds instanceof Closeable)) {
      // this is kinda hacky, but its only for the integration test - the
      // problem is that GeoServer and this thread have different class
      // loaders so the RocksDB "singleton" instances are not shared in
      // this JVM and GeoServer currently has a lock on the datastore
      // after the previous addlayer - add layer tries to lookup adapters
      // while it does not have the lock and therefore fails
      muteLogging();
      TestUtils.assertStatusCode(
          "Should return 400, that layer was already added",
          400,
          geoServerServiceClient.addLayer(
              dataStorePluginOptions.getGeoWaveNamespace(),
              WORKSPACE,
              null,
              null,
              "point"));
      unmuteLogging();
    }

    final BufferedImage biDirectRender =
        getWMSSingleTile(
            env.getMinX(),
            env.getMaxX(),
            env.getMinY(),
            env.getMaxY(),
            SimpleIngest.FEATURE_NAME,
            "point",
            920,
            360,
            null,
            true,
            false);

    final BufferedImage ref = ImageIO.read(new File(REFERENCE_WMS_IMAGE_PATH));

    // being a little lenient because of differences in O/S rendering
    TestUtils.testTileAgainstReference(biDirectRender, ref, 0, 0.07);

    BufferedImage biSubsamplingWithoutError =
        getWMSSingleTile(
            env.getMinX(),
            env.getMaxX(),
            env.getMinY(),
            env.getMaxY(),
            SimpleIngest.FEATURE_NAME,
            ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE,
            920,
            360,
            null,
            false,
            false);

    Assert.assertNotNull(ref);
    // being a little lenient because of differences in O/S rendering
    TestUtils.testTileAgainstReference(biSubsamplingWithoutError, ref, 0, 0.07);

    BufferedImage biSubsamplingWithExpectedError =
        getWMSSingleTile(
            env.getMinX(),
            env.getMaxX(),
            env.getMinY(),
            env.getMaxY(),
            SimpleIngest.FEATURE_NAME,
            ServicesTestEnvironment.TEST_STYLE_NAME_MINOR_SUBSAMPLE,
            920,
            360,
            null,
            false,
            false);
    TestUtils.testTileAgainstReference(biSubsamplingWithExpectedError, ref, 0.01, 0.15);

    BufferedImage biSubsamplingWithLotsOfError =
        getWMSSingleTile(
            env.getMinX(),
            env.getMaxX(),
            env.getMinY(),
            env.getMaxY(),
            SimpleIngest.FEATURE_NAME,
            ServicesTestEnvironment.TEST_STYLE_NAME_MAJOR_SUBSAMPLE,
            920,
            360,
            null,
            false,
            false);
    TestUtils.testTileAgainstReference(biSubsamplingWithLotsOfError, ref, 0.3, 0.4);

    final BufferedImage biDistributedRendering =
        getWMSSingleTile(
            env.getMinX(),
            env.getMaxX(),
            env.getMinY(),
            env.getMaxY(),
            SimpleIngest.FEATURE_NAME,
            ServicesTestEnvironment.TEST_STYLE_NAME_DISTRIBUTED_RENDER,
            920,
            360,
            null,
            true,
            false);
    TestUtils.testTileAgainstReference(biDistributedRendering, ref, 0, 0.07);

    // ------------------------------HEATMAP RENDERING----------------------

    // System.out.println("TEST - STARTING HEATMAP NO SPATIAL BINNING");

    // Test the count aggregation heatmap rendering (NO SPATIAL BINNING)
    // final BufferedImage refHeatMapNoSpatialBinning =
    // ImageIO.read(new File(REFERENCE_WMS_HEATMAP_NO_SB));
    //
    // final BufferedImage heatMapRenderingNoSpatBin =
    // getWMSSingleTile(
    // env.getMinX(),
    // env.getMaxX(),
    // env.getMinY(),
    // env.getMaxY(),
    // SimpleIngest.FEATURE_NAME,
    // ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP,
    // 920,
    // 360,
    // null,
    // false,
    // true);

    // Write output to a gif -- KEEP THIS HERE
    // ImageIO.write(
    // heatMapRenderingNoSpatialBinning,
    // "gif",
    // new
    // File("/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/data/heatmap-no-spatial-binning.gif"));

    // System.out.println("TEST - STARTING no spatial binning render test");
    // TestUtils.testTileAgainstReference(
    // heatMapRenderingNoSpatBin,
    // refHeatMapNoSpatialBinning,
    // 0,
    // 0.07);


    // Test the count aggregation heatmap rendering (CNT_AGGR)
    final BufferedImage refHeatMapCntAggr = ImageIO.read(new File(REFERENCE_WMS_HEATMAP_CNT_AGGR));

    final BufferedImage heatMapRenderingCntAggr =
        getWMSSingleTile(
            env.getMinX(),
            env.getMaxX(),
            env.getMinY(),
            env.getMaxY(),
            SimpleIngest.FEATURE_NAME,
            ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_CNT_AGGR,
            920,
            360,
            null,
            false,
            true);

    // Write output to a gif -- KEEP THIS HERE
    // ImageIO.write(
    // heatMapRenderingCntAggr,
    // "gif",
    // new File("/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/data/heatmap_cntAggr.gif"));
    TestUtils.testTileAgainstReference(heatMapRenderingCntAggr, refHeatMapCntAggr, 0, 0.07);


    // Test the field sum aggregation heatmap rendering (SUM_AGGR)
    final BufferedImage refHeatMapSumAggr = ImageIO.read(new File(REFERENCE_WMS_HEATMAP_SUM_AGGR));

    final BufferedImage heatMapRenderingSumAggr =
        getWMSSingleTile(
            env.getMinX(),
            env.getMaxX(),
            env.getMinY(),
            env.getMaxY(),
            SimpleIngest.FEATURE_NAME,
            ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_SUM_AGGR,
            920,
            360,
            null,
            false,
            true);

    // Write output to a gif -- KEEP THIS HERE
    // ImageIO.write(
    // heatMapRenderingSumAggr,
    // "gif",
    // new File("/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/data/heatmap_sumAggr.gif"));
    TestUtils.testTileAgainstReference(heatMapRenderingSumAggr, refHeatMapSumAggr, 0, 0.07);


    // Test the count statistics heatmap rendering (CNT_STATS)
    // final BufferedImage refHeatMapCntStats = ImageIO.read(new
    // File(REFERENCE_WMS_HEATMAP_CNT_STATS));

    // final BufferedImage heatMapRenderingCntStats =
    // getWMSSingleTile(
    // env.getMinX(),
    // env.getMaxX(),
    // env.getMinY(),
    // env.getMaxY(),
    // SimpleIngest.FEATURE_NAME,
    // ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_CNT_STATS,
    // 920,
    // 360,
    // null,
    // false,
    // true);

    // Write output to a gif -- KEEP THIS HERE
    // ImageIO.write(
    // heatMapRenderingCntStats,
    // "gif",
    // new File("/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/data/heatmap_cntStats.gif"));
    // The heatmap defaults to count aggregations since the count statistics did not yet exist in
    // the datastore
    // TestUtils.testTileAgainstReference(heatMapRenderingCntStats, refHeatMapCntAggr, 0, 0.07);


    // Test the field sum statistics heatmap rendering (SUM_STATS)
    // final BufferedImage refHeatMapSumStats = ImageIO.read(new
    // File(REFERENCE_WMS_HEATMAP_SUM_STATS));

    // final BufferedImage heatMapRenderingSumStats =
    // getWMSSingleTile(
    // env.getMinX(),
    // env.getMaxX(),
    // env.getMinY(),
    // env.getMaxY(),
    // SimpleIngest.FEATURE_NAME,
    // ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_SUM_STATS,
    // 920,
    // 360,
    // null,
    // false,
    // true);

    // Write output to a gif -- KEEP THIS HERE
    // ImageIO.write(
    // heatMapRenderingSumStats,
    // "gif",
    // new File("/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/data/heatmap_sumStats.gif"));
    // The heatmap defaults to field sum aggregations since the field sum statistics did not yet
    // exist in the datastore
    // TestUtils.testTileAgainstReference(heatMapRenderingSumStats, refHeatMapSumAggr, 0, 0.07);

    // //----------------------------------------------------------------------

    // Test subsampling with only the spatial-temporal index
    ds.removeIndex(spatialIdx.getName());
    ServicesTestEnvironment.getInstance().restartServices();

    // Test subsample rendering without error
    biSubsamplingWithoutError =
        getWMSSingleTile(
            env.getMinX(),
            env.getMaxX(),
            env.getMinY(),
            env.getMaxY(),
            SimpleIngest.FEATURE_NAME,
            ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE,
            920,
            360,
            null,
            true,
            false);
    Assert.assertNotNull(ref);
    // being a little lenient because of differences in O/S rendering
    TestUtils.testTileAgainstReference(biSubsamplingWithoutError, ref, 0, 0.071);

    // Test subsample rendering with expected error
    biSubsamplingWithExpectedError =
        getWMSSingleTile(
            env.getMinX(),
            env.getMaxX(),
            env.getMinY(),
            env.getMaxY(),
            SimpleIngest.FEATURE_NAME,
            ServicesTestEnvironment.TEST_STYLE_NAME_MINOR_SUBSAMPLE,
            920,
            360,
            null,
            true,
            false);
    TestUtils.testTileAgainstReference(biSubsamplingWithExpectedError, ref, 0.01, 0.151);

    // Test subsample rendering with lots of error
    biSubsamplingWithLotsOfError =
        getWMSSingleTile(
            env.getMinX(),
            env.getMaxX(),
            env.getMinY(),
            env.getMaxY(),
            SimpleIngest.FEATURE_NAME,
            ServicesTestEnvironment.TEST_STYLE_NAME_MAJOR_SUBSAMPLE,
            920,
            360,
            null,
            true,
            false);
    TestUtils.testTileAgainstReference(biSubsamplingWithLotsOfError, ref, 0.3, 0.41);
  }

  /**
   * Creates a buffered image using a specified process.
   * 
   * @param minX {double}
   * @param maxX {double}
   * @param minY {double}
   * @param maxY {double}
   * @param layer {String} The input grid.
   * @param style {String} The SLD to use.
   * @param width {Integer}
   * @param height {Integer}
   * @param outputFormat {String}
   * @param temporalFilter {Boolean}
   * @param spatialBinning {Boolean}
   * @return {BufferedImage} A buffered image.
   * @throws IOException
   * @throws URISyntaxException
   */
  private static BufferedImage getWMSSingleTile(
      final double minX,
      final double maxX,
      final double minY,
      final double maxY,
      final String layer,
      final String style,
      final int width,
      final int height,
      final String outputFormat,
      final boolean temporalFilter,
      final boolean spatialBinning) throws IOException, URISyntaxException { // TODO: might not need
                                                                             // spatialBinning here

    // Initiate an empty Uniform Resource Identifier (URI) builder
    final URIBuilder builder = new URIBuilder();

    // Build the URI
    builder.setScheme("http").setHost("localhost").setPort(
        ServicesTestEnvironment.JETTY_PORT).setPath(WMS_URL_PREFIX).setParameter(
            "service",
            "WMS").setParameter("version", WMS_VERSION).setParameter(
                "request",
                "GetMap").setParameter("layers", layer).setParameter(
                    "styles",
                    style == null ? "" : style).setParameter("crs", "EPSG:3857").setParameter(
                        "bbox",
                        String.format(
                            "%.2f, %.2f, %.2f, %.2f",
                            minX,
                            minY,
                            maxX,
                            maxY)).setParameter(
                                "format",
                                outputFormat == null ? "image/gif" : outputFormat).setParameter(
                                    "width",
                                    String.valueOf(width)).setParameter(
                                        "height",
                                        String.valueOf(height));

    // set a parameter if a temporal filter
    if (temporalFilter) {
      builder.setParameter(
          "cql_filter",
          "TimeStamp DURING 1997-01-01T00:00:00.000Z/1998-01-01T00:00:00.000Z");
    }

    // Build the http get command
    final HttpGet command = new HttpGet(builder.build());

    // Create the client and context
    final Pair<CloseableHttpClient, HttpClientContext> clientAndContext =
        GeoServerIT.createClientAndContext();

    // Get the client
    final CloseableHttpClient httpClient = clientAndContext.getLeft();

    // Get the context
    final HttpClientContext context = clientAndContext.getRight();

    // Execute the http command and process the response
    try {
      final HttpResponse resp = httpClient.execute(command, context);
      try (InputStream is = resp.getEntity().getContent()) {

        final BufferedImage image = ImageIO.read(is);

        Assert.assertNotNull(image);
        Assert.assertTrue(image.getWidth() == width);
        Assert.assertTrue(image.getHeight() == height);
        return image;
      }
    } finally {
      // Close the http connection
      httpClient.close();
    }
  }

  @Before
  public void setUp() {
    configServiceClient.configGeoServer("localhost:9011");
  }

  @After
  public void cleanup() {
    geoServerServiceClient.removeFeatureLayer(SimpleIngest.FEATURE_NAME);
    geoServerServiceClient.removeDataStore(dataStorePluginOptions.getGeoWaveNamespace(), WORKSPACE);
    geoServerServiceClient.removeStyle(ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE);
    geoServerServiceClient.removeStyle(ServicesTestEnvironment.TEST_STYLE_NAME_MINOR_SUBSAMPLE);
    geoServerServiceClient.removeStyle(ServicesTestEnvironment.TEST_STYLE_NAME_MAJOR_SUBSAMPLE);
    geoServerServiceClient.removeStyle(ServicesTestEnvironment.TEST_STYLE_NAME_DISTRIBUTED_RENDER);
    geoServerServiceClient.removeStyle(ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP);
    geoServerServiceClient.removeStyle(ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_CNT_AGGR);
    geoServerServiceClient.removeStyle(ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_SUM_AGGR);
    geoServerServiceClient.removeStyle(ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_CNT_STATS);
    geoServerServiceClient.removeStyle(ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_SUM_STATS);
    geoServerServiceClient.removeWorkspace(WORKSPACE);
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStorePluginOptions;
  }
}
