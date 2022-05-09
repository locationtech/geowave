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
import java.util.HashSet;
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
import org.junit.Ignore;
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

@RunWith(GeoWaveITRunner.class)
@Environments({Environment.SERVICES})
public class GeoServerIngestIT extends BaseServiceIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeoServerIngestIT.class);
  private static GeoServerServiceClient geoServerServiceClient;
  private static ConfigServiceClient configServiceClient;
  private static StoreServiceClient storeServiceClient;
  private static final String WORKSPACE = "testomatic";
  private static final String WORKSPACE2 = "testomatic2";
  private static final String WMS_VERSION = "1.3";
  private static final String WMS_URL_PREFIX = "/geoserver/wms";
  private static final String REFERENCE_WMS_IMAGE_PATH =
      TestUtils.isOracleJRE() ? "src/test/resources/wms/wms-grid-oraclejdk.gif"
          : "src/test/resources/wms/wms-grid.gif";

  // TODO: create a heatmap .gif using non-Oracle JRE.
  private static final String REFERENCE_WMS_HEATMAP_NO_SB =
      TestUtils.isOracleJRE() ? "src/test/resources/wms/X-wms-heatmap-no-spat-bin-oraclejdk.gif"
          : "src/test/resources/wms/X-wms-heatmap-no-spat-bin.gif";

  private static final String REFERENCE_WMS_HEATMAP_CNT_AGGR =
      TestUtils.isOracleJRE() ? "src/test/resources/wms/X-wms-heatmap-cnt-aggr-oraclejdk.gif"
          : "src/test/resources/wms/X-wms-heatmap-cnt-aggr.gif";

  private static final String REFERENCE_WMS_HEATMAP_SUM_AGGR =
      TestUtils.isOracleJRE() ? "src/test/resources/wms/X-wms-heatmap-sum-aggr-oraclejdk.gif"
          : "src/test/resources/wms/X-wms-heatmap-sum-aggr.gif";

  private static final String REFERENCE_WMS_HEATMAP_CNT_STATS =
      TestUtils.isOracleJRE() ? "src/test/resources/wms/X-wms-heatmap-cnt-stats-oraclejdk.gif"
          : "src/test/resources/wms/X-wms-heatmap-cnt-stats.gif";

  private static final String REFERENCE_WMS_HEATMAP_SUM_STATS =
      TestUtils.isOracleJRE() ? "src/test/resources/wms/X-wms-heatmap-sum-stats-oraclejdk.gif"
          : "src/test/resources/wms/X-wms-heatmap-sum-stats.gif";

  private static final String REFERENCE_WMS_HEATMAP_CNT_AGGR_ZOOM =
      TestUtils.isOracleJRE() ? "src/test/resources/wms/X-wms-heatmap-cnt-aggr-zoom-oraclejdk.gif"
          : "src/test/resources/wms/X-wms-heatmap-cnt-aggr-zoom.gif";

  private static final String REFERENCE_WMS_HEATMAP_SUM_AGGR_ZOOM =
      TestUtils.isOracleJRE() ? "src/test/resources/wms/X-wms-heatmap-sum-aggr-zoom-oraclejdk.gif"
          : "src/test/resources/wms/X-wms-heatmap-sum-aggr-zoom.gif";


  private static final String REFERENCE_WMS_HEATMAP_NO_SB_WGS84 =
      TestUtils.isOracleJRE()
          ? "src/test/resources/wms/W-wms-heatmap-no-spat-bin-wgs84-oraclejdk.gif"
          : "src/test/resources/wms/W-wms-heatmap-no-spat-bin-wgs84.gif";

  private static final String REFERENCE_WMS_HEATMAP_CNT_AGGR_WGS84 =
      TestUtils.isOracleJRE() ? "src/test/resources/wms/W-wms-heatmap-cnt-aggr-wgs84-oraclejdk.gif"
          : "src/test/resources/wms/W-wms-heatmap-cnt-aggr-wgs84.gif";

  private static final String REFERENCE_WMS_HEATMAP_CNT_AGGR_ZOOM_WGS84 =
      TestUtils.isOracleJRE()
          ? "src/test/resources/wms/wms-heatmap-cnt-aggr-wgs84-zoom-oraclejdk.gif"
          : "src/test/resources/wms/wms-heatmap-cnt-aggr-wgs84-zoom.gif";

  private static final String REFERENCE_WMS_HEATMAP_SUM_AGGR_ZOOM_WGS84 =
      TestUtils.isOracleJRE()
          ? "src/test/resources/wms/W-wms-heatmap-sum-aggr-wgs84-zoom-oraclejdk.gif"
          : "src/test/resources/wms/W-wms-heatmap-sum-aggr-wgs84-zoom.gif";


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
          // "importer" will be more handy than adding a layer by referencing the local
          // file system
          GeoWaveStoreType.ROCKSDB,
          // filesystem sporadically fails with a null response on spatial-temporal
          // subsampling
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

          // Add value of 2 to the SIZE field for sum aggregation and statistics
          pointBuilder.set("SIZE", 2);

          final SimpleFeature sft = pointBuilder.buildFeature(String.valueOf(featureId));
          feats.add(sft);
          featureId++;
        }
      }
    }
    return feats;
  }


  @SuppressWarnings({"unchecked", "rawtypes"})
  private ArrayList<Double> getZoomedCoordinates(BoundingBoxValue env) {

    ArrayList<Double> coordSet = new ArrayList();

    double widthX = env.getWidth() / 64;
    double heightY = env.getHeight() / 64;
    double centerX = (env.getMinX() + env.getMaxX()) / 2;
    double centerY = (env.getMinY() + env.getMaxY()) / 2;

    Double minX = centerX - widthX;
    Double maxX = centerX + widthX;
    Double minY = centerY - heightY;
    Double maxY = centerY + heightY;

    coordSet.add(minX);
    coordSet.add(maxX);
    coordSet.add(minY);
    coordSet.add(maxY);

    return coordSet;
  }

  /**
   * Test projected data.
   *
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testExamplesIngestProjected() throws Exception {
    final DataStore ds = dataStorePluginOptions.createDataStore();
    final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();

    System.out.println("TEST - IS ORACLE JRE? " + TestUtils.isOracleJRE());

    // Keep these Booleans for local testing purposes
    Boolean runNoSpatialBinning = true;
    Boolean runCntAggr = true;
    Boolean runCntAggrZoom = true;
    Boolean runSumAggr = true;
    Boolean runSumAggrZoom = true;
    Boolean runCntStats = true;
    Boolean runSumStats = true;

    // Use Web Mercator projection
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
    int ingestedFeatures = 0;
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

    // ----------------HEATMAP RESPONSE TESTS------------------------------------
    // Test response code for heatmap - no spatial binning
    TestUtils.assertStatusCode(
        "Should Publish '" + ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP + "' Style",
        201,
        geoServerServiceClient.addStyle(
            ServicesTestEnvironment.TEST_SLD_HEATMAP_FILE,
            ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP));

    // Test response code for heatmap CNT_AGGR
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

    // Test response code for heatmap CNT_STATS
    TestUtils.assertStatusCode(
        "Should Publish '" + ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_CNT_STATS + "' Style",
        201,
        geoServerServiceClient.addStyle(
            ServicesTestEnvironment.TEST_SLD_HEATMAP_FILE_CNT_STATS,
            ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_CNT_STATS));

    // Test response code for heatmap SUM_STATS
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
            true);

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
            true);

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
            true);
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
            true);
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
            true);
    TestUtils.testTileAgainstReference(biDistributedRendering, ref, 0, 0.07);

    // ------------------------------HEATMAP PROJECTED RENDERING----------------------

    // Get coordinates for zoomed-in tests
    ArrayList<Double> zoomCoords = getZoomedCoordinates(env);
    System.out.println("TEST - ZOOM COORDS: " + zoomCoords);

    Double minXzoom = zoomCoords.get(0);
    Double maxXzoom = zoomCoords.get(1);
    Double minYzoom = zoomCoords.get(2);
    Double maxYzoom = zoomCoords.get(3);

    System.out.println("TEST - MINX: " + minXzoom);
    System.out.println("TEST - MAXX: " + maxXzoom);
    System.out.println("TEST - MINY: " + minYzoom);
    System.out.println("TEST - MAXY: " + maxYzoom);

    // Test the count aggregation heatmap rendering (NO SPATIAL BINNING)
    if (runNoSpatialBinning) {
      BufferedImage heatMapRenderingNoSpatBin;

      System.out.println("TEST - STARTING NO SPATIAL BINNING");

      heatMapRenderingNoSpatBin =
          getWMSSingleTile(
              env.getMinX(),
              env.getMaxX(),
              env.getMinY(),
              env.getMaxY(),
              SimpleIngest.FEATURE_NAME,
              ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP,
              920,
              360,
              null,
              false,
              true);

      // ImageIO.write(
      // heatMapRenderingNoSpatBin,
      // "gif",
      // new
      // File("/home/me/Repos/GEOWAVE/geowave/test/src/test/resources/wms/X-wms-heatmap-no-spat-bin.gif"));
      // "/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/src/test/resources/wms/X-wms-heatmap-no-spat-bin-oraclejdk.gif"));

      final BufferedImage refHeatMapNoSpatialBinning =
          ImageIO.read(new File(REFERENCE_WMS_HEATMAP_NO_SB));

      TestUtils.testTileAgainstReference(
          heatMapRenderingNoSpatBin,
          refHeatMapNoSpatialBinning,
          0,
          0.07);
    }

    // Test the count aggregation heatmap rendering (CNT_AGGR)
    System.out.println("TEST - STARTING CNT AGGR ZOOM - PROJECTED");
    if (runCntAggr) {
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

      // ImageIO.write(
      // heatMapRenderingCntAggr,
      // "gif",
      // new
      // File("/home/me/Repos/GEOWAVE/geowave/test/src/test/resources/wms/X-wms-heatmap-cnt-aggr.gif"));
      // "/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/src/test/resources/wms/X-wms-heatmap-cnt-aggr-oraclejdk.gif"));

      final BufferedImage refHeatMapCntAggr =
          ImageIO.read(new File(REFERENCE_WMS_HEATMAP_CNT_AGGR));

      TestUtils.testTileAgainstReference(heatMapRenderingCntAggr, refHeatMapCntAggr, 0, 0.07);

    }

    if (runCntAggrZoom) {
      System.out.println("TEST - STARTING ZOOMED-IN VERSION");

      // Test zoomed-in version of heatmap count aggregation
      final BufferedImage heatMapRenderingCntAggrZoom =
          getWMSSingleTile(
              minXzoom,
              maxXzoom,
              minYzoom,
              maxYzoom,
              SimpleIngest.FEATURE_NAME,
              ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_CNT_AGGR,
              920,
              360,
              null,
              false,
              true);

      // ImageIO.write(
      // heatMapRenderingCntAggrZoom,
      // "gif",
      // new
      // File("/home/me/Repos/GEOWAVE/geowave/test/src/test/resources/wms/X-wms-heatmap-cnt-aggr-zoom.gif"));
      // "/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/src/test/resources/wms/X-wms-heatmap-cnt-aggr-zoom-oraclejdk.gif"));

      final BufferedImage refHeatMapCntAggrZoom =
          ImageIO.read(new File(REFERENCE_WMS_HEATMAP_CNT_AGGR_ZOOM));

      TestUtils.testTileAgainstReference(
          heatMapRenderingCntAggrZoom,
          refHeatMapCntAggrZoom,
          0,
          0.07);

    }

    // Test the field sum aggregation heatmap rendering (SUM_AGGR)
    if (runSumAggr) {
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

      // ImageIO.write(
      // heatMapRenderingSumAggr,
      // "gif",
      // new
      // File("/home/me/Repos/GEOWAVE/geowave/test/src/test/resources/wms/X-wms-heatmap-sum-aggr.gif"));
      // "/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/src/test/resources/wms/X-wms-heatmap-sum-aggr-oraclejdk.gif"));

      final BufferedImage refHeatMapSumAggr =
          ImageIO.read(new File(REFERENCE_WMS_HEATMAP_SUM_AGGR));

      TestUtils.testTileAgainstReference(heatMapRenderingSumAggr, refHeatMapSumAggr, 0, 0.07);

    }

    if (runSumAggrZoom) {
      System.out.println("TEST - STARTING ZOOMED-IN VERSION SUM_AGGR");

      // Test zoomed-in version of heatmap sum aggregation
      final BufferedImage heatMapRenderingSumAggrZoom =
          getWMSSingleTile(
              minXzoom,
              maxXzoom,
              minYzoom,
              maxYzoom,
              SimpleIngest.FEATURE_NAME,
              ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_SUM_AGGR,
              920,
              360,
              null,
              false,
              true);

      // ImageIO.write(
      // heatMapRenderingSumAggrZoom,
      // "gif",
      // new
      // File("/home/me/Repos/GEOWAVE/geowave/test/src/test/resources/wms/X-wms-heatmap-sum-aggr-zoom.gif"));
      // "/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/src/test/resources/wms/X-wms-heatmap-sum-aggr-zoom-oraclejdk.gif"));

      final BufferedImage refHeatMapSumAggrZoom =
          ImageIO.read(new File(REFERENCE_WMS_HEATMAP_SUM_AGGR_ZOOM));

      TestUtils.testTileAgainstReference(
          heatMapRenderingSumAggrZoom,
          refHeatMapSumAggrZoom,
          0.0,
          0.07);

    }

    // Test the count statistics heatmap rendering
    if (runCntStats) {

      final BufferedImage heatMapRenderingCntStats =
          getWMSSingleTile(
              env.getMinX(),
              env.getMaxX(),
              env.getMinY(),
              env.getMaxY(),
              SimpleIngest.FEATURE_NAME,
              ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_CNT_STATS,
              920,
              360,
              null,
              false,
              true);

      // ImageIO.write(
      // heatMapRenderingCntStats,
      // "gif",
      // new
      // File("/home/me/Repos/GEOWAVE/geowave/test/src/test/resources/wms/X-wms-heatmap-cnt-stats.gif"));
      // "/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/src/test/resources/wms/X-wms-heatmap-cnt-stats-oraclejdk.gif"));

      final BufferedImage refHeatMapCntStats =
          ImageIO.read(new File(REFERENCE_WMS_HEATMAP_CNT_STATS));

      TestUtils.testTileAgainstReference(heatMapRenderingCntStats, refHeatMapCntStats, 0, 0.07);

    }

    // Test the sum statistics heatmap rendering
    if (runSumStats) {
      final BufferedImage heatMapRenderingSumStats =
          getWMSSingleTile(
              env.getMinX(),
              env.getMaxX(),
              env.getMinY(),
              env.getMaxY(),
              SimpleIngest.FEATURE_NAME,
              ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_SUM_STATS,
              920,
              360,
              null,
              false,
              true);

      // ImageIO.write(
      // heatMapRenderingSumStats,
      // "gif",
      // new
      // File("/home/me/Repos/GEOWAVE/geowave/test/src/test/resources/wms/X-wms-heatmap-sum-stats.gif"));
      // "/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/src/test/resources/wms/X-wms-heatmap-sum-stats-oraclejdk.gif"));

      final BufferedImage refHeatMapSumStats =
          ImageIO.read(new File(REFERENCE_WMS_HEATMAP_SUM_STATS));

      TestUtils.testTileAgainstReference(heatMapRenderingSumStats, refHeatMapSumStats, 0, 0.07);

    }
    // -------------------------------------------------------------------------

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
            true);
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
            true);
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
            true);
    TestUtils.testTileAgainstReference(biSubsamplingWithLotsOfError, ref, 0.3, 0.41);
  }

  /**
   * Run data that is unprojected (has regular GCS WGS84, but no projection)
   * 
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testExamplesIngestUnProjected() throws Exception {
    final DataStore ds = dataStorePluginOptions.createDataStore();
    final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();

    System.out.println("TEST - STARTING UNPROJECTED TESTS!");

    // Set booleans for WGS84 tests
    Boolean runNoSpatialBinningWGS84 = true;
    Boolean runCntAggrWGS84 = true;
    Boolean runCntAggrZoomedWGS84 = true;
    Boolean runSumAggrZoomedWGS84 = true;

    // Use WGS84 coordinate system
    final Index spatialIdx = TestUtils.createWGS84SpatialIndex();

    // Set the spatial temporal index
    final Index spatialTemporalIdx = TestUtils.createWGS84SpatialTemporalIndex();

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
    final BoundingBoxStatistic wgs84Bounds =
        new BoundingBoxStatistic(fda.getTypeName(), sft.getGeometryDescriptor().getLocalName());

    // Set the source CRS
    wgs84Bounds.setSourceCrs(
        fda.getFeatureType().getGeometryDescriptor().getCoordinateReferenceSystem());

    // Set the destination CRS
    wgs84Bounds.setDestinationCrs(TestUtils.CUSTOM_CRS_WGS84);

    // Set the tag
    wgs84Bounds.setTag("WGS84_BOUNDS");

    // Add the statistic to the datastore
    ds.addStatistic(wgs84Bounds);

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
                    "WGS84_BOUNDS").build());

    // Check the status codes of various processes
    TestUtils.assertStatusCode(
        "Should Create 'testomatic2' Workspace",
        201,
        geoServerServiceClient.addWorkspace("testomatic2"));
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
            "testomatic2",
            dataStorePluginOptions.getGeoWaveNamespace()));


    // ----------------HEATMAP SLD RESPONSE TESTS------------------------------------

    // Test response code for heatmap NO SPATIAL BINNING
    TestUtils.assertStatusCode(
        "Should Publish '" + ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP + "' Style",
        201,
        geoServerServiceClient.addStyle(
            ServicesTestEnvironment.TEST_SLD_HEATMAP_FILE,
            ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP));

    // Test response code for heatmap CNT_AGGR
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

    // -----------------------------------------------------------------------------------------

    TestUtils.assertStatusCode(
        "Should Publish '" + SimpleIngest.FEATURE_NAME + "' Layer",
        201,
        geoServerServiceClient.addLayer(
            dataStorePluginOptions.getGeoWaveNamespace(),
            WORKSPACE2,
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
              WORKSPACE2,
              null,
              null,
              "point"));
      unmuteLogging();
    }

    // ------------------------------HEATMAP WGS84 RENDERING----------------------

    // Get coordinates for zoomed-in tests
    ArrayList<Double> zoomCoords = getZoomedCoordinates(env);
    System.out.println("TEST - ZOOM COORDS: " + zoomCoords);

    Double minXzoom = zoomCoords.get(0);
    Double maxXzoom = zoomCoords.get(1);
    Double minYzoom = zoomCoords.get(2);
    Double maxYzoom = zoomCoords.get(3);

    System.out.println("TEST - MINX: " + minXzoom);
    System.out.println("TEST - MAXX: " + maxXzoom);
    System.out.println("TEST - MINY: " + minYzoom);
    System.out.println("TEST - MAXY: " + maxYzoom);

    // Test the count aggregation heatmap rendering (NO SPATIAL BINNING)
    System.out.println("TEST - START NO SPATIAL BINNING WGS84");
    if (runNoSpatialBinningWGS84) {
      BufferedImage heatMapRenderingNoSpatBinWGS84;

      heatMapRenderingNoSpatBinWGS84 =
          getWMSSingleTile(
              env.getMinX(),
              env.getMaxX(),
              env.getMinY(),
              env.getMaxY(),
              SimpleIngest.FEATURE_NAME,
              ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP,
              920,
              360,
              null,
              false,
              false);

      // ImageIO.write(
      // heatMapRenderingNoSpatBinWGS84,
      // "gif",
      // new
      // File("/home/me/Repos/GEOWAVE/geowave/test/src/test/resources/wms/W-wms-heatmap-no-spat-bin-wgs84.gif"));
      // "/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/src/test/resources/wms/W-wms-heatmap-no-spat-bin-wgs84-oraclejdk.gif"));

      final BufferedImage refHeatMapNoSpatialBinningWGS84 =
          ImageIO.read(new File(REFERENCE_WMS_HEATMAP_NO_SB_WGS84));
      TestUtils.testTileAgainstReference(
          heatMapRenderingNoSpatBinWGS84,
          refHeatMapNoSpatialBinningWGS84,
          0,
          0.07);

    }

    // Test the count aggregation heatmap rendering WGS84 (CNT_AGGR)
    if (runCntAggrWGS84) {
      // TODO: if this is run, centroid at 0, 0 cannot be projected at full extent.
      final BufferedImage heatMapRenderingCntAggrWGS84 =
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
              false);


      // ImageIO.write(
      // heatMapRenderingCntAggrWGS84,
      // "gif",
      // new
      // File("/home/me/Repos/GEOWAVE/geowave/test/src/test/resources/wms/W-wms-heatmap-cnt-aggr-wgs84.gif"));
      // "/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/src/test/resources/wms/W-wms-heatmap-cnt-aggr-wgs84-oraclejdk.gif"));

      final BufferedImage refHeatMapCntAggrWGS84 =
          ImageIO.read(new File(REFERENCE_WMS_HEATMAP_CNT_AGGR_WGS84));
      TestUtils.testTileAgainstReference(
          heatMapRenderingCntAggrWGS84,
          refHeatMapCntAggrWGS84,
          0.0,
          0.07);

    }

    // Test the count aggregation heatmap rendering WGS84 (CNT_AGGR zoomed-in)
    System.out.println("TEST - STARTING heatmap render CNT_AGGR ZOOM WGS84");
    if (runCntAggrZoomedWGS84) {
      final BufferedImage heatMapRenderingCntAggrWGS84Zoomed =
          getWMSSingleTile(
              minXzoom,
              maxXzoom,
              minYzoom,
              maxYzoom,
              SimpleIngest.FEATURE_NAME,
              ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_CNT_AGGR,
              920,
              360,
              null,
              false,
              false);


      // ImageIO.write(
      // heatMapRenderingCntAggrWGS84Zoomed,
      // "gif",
      // new
      // File("/home/me/Repos/GEOWAVE/geowave/test/src/test/resources/wms/wms-heatmap-cnt-aggr-wgs84-zoom.gif"));
      // "/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/src/test/resources/wms/wms-heatmap-cnt-aggr-wgs84-zoom-oraclejdk.gif"));

      final BufferedImage refHeatMapCntAggrWGS84Zoom =
          ImageIO.read(new File(REFERENCE_WMS_HEATMAP_CNT_AGGR_ZOOM_WGS84));
      TestUtils.testTileAgainstReference(
          heatMapRenderingCntAggrWGS84Zoomed,
          refHeatMapCntAggrWGS84Zoom,
          0.0,
          0.07);

    }

    // Test the sum aggregation heatmap rendering WGS84 (SUM_AGGR zoomed-in)
    System.out.println("TEST - STARTING SUM AGGR WGS84 ZOOM");
    if (runSumAggrZoomedWGS84) {
      final BufferedImage heatMapRenderingSumAggrWGS84Zoomed =
          getWMSSingleTile(
              minXzoom,
              maxXzoom,
              minYzoom,
              maxYzoom,
              SimpleIngest.FEATURE_NAME,
              ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_SUM_AGGR,
              920,
              360,
              null,
              false,
              false);

      // ImageIO.write(
      // heatMapRenderingSumAggrWGS84Zoomed,
      // "gif",
      // new
      // File("/home/me/Repos/GEOWAVE/geowave/test/src/test/resources/wms/W-wms-heatmap-sum-aggr-wgs84-zoom.gif"));
      // "/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/src/test/resources/wms/W-wms-heatmap-sum-aggr-wgs84-zoom-oraclejdk.gif"));

      final BufferedImage refHeatMapSumAggrWGS84Zoom =
          ImageIO.read(new File(REFERENCE_WMS_HEATMAP_SUM_AGGR_ZOOM_WGS84));
      TestUtils.testTileAgainstReference(
          heatMapRenderingSumAggrWGS84Zoomed,
          refHeatMapSumAggrWGS84Zoom,
          0.0,
          0.07);

    }
    // ds.removeIndex(spatialIdx.getName());
    // ds.deleteAll();
    // ServicesTestEnvironment.getInstance().restartServices();
    // ----------------------------------------------------------------------
  }


  /**
   * Creates a buffered image using a specified process.
   * 
   * @param minX {double} Minimum longitude of the extent envelope.
   * @param maxX {double} Maximum longitude of the extent envelope.
   * @param minY {double} Minimum latitude of the extent envelope.
   * @param maxY {double} Maximum latitude of the extent envelope.
   * @param layer {String} The input grid.
   * @param style {String} The SLD to use.
   * @param width {Integer} Width (in pixels) of the extent.
   * @param height {Integer} Height (in pixels) of the extent.
   * @param outputFormat {String} Output format.
   * @param temporalFilter {Boolean} If the data uses a temporal component.
   * @param projected {Boolean} Indicates if the data is projected or just GCS (WGS84).
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
      final boolean projected) throws IOException, URISyntaxException {

    // String crsToUse = projected ? "EPSG:3857" : "EPSG:4326";
    String crsToUse = projected ? TestUtils.CUSTOM_CRSCODE : TestUtils.CUSTOM_CRSCODE_WGS84;


    System.out.println("TEST - PROJECTED? " + projected);
    System.out.println("TEST - CRS TO USE: " + crsToUse);
    System.out.println("TEST - minX: " + minX);
    System.out.println("TEST - maxX: " + maxX);
    System.out.println("TEST - minY: " + minY);
    System.out.println("TEST - maxY: " + maxY);
    System.out.println("TEST - layer: " + layer);
    System.out.println("TEST - style: " + style);
    System.out.println("TEST - width: " + width);
    System.out.println("TEST - height: " + height);
    System.out.println("TEST - outputFormat: " + outputFormat);
    System.out.println("TEST - temporalFilter: " + temporalFilter);


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
                    style == null ? "" : style).setParameter("crs", crsToUse).setParameter(
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
    System.out.println("TEST - CLEANING UP!");
    geoServerServiceClient.removeFeatureLayer(SimpleIngest.FEATURE_NAME);
    geoServerServiceClient.removeDataStore(dataStorePluginOptions.getGeoWaveNamespace(), WORKSPACE);
    geoServerServiceClient.removeDataStore(
        dataStorePluginOptions.getGeoWaveNamespace(),
        WORKSPACE2);
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
    geoServerServiceClient.removeWorkspace(WORKSPACE2);
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStorePluginOptions;
  }
}
