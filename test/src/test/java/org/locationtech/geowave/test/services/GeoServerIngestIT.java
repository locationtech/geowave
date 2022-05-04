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

  private static Boolean runProjected = true;
  private static Boolean runUnprojected = false;

  // TODO: create a heatmap .gif using non-Oracle JRE.
  // private static final String REFERENCE_WMS_HEATMAP_NO_SB =
  // TestUtils.isOracleJRE() ?
  // "src/test/resources/wms/wms-heatmap-no-spatial-binning.gif"
  // : "src/test/resources/wms/wms-heatmap-no-spatial-binning.gif";

  private static final String REFERENCE_WMS_HEATMAP_CNT_AGGR =
      TestUtils.isOracleJRE() ? "src/test/resources/wms/wms-heatmap-cnt-aggr-oraclejdk.gif"
          : "src/test/resources/wms/wms-heatmap-cnt-aggr.gif";

  private static final String REFERENCE_WMS_HEATMAP_SUM_AGGR =
      TestUtils.isOracleJRE() ? "src/test/resources/wms/wms-heatmap-sum-aggr-oraclejdk.gif"
          : "src/test/resources/wms/wms-heatmap-sum-aggr.gif";

  private static final String REFERENCE_WMS_HEATMAP_CNT_STATS =
      TestUtils.isOracleJRE() ? "src/test/resources/wms/wms-heatmap-cnt-stats-oraclejdk.gif"
          : "src/test/resources/wms/wms-heatmap-cnt-stats.gif";

  private static final String REFERENCE_WMS_HEATMAP_SUM_STATS =
      TestUtils.isOracleJRE() ? "src/test/resources/wms/wms-heatmap-sum-stats-oraclejdk.gif"
          : "src/test/resources/wms/wms-heatmap-sum-stats.gif";

  private static final String REFERENCE_WMS_HEATMAP_CNT_AGGR_ZOOM =
      TestUtils.isOracleJRE() ? "src/test/resources/wms/wms-heatmap-cnt-aggr-zoom-oraclejdk.gif"
          : "src/test/resources/wms/wms-heatmap-cnt-aggr-zoom-oraclejdk.gif";

  private static final String REFERENCE_WMS_HEATMAP_CNT_AGGR_ZOOM_WGS84 =
      TestUtils.isOracleJRE()
          ? "src/test/resources/wms/wms-heatmap-cnt-aggr-wgs84-zoom-oraclejdk.gif"
          : "src/test/resources/wms/wms-heatmap-cnt-aggr-wgs84-zoom-oraclejdk.gif";



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

          // Create a random number for the SIZE field for sum aggregation and statistics
          // testing
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
  public void testExamplesIngestProjected() throws Exception {
    if (runProjected) {
      final DataStore ds = dataStorePluginOptions.createDataStore();
      final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();


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
          "Should Publish '"
              + ServicesTestEnvironment.TEST_STYLE_NAME_DISTRIBUTED_RENDER
              + "' Style",
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
          "Should Publish '"
              + ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_CNT_STATS
              + "' Style",
          201,
          geoServerServiceClient.addStyle(
              ServicesTestEnvironment.TEST_SLD_HEATMAP_FILE_CNT_STATS,
              ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_CNT_STATS));

      // Test response code for heatmap SUM_STATS
      TestUtils.assertStatusCode(
          "Should Publish '"
              + ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_SUM_STATS
              + "' Style",
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

      // ------------------------------HEATMAP RENDERING----------------------
      runHeatMapRenderingProjectedTests(env);
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
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testExamplesIngestNotProjected() throws Exception {
    if (runUnprojected) {
      final DataStore ds = dataStorePluginOptions.createDataStore();
      final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();

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


      // ----------------HEATMAP RESPONSE TESTS------------------------------------

      // Test response code for heatmap CNT_AGGR
      TestUtils.assertStatusCode(
          "Should Publish '" + ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_CNT_AGGR + "' Style",
          201,
          geoServerServiceClient.addStyle(
              ServicesTestEnvironment.TEST_SLD_HEATMAP_FILE_CNT_AGGR,
              ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_CNT_AGGR));

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

      // ------------------------------HEATMAP WGS84 RENDERING----------------------

      // TODO: if this is run, centroid at 0, 0 cannot be projected at full extent.
      Boolean runCntAggr = false;
      Boolean runZoomed = true;

      Boolean writeGif = false;

      // Test the count aggregation heatmap rendering WGS84 (CNT_AGGR)
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
                false);

        if (writeGif) {
          ImageIO.write(
              heatMapRenderingCntAggr,
              "gif",
              new File(
                  "/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/src/test/resources/wms/t_heatmap_cntAggr_WGS84.gif"));
        }
      }

      // Test the count aggregation heatmap rendering WGS84 (CNT_AGGR zoomed-in)
      if (runZoomed) {
        final BufferedImage refHeatMapCntAggrWGS84Zoom =
            ImageIO.read(new File(REFERENCE_WMS_HEATMAP_CNT_AGGR_ZOOM_WGS84));

        final BufferedImage heatMapRenderingCntAggrWGS84Zoomed =
            getWMSSingleTile(
                env.getMinX() / 4,
                env.getMaxX() / 4,
                env.getMinY() / 4,
                env.getMaxY() / 4,
                SimpleIngest.FEATURE_NAME,
                ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_CNT_AGGR,
                920,
                360,
                null,
                false,
                false);

        if (writeGif) {
          ImageIO.write(
              heatMapRenderingCntAggrWGS84Zoomed,
              "gif",
              new File(
                  "/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/src/test/resources/wms/t_heatmap_cntAggr_WGS84_zoomed.gif"));
        }
        TestUtils.testTileAgainstReference(
            heatMapRenderingCntAggrWGS84Zoomed,
            refHeatMapCntAggrWGS84Zoom,
            0.0,
            0.07);

      }
      ds.removeIndex(spatialIdx.getName());
      ServicesTestEnvironment.getInstance().restartServices();
    }
    // ----------------------------------------------------------------------
  }

  private static void runHeatMapRenderingProjectedTests(BoundingBoxValue env) {
    // ------------------------------HEATMAP RENDERING----------------------
    // Keep these Booleans for local testing purposes
    Boolean runNoSpatialBinning = false;
    Boolean runCntAggr = true;
    Boolean runCntAggrZoom = true;
    Boolean runSumAggr = true;
    Boolean runCntStats = true;
    Boolean runSumStats = true;

    Boolean writeGif = false;
    Boolean writeGifZoom = false;

    try {
      // Test the count aggregation heatmap rendering (NO SPATIAL BINNING)
      if (runNoSpatialBinning) {
        // final BufferedImage refHeatMapNoSpatialBinning =
        // ImageIO.read(new File(REFERENCE_WMS_HEATMAP_NO_SB));

        BufferedImage heatMapRenderingNoSpatBin;

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


        // Write output to a gif -- KEEP THIS HERE
        if (writeGif) {
          ImageIO.write(
              heatMapRenderingNoSpatBin,
              "gif",
              new File(
                  "/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/src/test/resources/wms/c-wms-heatmap-no-spat-bin-oraclejdk.gif"));
        }

        // TestUtils.testTileAgainstReference(
        // heatMapRenderingNoSpatBin,
        // refHeatMapNoSpatialBinning,
        // 0,
        // 0.07);
      }

      // Get the count aggregation heatmap gif
      final BufferedImage refHeatMapCntAggr =
          ImageIO.read(new File(REFERENCE_WMS_HEATMAP_CNT_AGGR));

      // Test the count aggregation heatmap rendering (CNT_AGGR)
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

        if (writeGif) {
          ImageIO.write(
              heatMapRenderingCntAggr,
              "gif",
              new File(
                  "/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/src/test/resources/wms/c-wms-heatmap-cnt-aggr-oraclejdk.gif"));
        }

        TestUtils.testTileAgainstReference(heatMapRenderingCntAggr, refHeatMapCntAggr, 0, 0.07);
      }

      if (runCntAggrZoom) {
        System.out.println("TEST - STARTING ZOOMED-IN VERSION");

        // Get the count aggregation zoom heatmap gif
        final BufferedImage refHeatMapCntAggrZoom =
            ImageIO.read(new File(REFERENCE_WMS_HEATMAP_CNT_AGGR_ZOOM));

        // Test zoomed-in version of heatmap count aggregation
        final BufferedImage heatMapRenderingCntAggrZoom =
            getWMSSingleTile(
                env.getMinX() / 100000,
                env.getMaxX() / 100000,
                env.getMinY() / 100000,
                env.getMaxY() / 100000,
                SimpleIngest.FEATURE_NAME,
                ServicesTestEnvironment.TEST_STYLE_NAME_HEATMAP_CNT_STATS,
                920,
                360,
                null,
                false,
                true);

        if (writeGifZoom) {
          ImageIO.write(
              heatMapRenderingCntAggrZoom,
              "gif",
              new File(
                  "/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/src/test/resources/wms/wms-heatmap-cnt-aggr-zoom-oraclejdk.gif"));
        }

        TestUtils.testTileAgainstReference(
            heatMapRenderingCntAggrZoom,
            refHeatMapCntAggrZoom,
            0,
            0.07);
      }

      // Get the sum aggregation heatmap gif
      final BufferedImage refHeatMapSumAggr =
          ImageIO.read(new File(REFERENCE_WMS_HEATMAP_SUM_AGGR));

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

        if (writeGif) {
          ImageIO.write(
              heatMapRenderingSumAggr,
              "gif",
              new File(
                  "/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/src/test/resources/wms/c-wms-heatmap-sum-aggr-oraclejdk.gif"));
        }

        TestUtils.testTileAgainstReference(heatMapRenderingSumAggr, refHeatMapSumAggr, 0, 0.07);
      }

      // Test the count statistics heatmap rendering initial run
      if (runCntStats) {
        final BufferedImage heatMapRenderingCntStats1 =
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

        // Defaults to CNT_AGGR on initial run
        TestUtils.testTileAgainstReference(heatMapRenderingCntStats1, refHeatMapCntAggr, 0, 0.07);

        // Test the count statistics heatmap rendering subsequent run
        final BufferedImage refHeatMapCntStats =
            ImageIO.read(new File(REFERENCE_WMS_HEATMAP_CNT_STATS));

        final BufferedImage heatMapRenderingCntStats2 =
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

        if (writeGif) {
          ImageIO.write(
              heatMapRenderingCntStats2,
              "gif",
              new File(
                  "/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/src/test/resources/wms/c-wms-heatmap-cnt-stats-oraclejdk.gif"));
        }

        TestUtils.testTileAgainstReference(heatMapRenderingCntStats2, refHeatMapCntStats, 0, 0.07);
      }

      // Test the sum statistics heatmap rendering initial run
      if (runSumStats) {
        final BufferedImage heatMapRenderingSumStats1 =
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

        // Defaults to field SUM_AGGR on initial run
        TestUtils.testTileAgainstReference(heatMapRenderingSumStats1, refHeatMapSumAggr, 0, 0.07);

        // Test subsequent run of field sum statistics heatmap rendering (SUM_STATS)
        final BufferedImage refHeatMapSumStats =
            ImageIO.read(new File(REFERENCE_WMS_HEATMAP_SUM_STATS));

        final BufferedImage heatMapRenderingSumStats2 =
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

        if (writeGif) {
          ImageIO.write(
              heatMapRenderingSumStats2,
              "gif",
              new File(
                  "/home/milla/repos/SAFEHOUSE/GEOWAVE/geowave/test/src/test/resources/wms/c-wms-heatmap-sum-stats-oraclejdk.gif"));
        }

        TestUtils.testTileAgainstReference(heatMapRenderingSumStats2, refHeatMapSumStats, 0, 0.07);
      }
      // ----------------------------------------------------------------------
    } catch (IOException e) {
      e.printStackTrace();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
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
      final boolean projected) throws IOException, URISyntaxException {

    // String crsToUse = projected ? "EPSG:3857" : "EPSG:4326";
    String crsToUse = projected ? TestUtils.CUSTOM_CRSCODE : TestUtils.CUSTOM_CRSCODE_WGS84;


    System.out.println("TEST - PROJECTED? " + projected);
    System.out.println("TEST - CRS TO USE: " + crsToUse);
    System.out.println("TEST - minX: " + minX);
    System.out.println("TEST - maxX: " + maxX);
    System.out.println("TEST - minY: " + minY);
    System.out.println("TEST - maxY: " + maxY);

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
