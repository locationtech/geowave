/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
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
import org.locationtech.geowave.core.geotime.store.query.api.VectorStatisticsQueryBuilder;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
import org.locationtech.geowave.service.client.ConfigServiceClient;
import org.locationtech.geowave.service.client.GeoServerServiceClient;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
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
  private static final String WORKSPACE = "testomatic";
  private static final String WMS_VERSION = "1.3";
  private static final String WMS_URL_PREFIX = "/geoserver/wms";
  private static final String REFERENCE_WMS_IMAGE_PATH = "src/test/resources/wms/wms-grid.gif";

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
      // this JVM and GeoServer, for file-based geoserver data sources, using the REST "importer"
      // will be more handy than adding a layer by referencing the local file system
      // GeoWaveStoreType.ROCKSDB
      },
      namespace = testName)
  protected DataStorePluginOptions dataStorePluginOptions;

  private static long startMillis;

  @BeforeClass
  public static void setup() {
    geoServerServiceClient = new GeoServerServiceClient(ServicesTestEnvironment.GEOWAVE_BASE_URL);

    configServiceClient = new ConfigServiceClient(ServicesTestEnvironment.GEOWAVE_BASE_URL);
    startMillis = System.currentTimeMillis();
    TestUtils.printStartOfTest(LOGGER, testName);
  }

  @AfterClass
  public static void reportTest() {
    TestUtils.printEndOfTest(LOGGER, testName, startMillis);
  }

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
    // extremes are close to -180,180,-90,and 90 wiuthout exactly matching
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
          // Note since trajectoryID and comment are marked as
          // nillable we
          // don't need to set them (they default ot null).

          final SimpleFeature sft = pointBuilder.buildFeature(String.valueOf(featureId));
          feats.add(sft);
          featureId++;
        }
      }
    }
    return feats;
  }

  @Test
  public void testExamplesIngest() throws IOException, URISyntaxException {
    final DataStore ds = dataStorePluginOptions.createDataStore();
    final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();
    final Index spatialIdx = TestUtils.createWebMercatorSpatialIndex();
    final Index spatialTemporalIdx = TestUtils.createWebMercatorSpatialTemporalIndex();
    final GeotoolsFeatureDataAdapter fda = SimpleIngest.createDataAdapter(sft);
    final List<SimpleFeature> features =
        getGriddedTemporalFeatures(new SimpleFeatureBuilder(sft), 8675309);
    LOGGER.info(
        String.format("Beginning to ingest a uniform grid of %d features", features.size()));
    int ingestedFeatures = 0;
    final int featuresPer5Percent = features.size() / 20;
    ds.addType(fda, spatialIdx, spatialTemporalIdx);
    try (Writer writer = ds.createWriter(fda.getTypeName())) {
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
    final Envelope env =
        ds.aggregateStatistics(
            VectorStatisticsQueryBuilder.newBuilder().factory().bbox().fieldName(
                sft.getGeometryDescriptor().getLocalName()).build());
    TestUtils.assertStatusCode(
        "Should Create 'testomatic' Workspace",
        201,
        geoServerServiceClient.addWorkspace("testomatic"));
    configServiceClient.addStoreReRoute(
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
            null);

    final BufferedImage ref = ImageIO.read(new File(REFERENCE_WMS_IMAGE_PATH));

    // being a little lenient because of differences in O/S rendering
    TestUtils.testTileAgainstReference(biDirectRender, ref, 0, 0.07);

    final BufferedImage biSubsamplingWithoutError =
        getWMSSingleTile(
            env.getMinX(),
            env.getMaxX(),
            env.getMinY(),
            env.getMaxY(),
            SimpleIngest.FEATURE_NAME,
            ServicesTestEnvironment.TEST_STYLE_NAME_NO_DIFFERENCE,
            920,
            360,
            null);
    Assert.assertNotNull(ref);

    // being a little lenient because of differences in O/S rendering
    TestUtils.testTileAgainstReference(biSubsamplingWithoutError, ref, 0, 0.07);

    final BufferedImage biSubsamplingWithExpectedError =
        getWMSSingleTile(
            env.getMinX(),
            env.getMaxX(),
            env.getMinY(),
            env.getMaxY(),
            SimpleIngest.FEATURE_NAME,
            ServicesTestEnvironment.TEST_STYLE_NAME_MINOR_SUBSAMPLE,
            920,
            360,
            null);

    TestUtils.testTileAgainstReference(biSubsamplingWithExpectedError, ref, 0.05, 0.15);

    final BufferedImage biSubsamplingWithLotsOfError =
        getWMSSingleTile(
            env.getMinX(),
            env.getMaxX(),
            env.getMinY(),
            env.getMaxY(),
            SimpleIngest.FEATURE_NAME,
            ServicesTestEnvironment.TEST_STYLE_NAME_MAJOR_SUBSAMPLE,
            920,
            360,
            null);

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
            null);
    TestUtils.testTileAgainstReference(biDistributedRendering, ref, 0, 0.07);
  }

  private static BufferedImage getWMSSingleTile(
      final double minX,
      final double maxX,
      final double minY,
      final double maxY,
      final String layer,
      final String style,
      final int width,
      final int height,
      final String outputFormat) throws IOException, URISyntaxException {
    final URIBuilder builder = new URIBuilder();
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
                                        String.valueOf(height)).setParameter(
                                            "cql_filter",
                                            "TimeStamp DURING 1997-01-01T00:00:00.000Z/1998-01-01T00:00:00.000Z");

    final HttpGet command = new HttpGet(builder.build());

    final Pair<CloseableHttpClient, HttpClientContext> clientAndContext =
        GeoServerIT.createClientAndContext();
    final CloseableHttpClient httpClient = clientAndContext.getLeft();
    final HttpClientContext context = clientAndContext.getRight();
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
    geoServerServiceClient.removeWorkspace(WORKSPACE);
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStorePluginOptions;
  }
}
