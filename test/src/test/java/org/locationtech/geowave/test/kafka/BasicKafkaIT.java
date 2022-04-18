/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.raster.util.ZipUtils;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.ExplicitSpatialQuery;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic.BoundingBoxValue;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.InternalStatisticsHelper;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveIT;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
@Environments({Environment.KAFKA})
public class BasicKafkaIT extends AbstractGeoWaveIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(BasicKafkaIT.class);
  private static final Map<String, Integer> EXPECTED_COUNT_PER_ADAPTER_ID = new HashMap<>();

  static {
    EXPECTED_COUNT_PER_ADAPTER_ID.put("gpxpoint", 11911);
    EXPECTED_COUNT_PER_ADAPTER_ID.put("gpxtrack", 4);
  }

  protected static final String TEST_DATA_ZIP_RESOURCE_PATH =
      TestUtils.TEST_RESOURCE_PACKAGE + "mapreduce-testdata.zip";
  protected static final String OSM_GPX_INPUT_DIR = TestUtils.TEST_CASE_BASE + "osm_gpx_test_case/";

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB,
          GeoWaveStoreType.FILESYSTEM})
  protected DataStorePluginOptions dataStorePluginOptions;

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStorePluginOptions;
  }

  private static long startMillis;

  @BeforeClass
  public static void extractTestFiles() throws URISyntaxException {
    ZipUtils.unZipFile(
        new File(
            BasicKafkaIT.class.getClassLoader().getResource(TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
        TestUtils.TEST_CASE_BASE);

    startMillis = System.currentTimeMillis();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*         RUNNING BasicKafkaIT          *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @AfterClass
  public static void reportTest() {
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*      FINISHED BasicKafkaIT            *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @Test
  public void testBasicIngestGpx() throws Exception {
    KafkaTestUtils.testKafkaStage(OSM_GPX_INPUT_DIR);
    KafkaTestUtils.testKafkaIngest(dataStorePluginOptions, false, OSM_GPX_INPUT_DIR);

    final DataStatisticsStore statsStore = dataStorePluginOptions.createDataStatisticsStore();
    final PersistentAdapterStore adapterStore = dataStorePluginOptions.createAdapterStore();
    int adapterCount = 0;

    final InternalDataAdapter<?>[] adapters = adapterStore.getAdapters();
    for (final InternalDataAdapter<?> internalDataAdapter : adapters) {
      final FeatureDataAdapter adapter = (FeatureDataAdapter) internalDataAdapter.getAdapter();
      final BoundingBoxValue bboxValue =
          InternalStatisticsHelper.getFieldStatistic(
              statsStore,
              BoundingBoxStatistic.STATS_TYPE,
              adapter.getTypeName(),
              adapter.getFeatureType().getGeometryDescriptor().getLocalName());

      final CountValue count =
          InternalStatisticsHelper.getDataTypeStatistic(
              statsStore,
              CountStatistic.STATS_TYPE,
              adapter.getTypeName());

      // then query it
      final GeometryFactory factory = new GeometryFactory();
      final Envelope env =
          new Envelope(
              bboxValue.getMinX(),
              bboxValue.getMaxX(),
              bboxValue.getMinY(),
              bboxValue.getMaxY());
      final Geometry spatialFilter = factory.toGeometry(env);
      final QueryConstraints query = new ExplicitSpatialQuery(spatialFilter);
      final int resultCount = testQuery(adapter, query);
      assertTrue(
          "'" + adapter.getTypeName() + "' adapter must have at least one element in its statistic",
          count.getValue() > 0);
      assertEquals(
          "'"
              + adapter.getTypeName()
              + "' adapter should have the same results from a spatial query of '"
              + env
              + "' as its total count statistic",
          count.getValue().intValue(),
          resultCount);
      assertEquals(
          "'" + adapter.getTypeName() + "' adapter entries ingested does not match expected count",
          EXPECTED_COUNT_PER_ADAPTER_ID.get(adapter.getTypeName()),
          new Integer(resultCount));
      adapterCount++;
    }
    assertTrue("There should be exactly two adapters", (adapterCount == 2));
  }

  private int testQuery(final DataTypeAdapter<?> adapter, final QueryConstraints query)
      throws Exception {
    final org.locationtech.geowave.core.store.api.DataStore geowaveStore =
        dataStorePluginOptions.createDataStore();

    final CloseableIterator<?> accumuloResults =
        geowaveStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                TestUtils.DEFAULT_SPATIAL_INDEX.getName()).build());

    int resultCount = 0;
    while (accumuloResults.hasNext()) {
      accumuloResults.next();

      resultCount++;
    }
    accumuloResults.close();

    return resultCount;
  }
}
