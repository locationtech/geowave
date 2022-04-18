/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.basic;

import static org.hamcrest.CoreMatchers.is;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.vector.query.aggregation.VectorCountAggregation;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.geotime.store.query.aggregate.SpatialSimpleFeatureBinningStrategy;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.AggregationQueryBuilder;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IngestOptions;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.format.geotools.vector.GeoToolsVectorDataOptions;
import org.locationtech.geowave.format.geotools.vector.GeoToolsVectorDataStoreIngestFormat;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveSpatialBinningAggregationIT extends AbstractGeoWaveBasicVectorIT {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeoWaveSpatialBinningAggregationIT.class);
  private static long startMillis;
  @GeoWaveTestStore(
      value = {
          GeoWaveTestStore.GeoWaveStoreType.ACCUMULO,
          GeoWaveTestStore.GeoWaveStoreType.BIGTABLE,
          GeoWaveTestStore.GeoWaveStoreType.CASSANDRA,
          GeoWaveTestStore.GeoWaveStoreType.DYNAMODB,
          GeoWaveTestStore.GeoWaveStoreType.FILESYSTEM,
          GeoWaveTestStore.GeoWaveStoreType.HBASE,
          GeoWaveTestStore.GeoWaveStoreType.KUDU,
          GeoWaveTestStore.GeoWaveStoreType.REDIS,
          GeoWaveTestStore.GeoWaveStoreType.ROCKSDB})
  protected DataStorePluginOptions dataStoreOptions;

  @BeforeClass
  public static void reportTestStart() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("-----------------------------------------------");
    LOGGER.warn("*                                             *");
    LOGGER.warn("* RUNNING GeoWaveSpatialBinningAggregationIT *");
    LOGGER.warn("*                                             *");
    LOGGER.warn("-----------------------------------------------");
  }

  @AfterClass
  public static void reportTestFinish() {
    LOGGER.warn("------------------------------------------------");
    LOGGER.warn("*                                              *");
    LOGGER.warn("* FINISHED GeoWaveSpatialBinningAggregationIT *");
    LOGGER.warn(
        "*                {}s elapsed.                  *",
        ((System.currentTimeMillis() - startMillis) / 1000));
    LOGGER.warn("*                                              *");
    LOGGER.warn("------------------------------------------------");
  }

  @Test
  public void testIngestThenBinnedQuery() {
    final IngestOptions.Builder<SimpleFeature> builder = IngestOptions.newBuilder();
    final DataStore dataStore = dataStoreOptions.createDataStore();

    dataStore.ingest(
        HAIL_SHAPEFILE_FILE,
        builder.threads(4).format(
            new GeoToolsVectorDataStoreIngestFormat().createLocalFileIngestPlugin(
                new GeoToolsVectorDataOptions())).build(),
        TestUtils.DimensionalityType.SPATIAL_TEMPORAL.getDefaultIndices());

    try {
      for (final SpatialBinningType type : SpatialBinningType.values()) {
        for (int precision = 1; precision < 7; precision++) {
          testBinnedAggregation(
              type,
              precision,
              new File(TEST_POLYGON_TEMPORAL_FILTER_FILE).toURI().toURL(),
              TestUtils.DEFAULT_SPATIAL_TEMPORAL_INDEX,
              dataStore);
        }
      }
    } catch (final Exception e) {
      e.printStackTrace();
      TestUtils.deleteAll(dataStoreOptions);
      Assert.fail(
          "Error occurred while testing a polygon and time range query of spatial temporal index: '"
              + e.getLocalizedMessage()
              + '\'');
    }
    TestUtils.deleteAll(dataStoreOptions);
  }

  public void testBinnedAggregation(
      final SpatialBinningType type,
      final int precision,
      final URL savedFilterResource,
      final Index index,
      final DataStore dataStore) throws IOException {
    final QueryConstraints constraints = TestUtils.resourceToQuery(savedFilterResource, null, true);
    final PersistentAdapterStore adapterStore = getDataStorePluginOptions().createAdapterStore();

    final InternalDataAdapter<?>[] adapters = adapterStore.getAdapters();
    for (final InternalDataAdapter<?> internalDataAdapter : adapters) {
      final AggregationQueryBuilder<FieldNameParam, Long, SimpleFeature, ?> builder =
          AggregationQueryBuilder.newBuilder();
      // count the geometries in the data, and bin by geohashes.
      builder.indexName(index.getName());
      builder.constraints(constraints);
      builder.aggregate(
          internalDataAdapter.getTypeName(),
          new VectorCountAggregation(new FieldNameParam("the_geom")));

      final AggregationQuery<?, Map<ByteArray, Long>, SimpleFeature> query =
          builder.buildWithBinningStrategy(
              new SpatialSimpleFeatureBinningStrategy(type, precision, true),
              -1);
      final Map<ByteArray, Long> result = dataStore.aggregate(query);
      Assert.assertThat(result.values().stream().reduce(0L, Long::sum), is(84L));
    }
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStoreOptions;
  }
}
