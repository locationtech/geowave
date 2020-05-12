/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.basic;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.vector.query.aggregation.VectorCountAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.geotime.store.query.aggregate.GeohashBinningStrategy;
import org.locationtech.geowave.core.geotime.store.query.aggregate.GeohashSimpleFeatureBinningStrategy;
import org.locationtech.geowave.core.index.persist.PersistableList;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.AggregationQueryBuilder;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IngestOptions;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.query.aggregate.BinningAggregationOptions;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.format.geotools.vector.GeoToolsVectorDataOptions;
import org.locationtech.geowave.format.geotools.vector.GeoToolsVectorDataStoreIngestFormat;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import static org.hamcrest.CoreMatchers.is;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveBinningAggregationIT extends AbstractGeoWaveBasicVectorIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveBinningAggregationIT.class);
  private static long startMillis;
  @GeoWaveTestStore(
      value = {
          GeoWaveTestStore.GeoWaveStoreType.ACCUMULO,
          GeoWaveTestStore.GeoWaveStoreType.BIGTABLE,
          GeoWaveTestStore.GeoWaveStoreType.CASSANDRA,
          GeoWaveTestStore.GeoWaveStoreType.DYNAMODB,
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
    LOGGER.warn("* RUNNING GeoWaveBinningAggregationIT *");
    LOGGER.warn("*                                             *");
    LOGGER.warn("-----------------------------------------------");
  }

  @AfterClass
  public static void reportTestFinish() {
    LOGGER.warn("------------------------------------------------");
    LOGGER.warn("*                                              *");
    LOGGER.warn("* FINISHED GeoWaveBinningAggregationIT *");
    LOGGER.warn(
        "*                {}s elapsed.                  *",
        ((System.currentTimeMillis() - startMillis) / 1000));
    LOGGER.warn("*                                              *");
    LOGGER.warn("------------------------------------------------");
  }

  @Test
  public void testIngestThenBinnedQuery() {
    final IngestOptions.Builder<SimpleFeature> builder = IngestOptions.newBuilder();
    final DataStore dataStore = this.dataStoreOptions.createDataStore();

    dataStore.ingest(
        HAIL_SHAPEFILE_FILE,
        builder.threads(4).format(
            new GeoToolsVectorDataStoreIngestFormat().createLocalFileIngestPlugin(
                new GeoToolsVectorDataOptions())).build(),
        TestUtils.DimensionalityType.SPATIAL_TEMPORAL.getDefaultIndices());

    try {
      this.testBinnedAggregation(
          new File(TEST_POLYGON_TEMPORAL_FILTER_FILE).toURI().toURL(),
          TestUtils.DEFAULT_SPATIAL_TEMPORAL_INDEX,
          dataStore);
    } catch (final Exception e) {
      e.printStackTrace();
      TestUtils.deleteAll(this.dataStoreOptions);
      Assert.fail(
          "Error occurred while testing a polygon and time range query of spatial temporal index: '"
              + e.getLocalizedMessage()
              + '\'');
    }
    TestUtils.deleteAll(this.dataStoreOptions);
  }

  public void testBinnedAggregation(final URL savedFilterResource, Index index, DataStore dataStore)
      throws IOException {
    final QueryConstraints constraints = TestUtils.resourceToQuery(savedFilterResource, null, true);
    final PersistentAdapterStore adapterStore = getDataStorePluginOptions().createAdapterStore();

    try (CloseableIterator<InternalDataAdapter<?>> adapterItr = adapterStore.getAdapters()) {
      while (adapterItr.hasNext()) {
        final InternalDataAdapter<?> internalDataAdapter = adapterItr.next();
        AggregationQueryBuilder<FieldNameParam, Long, SimpleFeature, ?> builder =
            AggregationQueryBuilder.newBuilder();
        // count the geometries in the data, and bin by geohashes.
        builder.indexName(index.getName());
        builder.constraints(constraints);
        builder.aggregate(
            internalDataAdapter.getTypeName(),
            new VectorCountAggregation(new FieldNameParam("the_geom")));

        AggregationQuery<?, Map<String, Long>, SimpleFeature> query =
            builder.buildWithBinningStrategy(new GeohashSimpleFeatureBinningStrategy(6), -1);
        final Map<String, Long> result = dataStore.aggregate(query);
        Assert.assertThat(result.size(), is(36));
        Assert.assertThat(result.values().stream().reduce(0L, Long::sum), is(84L));
        result.keySet().forEach(k -> Assert.assertThat(k.length(), is(6)));
        result.forEach((k, v) -> System.out.printf("%s: %d%n", k, v));
      }
    }
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return this.dataStoreOptions;
  }
}
