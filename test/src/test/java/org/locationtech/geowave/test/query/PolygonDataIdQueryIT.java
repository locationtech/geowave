/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.query;

import java.io.IOException;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.query.constraints.DataIdQuery;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveIT;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
public class PolygonDataIdQueryIT extends AbstractGeoWaveIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolygonDataIdQueryIT.class);
  private static SimpleFeatureType simpleFeatureType;
  private static FeatureDataAdapter dataAdapter;
  private static final String GEOMETRY_ATTRIBUTE = "geometry";
  private static final String DATA_ID = "dataId";
  private static Index index =
      new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());

  @GeoWaveTestStore({
      GeoWaveStoreType.ACCUMULO,
      GeoWaveStoreType.CASSANDRA,
      GeoWaveStoreType.HBASE,
      GeoWaveStoreType.DYNAMODB,
      GeoWaveStoreType.KUDU,
      GeoWaveStoreType.REDIS,
      GeoWaveStoreType.ROCKSDB})
  protected DataStorePluginOptions dataStore;

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStore;
  }

  private static long startMillis;

  @Test
  public void testPolygonDataIdQueryResults() {
    final CloseableIterator<SimpleFeature> matches =
        (CloseableIterator) dataStore.createDataStore().query(
            QueryBuilder.newBuilder().addTypeName(dataAdapter.getTypeName()).indexName(
                TestUtils.DEFAULT_SPATIAL_INDEX.getName()).constraints(
                    new DataIdQuery(StringUtils.stringToBinary(DATA_ID))).build());
    int numResults = 0;
    while (matches.hasNext()) {
      matches.next();
      numResults++;
    }
    Assert.assertTrue("Expected 1 result, but returned " + numResults, numResults == 1);
  }

  @BeforeClass
  public static void setupData() throws IOException {
    simpleFeatureType = getSimpleFeatureType();
    dataAdapter = new FeatureDataAdapter(simpleFeatureType);
    dataAdapter.init(index);

    startMillis = System.currentTimeMillis();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*         RUNNING PolygonDataIdQueryIT  *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @AfterClass
  public static void reportTest() {
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*      FINISHED PolygonDataIdQueryIT    *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @Before
  public void ingestSampleData() throws IOException {
    final DataStore store = dataStore.createDataStore();
    store.addType(dataAdapter, TestUtils.DEFAULT_SPATIAL_INDEX);
    try (@SuppressWarnings("unchecked")
    Writer writer = store.createWriter(dataAdapter.getTypeName())) {
      writer.write(
          buildSimpleFeature(
              DATA_ID,
              GeometryUtils.GEOMETRY_FACTORY.createPolygon(
                  new Coordinate[] {
                      new Coordinate(1.0249, 1.0319),
                      new Coordinate(1.0261, 1.0319),
                      new Coordinate(1.0261, 1.0323),
                      new Coordinate(1.0249, 1.0319)})));
    }
  }

  @After
  public void deleteSampleData() throws IOException {

    LOGGER.info("Deleting canned data...");
    TestUtils.deleteAll(dataStore);
    LOGGER.info("Delete complete.");
  }

  private static SimpleFeatureType getSimpleFeatureType() {
    SimpleFeatureType type = null;
    try {
      type = DataUtilities.createType("data", GEOMETRY_ATTRIBUTE + ":Geometry");
    } catch (final SchemaException e) {
      LOGGER.error("Unable to create SimpleFeatureType", e);
    }
    return type;
  }

  private static SimpleFeature buildSimpleFeature(final String dataId, final Geometry geo) {
    final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(simpleFeatureType);
    builder.set(GEOMETRY_ATTRIBUTE, geo);
    return builder.buildFeature(dataId);
  }
}
