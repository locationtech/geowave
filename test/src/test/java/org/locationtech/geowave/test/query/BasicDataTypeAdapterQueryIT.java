/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.Date;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.geotime.adapter.annotation.GeoWaveSpatialField;
import org.locationtech.geowave.core.geotime.adapter.annotation.GeoWaveTemporalField;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialFieldValue;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveDataType;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveField;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveBasicVectorIT;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jersey.repackaged.com.google.common.collect.Iterators;

@RunWith(GeoWaveITRunner.class)
public class BasicDataTypeAdapterQueryIT extends AbstractGeoWaveBasicVectorIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(BasicDataTypeAdapterQueryIT.class);
  private static final String TYPE_NAME = "testType";
  private static final int TOTAL_FEATURES = 128;
  private static final long ONE_DAY_MILLIS = 1000 * 60 * 60 * 24;

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB,
          GeoWaveStoreType.FILESYSTEM})
  protected DataStorePluginOptions dataStore;

  private static long startMillis;

  @BeforeClass
  public static void reportTestStart() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("---------------------------------------");
    LOGGER.warn("*                                     *");
    LOGGER.warn("* RUNNING BasicDataTypeAdapterQueryIT *");
    LOGGER.warn("*                                     *");
    LOGGER.warn("--------------------------------------");
  }

  @AfterClass
  public static void reportTestFinish() {
    LOGGER.warn("----------------------------------------");
    LOGGER.warn("*                                      *");
    LOGGER.warn("* FINISHED BasicDataTypeAdapterQueryIT *");
    LOGGER.warn(
        "*            "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.              *");
    LOGGER.warn("*                                      *");
    LOGGER.warn("----------------------------------------");
  }

  @After
  public void cleanupWorkspace() {
    TestUtils.deleteAll(dataStore);
  }

  @Test
  public void testIngestAndQueryAnnotatedBasicDataTypeAdapter() {
    final DataStore ds = dataStore.createDataStore();
    final DataTypeAdapter<AnnotatedTestType> adapter =
        BasicDataTypeAdapter.newAdapter(TYPE_NAME, AnnotatedTestType.class, "name");
    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    ds.addType(adapter, spatialIndex);
    try (Writer<Object> writer = ds.createWriter(TYPE_NAME)) {
      for (int i = 0; i < TOTAL_FEATURES; i++) {
        final double coordinate = i - (TOTAL_FEATURES / 2);
        writer.write(
            new AnnotatedTestType(
                GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(coordinate, coordinate)),
                new Date(i * ONE_DAY_MILLIS),
                Long.toHexString((long) (coordinate * 1000)),
                coordinate % 2 == 0));
      }
    }

    Query<AnnotatedTestType> query =
        QueryBuilder.newBuilder(AnnotatedTestType.class).filter(
            SpatialFieldValue.of("geometry").bbox(0.5, 0.5, 32.5, 32.5)).build();

    // Query data
    try (CloseableIterator<AnnotatedTestType> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(iterator));
    }
  }

  @Test
  public void testIngestAndQueryPojoBasicDataTypeAdapter() {
    final DataStore ds = dataStore.createDataStore();
    final DataTypeAdapter<PojoTestType> adapter =
        BasicDataTypeAdapter.newAdapter(TYPE_NAME, PojoTestType.class, "name");
    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    ds.addType(adapter, spatialIndex);
    try (Writer<Object> writer = ds.createWriter(TYPE_NAME)) {
      for (int i = 0; i < TOTAL_FEATURES; i++) {
        final double coordinate = i - (TOTAL_FEATURES / 2);
        writer.write(
            new PojoTestType(
                GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(coordinate, coordinate)),
                new Date(i * ONE_DAY_MILLIS),
                Long.toHexString((long) (coordinate * 1000)),
                coordinate % 2 == 0,
                coordinate));
      }
    }

    Query<PojoTestType> query =
        QueryBuilder.newBuilder(PojoTestType.class).filter(
            SpatialFieldValue.of("geometry").bbox(0.5, 0.5, 32.5, 32.5)).build();

    // Query data
    try (CloseableIterator<PojoTestType> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(iterator));
    }
  }

  @GeoWaveDataType
  public static class AnnotatedTestType {

    @GeoWaveSpatialField(spatialIndexHint = true)
    private Geometry geometry;

    @GeoWaveTemporalField(timeIndexHint = true)
    private Date date;

    @GeoWaveField
    private String name;

    @GeoWaveField
    private boolean primitiveField;

    protected AnnotatedTestType() {}

    public AnnotatedTestType(
        final Geometry geometry,
        final Date date,
        final String name,
        final boolean primitiveField) {
      this.geometry = geometry;
      this.date = date;
      this.name = name;
      this.primitiveField = primitiveField;
    }

    public Geometry getGeometry() {
      return geometry;
    }

    public Date getDate() {
      return date;
    }

    public String getName() {
      return name;
    }

    public boolean getPrimitiveField() {
      return primitiveField;
    }
  }

  public static class PojoTestType {

    private Geometry geometry;

    private Date date;

    private String name;

    private boolean primitiveBoolean;

    public double primitiveDouble;

    protected PojoTestType() {}

    public PojoTestType(
        final Geometry geometry,
        final Date date,
        final String name,
        final boolean primitiveBoolean,
        final double primitiveDouble) {
      this.geometry = geometry;
      this.date = date;
      this.name = name;
      this.primitiveBoolean = primitiveBoolean;
      this.primitiveDouble = primitiveDouble;
    }

    public Geometry getGeometry() {
      return geometry;
    }

    public void setGeometry(final Geometry geometry) {
      this.geometry = geometry;
    }

    public Date getDate() {
      return date;
    }

    public void setDate(final Date date) {
      this.date = date;
    }

    public String getName() {
      return name;
    }

    public void setName(final String name) {
      this.name = name;
    }

    public boolean getPrimitiveBoolean() {
      return primitiveBoolean;
    }

    public void setPrimitiveBoolean(final boolean primitiveBoolean) {
      this.primitiveBoolean = primitiveBoolean;
    }
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStore;
  }

}
