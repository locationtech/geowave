/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.basic;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.annotation.Nullable;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialTemporalOptions;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveGeometryPrecisionIT extends AbstractGeoWaveBasicVectorIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGeometryPrecisionIT.class);

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB},
      options = "enableSecondaryIndexing=false")
  protected DataStorePluginOptions dataStorePluginOptions;

  private static long startMillis;

  private static final String FEATURE_TYPE_NAME = "BasicFeature";
  private static final String GEOMETRY_ATTRIBUTE_NAME = "geom";
  private static final String TIME_ATTRIBUTE_NAME = "timestamp";
  private static SimpleFeatureType featureType;
  private static Index spatialIndex;
  private static Index spatialTemporalIndex;
  private static DataStore dataStore;

  @BeforeClass
  public static void reportTestStart() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("--------------------------------------");
    LOGGER.warn("*                                    *");
    LOGGER.warn("* RUNNING GeoWaveGeometryPrecisionIT *");
    LOGGER.warn("*                                    *");
    LOGGER.warn("--------------------------------------");
  }

  @AfterClass
  public static void reportTestFinish() {
    LOGGER.warn("---------------------------------------");
    LOGGER.warn("*                                     *");
    LOGGER.warn("* FINISHED GeoWaveGeometryPrecisionIT *");
    LOGGER.warn(
        "*            "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.             *");
    LOGGER.warn("*                                     *");
    LOGGER.warn("---------------------------------------");
  }

  @BeforeClass
  public static void createFeatureType() {
    final SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
    final AttributeTypeBuilder ab = new AttributeTypeBuilder();

    sftBuilder.setName(FEATURE_TYPE_NAME);

    sftBuilder.add(
        ab.binding(Geometry.class).nillable(false).buildDescriptor(GEOMETRY_ATTRIBUTE_NAME));
    sftBuilder.add(ab.binding(Date.class).nillable(true).buildDescriptor(TIME_ATTRIBUTE_NAME));

    featureType = sftBuilder.buildFeatureType();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void ingestData(final Geometry[] geometries, final @Nullable Integer geometryPrecision) {
    dataStore = dataStorePluginOptions.createDataStore();
    final SpatialOptions spatialOptions = new SpatialOptions();
    spatialOptions.setGeometryPrecision(geometryPrecision);
    spatialIndex = new SpatialDimensionalityTypeProvider().createIndex(spatialOptions);
    final SpatialTemporalOptions spatialTemporalOptions = new SpatialTemporalOptions();
    spatialTemporalOptions.setGeometryPrecision(geometryPrecision);
    spatialTemporalIndex =
        new SpatialTemporalDimensionalityTypeProvider().createIndex(spatialTemporalOptions);
    final GeotoolsFeatureDataAdapter fda = SimpleIngest.createDataAdapter(featureType);
    final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(featureType);

    final List<SimpleFeature> features = new ArrayList<>();

    for (int i = 0; i < geometries.length; i++) {
      builder.set(GEOMETRY_ATTRIBUTE_NAME, geometries[i]);
      builder.set(TIME_ATTRIBUTE_NAME, new Date());
      features.add(builder.buildFeature(String.valueOf(i)));
    }

    dataStore.addType(fda, spatialIndex, spatialTemporalIndex);
    try (Writer writer = dataStore.createWriter(fda.getTypeName())) {
      for (final SimpleFeature feat : features) {
        writer.write(feat);
      }
    }
  }

  private void testPrecision(
      final Geometry[] geometries,
      final Geometry[] expected,
      final @Nullable Integer geometryPrecision) {
    ingestData(geometries, geometryPrecision);
    VectorQueryBuilder builder = VectorQueryBuilder.newBuilder();
    Query<SimpleFeature> query =
        builder.addTypeName(FEATURE_TYPE_NAME).indexName(spatialIndex.getName()).constraints(
            builder.constraintsFactory().noConstraints()).build();

    try (CloseableIterator<SimpleFeature> features = dataStore.query(query)) {
      final List<SimpleFeature> results = Lists.newArrayList(features);
      Assert.assertEquals(3, results.size());
      for (final SimpleFeature feature : results) {
        final int geometryIndex = Integer.parseInt(feature.getID());
        Assert.assertEquals(expected[geometryIndex], feature.getDefaultGeometry());
      }
    }

    builder = VectorQueryBuilder.newBuilder();
    query =
        builder.addTypeName(FEATURE_TYPE_NAME).indexName(
            spatialTemporalIndex.getName()).constraints(
                builder.constraintsFactory().noConstraints()).build();

    try (CloseableIterator<SimpleFeature> features = dataStore.query(query)) {
      final List<SimpleFeature> results = Lists.newArrayList(features);
      Assert.assertEquals(3, results.size());
      for (final SimpleFeature feature : results) {
        final int geometryIndex = Integer.parseInt(feature.getID());
        Assert.assertEquals(expected[geometryIndex], feature.getDefaultGeometry());
      }
    }
  }

  @Test
  public void testFullPrecision() {
    final GeometryFactory factory = GeometryUtils.GEOMETRY_FACTORY;
    final Geometry[] geometries =
        new Geometry[] {
            factory.createPoint(new Coordinate(12.123456789, -10.987654321)),
            factory.createLineString(
                new Coordinate[] {
                    new Coordinate(123456789.987654321, -123456789.987654321),
                    new Coordinate(987654321.123456789, -987654321.123456789)}),
            factory.createPoint(new Coordinate(0, 0))};
    testPrecision(geometries, geometries, null);
  }

  @Test
  public void testMaxPrecision() {
    final GeometryFactory factory = GeometryUtils.GEOMETRY_FACTORY;
    final Geometry[] geometries =
        new Geometry[] {
            factory.createPoint(new Coordinate(12.123456789, -10.987654321)),
            factory.createLineString(
                new Coordinate[] {
                    new Coordinate(123456789.987654321, -123456789.987654321),
                    new Coordinate(987654321.123456789, -987654321.123456789)}),
            factory.createPoint(new Coordinate(0, 0))};
    final Geometry[] expected =
        new Geometry[] {
            factory.createPoint(new Coordinate(12.1234568, -10.9876543)),
            factory.createLineString(
                new Coordinate[] {
                    new Coordinate(123456789.9876543, -123456789.9876543),
                    new Coordinate(987654321.1234568, -987654321.1234568)}),
            factory.createPoint(new Coordinate(0, 0))};
    testPrecision(geometries, expected, GeometryUtils.MAX_GEOMETRY_PRECISION);
  }

  @Test
  public void testPrecision3() {
    final GeometryFactory factory = GeometryUtils.GEOMETRY_FACTORY;
    final Geometry[] geometries =
        new Geometry[] {
            factory.createPoint(new Coordinate(12.123456789, -10.987654321)),
            factory.createLineString(
                new Coordinate[] {
                    new Coordinate(123456789.987654321, -123456789.987654321),
                    new Coordinate(987654321.123456789, -987654321.123456789)}),
            factory.createPoint(new Coordinate(0, 0))};
    final Geometry[] expected =
        new Geometry[] {
            factory.createPoint(new Coordinate(12.123, -10.988)),
            factory.createLineString(
                new Coordinate[] {
                    new Coordinate(123456789.988, -123456789.988),
                    new Coordinate(987654321.123, -987654321.123)}),
            factory.createPoint(new Coordinate(0, 0))};
    testPrecision(geometries, expected, 3);
  }

  @Test
  public void testPrecision0() {
    final GeometryFactory factory = GeometryUtils.GEOMETRY_FACTORY;
    final Geometry[] geometries =
        new Geometry[] {
            factory.createPoint(new Coordinate(12.123456789, -10.987654321)),
            factory.createLineString(
                new Coordinate[] {
                    new Coordinate(123456789.987654321, -123456789.987654321),
                    new Coordinate(987654321.123456789, -987654321.123456789)}),
            factory.createPoint(new Coordinate(0, 0))};
    final Geometry[] expected =
        new Geometry[] {
            factory.createPoint(new Coordinate(12, -11)),
            factory.createLineString(
                new Coordinate[] {
                    new Coordinate(123456790, -123456790),
                    new Coordinate(987654321, -987654321)}),
            factory.createPoint(new Coordinate(0, 0))};
    testPrecision(geometries, expected, 0);
  }

  @Test
  public void testNegativePrecision() {
    final GeometryFactory factory = GeometryUtils.GEOMETRY_FACTORY;
    final Geometry[] geometries =
        new Geometry[] {
            factory.createPoint(new Coordinate(12.123456789, -10.987654321)),
            factory.createLineString(
                new Coordinate[] {
                    new Coordinate(123456789.987654321, -123456789.987654321),
                    new Coordinate(987654321.123456789, -987654321.123456789)}),
            factory.createPoint(new Coordinate(0, 0))};
    final Geometry[] expected =
        new Geometry[] {
            factory.createPoint(new Coordinate(0, 0)),
            factory.createLineString(
                new Coordinate[] {
                    new Coordinate(123457000, -123457000),
                    new Coordinate(987654000, -987654000)}),
            factory.createPoint(new Coordinate(0, 0))};
    testPrecision(geometries, expected, -3);
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStorePluginOptions;
  }
}
