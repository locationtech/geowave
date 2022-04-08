/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.Date;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialFieldValue;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalFieldValue;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.simple.SimpleIntegerIndexStrategy;
import org.locationtech.geowave.core.index.text.TextIndexStrategy;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.api.AttributeIndex;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IndexFieldMapper;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.AttributeDimensionalityTypeProvider;
import org.locationtech.geowave.core.store.index.AttributeIndexOptions;
import org.locationtech.geowave.core.store.index.CustomIndex;
import org.locationtech.geowave.core.store.index.TextAttributeIndexProvider.AdapterFieldTextIndexEntryConverter;
import org.locationtech.geowave.core.store.query.filter.expression.Filter;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextFieldValue;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jersey.repackaged.com.google.common.collect.Iterators;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveAttributeIndexIT extends AbstractGeoWaveBasicVectorIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveAttributeIndexIT.class);
  private static final String TYPE_NAME = "testType";
  private static final String DEFAULT_GEOMETRY_FIELD = "geom";
  private static final String ALTERNATE_GEOMETRY_FIELD = "alt";
  private static final String TIMESTAMP_FIELD = "Timestamp";
  private static final String INTEGER_FIELD = "Integer";
  private static final String COMMENT_FIELD = "Comment";
  private static final int TOTAL_FEATURES = 100;
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

  private SimpleFeatureBuilder featureBuilder;

  private static long startMillis;

  @BeforeClass
  public static void reportTestStart() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("-----------------------------------");
    LOGGER.warn("*                                 *");
    LOGGER.warn("* RUNNING GeoWaveAttributeIndexIT *");
    LOGGER.warn("*                                 *");
    LOGGER.warn("----------------------------------");
  }

  @AfterClass
  public static void reportTestFinish() {
    LOGGER.warn("------------------------------------");
    LOGGER.warn("*                                  *");
    LOGGER.warn("* FINISHED GeoWaveAttributeIndexIT *");
    LOGGER.warn(
        "*          "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.            *");
    LOGGER.warn("*                                  *");
    LOGGER.warn("------------------------------------");
  }

  @After
  public void cleanupWorkspace() {
    TestUtils.deleteAll(dataStore);
  }

  private DataTypeAdapter<SimpleFeature> createDataAdapter() {

    final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
    final AttributeTypeBuilder ab = new AttributeTypeBuilder();

    builder.setName(TYPE_NAME);

    builder.add(ab.binding(Geometry.class).nillable(false).buildDescriptor(DEFAULT_GEOMETRY_FIELD));
    builder.add(ab.binding(Date.class).nillable(true).buildDescriptor(TIMESTAMP_FIELD));
    builder.add(ab.binding(Double.class).nillable(false).buildDescriptor("Latitude"));
    builder.add(ab.binding(Double.class).nillable(false).buildDescriptor("Longitude"));
    builder.add(ab.binding(Integer.class).nillable(true).buildDescriptor(INTEGER_FIELD));
    builder.add(ab.binding(String.class).nillable(true).buildDescriptor("ID"));
    builder.add(ab.binding(String.class).nillable(true).buildDescriptor(COMMENT_FIELD));
    builder.add(ab.binding(Point.class).nillable(true).buildDescriptor(ALTERNATE_GEOMETRY_FIELD));
    builder.setDefaultGeometry(DEFAULT_GEOMETRY_FIELD);

    final SimpleFeatureType featureType = builder.buildFeatureType();
    featureBuilder = new SimpleFeatureBuilder(featureType);

    final SimpleFeatureType sft = featureType;
    final GeotoolsFeatureDataAdapter<SimpleFeature> fda = SimpleIngest.createDataAdapter(sft);
    return fda;
  }

  private final String[] comment = new String[] {"A", "B", "C", null};

  // Each default geometry lies along the line from -50, -50 to 50, 50, while the alternate
  // geometry lies along the line of -50, 50 to 50, -50. This ensures that the alternate geometry
  // lies in different quadrants of the coordinate system.
  private void ingestData(final DataStore dataStore) {
    try (Writer<Object> writer = dataStore.createWriter(TYPE_NAME)) {
      for (int i = 0; i < TOTAL_FEATURES; i++) {
        final double coordinate = i - (TOTAL_FEATURES / 2);
        featureBuilder.set(
            DEFAULT_GEOMETRY_FIELD,
            GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(coordinate, coordinate)));
        featureBuilder.set(TIMESTAMP_FIELD, (i % 2) == 0 ? new Date(i * ONE_DAY_MILLIS) : null);
        featureBuilder.set("Latitude", coordinate);
        featureBuilder.set("Longitude", coordinate);
        featureBuilder.set(INTEGER_FIELD, (i % 4) == 0 ? i : null);
        featureBuilder.set("ID", Double.toHexString(coordinate * 1000));
        featureBuilder.set(COMMENT_FIELD, comment[i % 4]);
        featureBuilder.set(
            ALTERNATE_GEOMETRY_FIELD,
            (i % 2) == 1 ? GeometryUtils.GEOMETRY_FACTORY.createPoint(
                new Coordinate(coordinate, -coordinate)) : null);
        writer.write(featureBuilder.buildFeature(Integer.toString(i)));
      }
    }
  }

  @Test
  public void testGeometryAttributeIndex() {
    final DataStore ds = dataStore.createDataStore();

    final DataTypeAdapter<SimpleFeature> adapter = createDataAdapter();

    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    ds.addType(adapter, spatialIndex);
    Index geometryAttributeIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, ALTERNATE_GEOMETRY_FIELD));

    ds.addIndex(TYPE_NAME, geometryAttributeIndex);

    geometryAttributeIndex = ds.getIndex(geometryAttributeIndex.getName());

    assertTrue(geometryAttributeIndex instanceof AttributeIndex);
    assertEquals(
        ALTERNATE_GEOMETRY_FIELD,
        ((AttributeIndex) geometryAttributeIndex).getAttributeName());

    final InternalAdapterStore adapterStore = dataStore.createInternalAdapterStore();
    final AdapterIndexMappingStore mappingStore = dataStore.createAdapterIndexMappingStore();

    // Get the mapping for the attribute index
    final AdapterToIndexMapping mapping =
        mappingStore.getMapping(
            adapterStore.getAdapterId(adapter.getTypeName()),
            geometryAttributeIndex.getName());

    assertEquals(1, mapping.getIndexFieldMappers().size());
    final IndexFieldMapper<?, ?> fieldMapper = mapping.getIndexFieldMappers().get(0);
    assertEquals(Geometry.class, fieldMapper.adapterFieldType());
    assertEquals(Geometry.class, fieldMapper.indexFieldType());
    assertEquals(1, fieldMapper.getAdapterFields().length);
    assertEquals(ALTERNATE_GEOMETRY_FIELD, fieldMapper.getAdapterFields()[0]);

    // Ingest data
    ingestData(ds);

    // Query data from attribute index
    try (CloseableIterator<SimpleFeature> iterator =
        ds.query(
            QueryBuilder.newBuilder(SimpleFeature.class).indexName(
                geometryAttributeIndex.getName()).build())) {
      assertTrue(iterator.hasNext());
      // Half of the values are null and won't be indexed
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(iterator));
    }

    final Filter bboxFilter =
        SpatialFieldValue.of(ALTERNATE_GEOMETRY_FIELD).bbox(-50.5, 0.5, 0.5, 50.5);
    // Query data from attribute index with a spatial constraint
    try (CloseableIterator<SimpleFeature> iterator =
        ds.query(
            QueryBuilder.newBuilder(SimpleFeature.class).indexName(
                geometryAttributeIndex.getName()).filter(bboxFilter).build())) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(iterator));
    }
  }

  @Test
  public void testTemporalAttributeIndex() {
    final DataStore ds = dataStore.createDataStore();

    final DataTypeAdapter<SimpleFeature> adapter = createDataAdapter();

    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    ds.addType(adapter, spatialIndex);
    Index temporalAttributeIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, TIMESTAMP_FIELD));

    ds.addIndex(TYPE_NAME, temporalAttributeIndex);

    temporalAttributeIndex = ds.getIndex(temporalAttributeIndex.getName());

    assertTrue(temporalAttributeIndex instanceof AttributeIndex);
    assertEquals(TIMESTAMP_FIELD, ((AttributeIndex) temporalAttributeIndex).getAttributeName());

    final InternalAdapterStore adapterStore = dataStore.createInternalAdapterStore();
    final AdapterIndexMappingStore mappingStore = dataStore.createAdapterIndexMappingStore();

    // Get the mapping for the attribute index
    final AdapterToIndexMapping mapping =
        mappingStore.getMapping(
            adapterStore.getAdapterId(adapter.getTypeName()),
            temporalAttributeIndex.getName());

    assertEquals(1, mapping.getIndexFieldMappers().size());
    final IndexFieldMapper<?, ?> fieldMapper = mapping.getIndexFieldMappers().get(0);
    assertEquals(Date.class, fieldMapper.adapterFieldType());
    assertEquals(Long.class, fieldMapper.indexFieldType());
    assertEquals(1, fieldMapper.getAdapterFields().length);
    assertEquals(TIMESTAMP_FIELD, fieldMapper.getAdapterFields()[0]);

    // Ingest data
    ingestData(ds);

    // Query data from attribute index
    try (CloseableIterator<SimpleFeature> iterator =
        ds.query(
            QueryBuilder.newBuilder(SimpleFeature.class).indexName(
                temporalAttributeIndex.getName()).build())) {
      assertTrue(iterator.hasNext());
      // Half of the values are null and won't be indexed
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(iterator));
    }

    final Filter timeFilter =
        TemporalFieldValue.of(TIMESTAMP_FIELD).isBetween(
            new Date((long) (ONE_DAY_MILLIS * 10.5)),
            new Date((long) (ONE_DAY_MILLIS * 24.5)));

    // Query data from attribute index with a numeric range constraint
    try (CloseableIterator<SimpleFeature> iterator =
        ds.query(
            QueryBuilder.newBuilder(SimpleFeature.class).indexName(
                temporalAttributeIndex.getName()).filter(timeFilter).build())) {
      assertTrue(iterator.hasNext());
      assertEquals(7, Iterators.size(iterator));
    }

  }

  @Test
  public void testNumericAttributeIndex() {
    final DataStore ds = dataStore.createDataStore();

    final DataTypeAdapter<SimpleFeature> adapter = createDataAdapter();

    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    ds.addType(adapter, spatialIndex);
    Index integerAttributeIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, INTEGER_FIELD));

    ds.addIndex(TYPE_NAME, integerAttributeIndex);

    integerAttributeIndex = ds.getIndex(integerAttributeIndex.getName());

    assertTrue(integerAttributeIndex instanceof AttributeIndex);
    assertEquals(INTEGER_FIELD, ((AttributeIndex) integerAttributeIndex).getAttributeName());
    assertTrue(integerAttributeIndex.getIndexStrategy() instanceof SimpleIntegerIndexStrategy);

    final InternalAdapterStore adapterStore = dataStore.createInternalAdapterStore();
    final AdapterIndexMappingStore mappingStore = dataStore.createAdapterIndexMappingStore();

    // Get the mapping for the attribute index
    final AdapterToIndexMapping mapping =
        mappingStore.getMapping(
            adapterStore.getAdapterId(adapter.getTypeName()),
            integerAttributeIndex.getName());

    assertEquals(1, mapping.getIndexFieldMappers().size());
    final IndexFieldMapper<?, ?> fieldMapper = mapping.getIndexFieldMappers().get(0);
    assertEquals(Integer.class, fieldMapper.adapterFieldType());
    assertEquals(Integer.class, fieldMapper.indexFieldType());
    assertEquals(1, fieldMapper.getAdapterFields().length);
    assertEquals(INTEGER_FIELD, fieldMapper.getAdapterFields()[0]);

    // Ingest data
    ingestData(ds);

    // Query data from attribute index
    try (CloseableIterator<SimpleFeature> iterator =
        ds.query(
            QueryBuilder.newBuilder(SimpleFeature.class).indexName(
                integerAttributeIndex.getName()).build())) {
      assertTrue(iterator.hasNext());
      // Only one quarter of features should be indexed
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(iterator));
    }

    final Filter rangeFilter = NumericFieldValue.of(INTEGER_FIELD).isBetween(1.0, 40.0);
    // Query data from attribute index with a numeric range constraint
    try (CloseableIterator<SimpleFeature> iterator =
        ds.query(
            QueryBuilder.newBuilder(SimpleFeature.class).indexName(
                integerAttributeIndex.getName()).filter(rangeFilter).build())) {
      assertTrue(iterator.hasNext());
      assertEquals(10, Iterators.size(iterator));
    }

  }

  @Test
  public void testTextAttributeIndex() {
    final DataStore ds = dataStore.createDataStore();

    final DataTypeAdapter<SimpleFeature> adapter = createDataAdapter();

    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    ds.addType(adapter, spatialIndex);
    Index textAttributeIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, COMMENT_FIELD));

    ds.addIndex(TYPE_NAME, textAttributeIndex);

    textAttributeIndex = ds.getIndex(textAttributeIndex.getName());

    assertTrue(textAttributeIndex instanceof AttributeIndex);
    assertEquals(COMMENT_FIELD, ((AttributeIndex) textAttributeIndex).getAttributeName());

    assertTrue(textAttributeIndex instanceof CustomIndex);
    assertTrue(
        ((CustomIndex<?, ?>) textAttributeIndex).getCustomIndexStrategy() instanceof TextIndexStrategy);
    final TextIndexStrategy<?> indexStrategy =
        (TextIndexStrategy<?>) ((CustomIndex<?, ?>) textAttributeIndex).getCustomIndexStrategy();
    assertTrue(indexStrategy.getEntryConverter() instanceof AdapterFieldTextIndexEntryConverter);
    final AdapterFieldTextIndexEntryConverter<?> converter =
        (AdapterFieldTextIndexEntryConverter<?>) indexStrategy.getEntryConverter();
    assertEquals(COMMENT_FIELD, converter.getFieldName());
    assertNotNull(converter.getAdapter());
    assertEquals(adapter.getTypeName(), converter.getAdapter().getTypeName());
    assertEquals(
        adapter.getFieldDescriptor(COMMENT_FIELD),
        converter.getAdapter().getFieldDescriptor(COMMENT_FIELD));


    final InternalAdapterStore adapterStore = dataStore.createInternalAdapterStore();
    final AdapterIndexMappingStore mappingStore = dataStore.createAdapterIndexMappingStore();

    // Get the mapping for the attribute index
    final AdapterToIndexMapping mapping =
        mappingStore.getMapping(
            adapterStore.getAdapterId(adapter.getTypeName()),
            textAttributeIndex.getName());

    // The text index is a custom index, so there won't be any direct field mappings
    assertEquals(0, mapping.getIndexFieldMappers().size());

    // Ingest data
    ingestData(ds);

    // Query data from attribute index
    try (CloseableIterator<SimpleFeature> iterator =
        ds.query(
            QueryBuilder.newBuilder(SimpleFeature.class).indexName(
                textAttributeIndex.getName()).build())) {
      assertTrue(iterator.hasNext());
      // The null values are not indexed, so only 3/4 of the data should be present
      assertEquals((int) (TOTAL_FEATURES * 0.75), Iterators.size(iterator));
    }

    final Filter textFilter = TextFieldValue.of(COMMENT_FIELD).startsWith("c", true);
    // Query data from attribute index with a text constraint
    try (CloseableIterator<SimpleFeature> iterator =
        ds.query(
            QueryBuilder.newBuilder(SimpleFeature.class).indexName(
                textAttributeIndex.getName()).filter(textFilter).build())) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(iterator));
    }

  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStore;
  }
}
