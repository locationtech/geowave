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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.vector.query.aggregation.VectorCountAggregation;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalOptions;
import org.locationtech.geowave.core.geotime.index.TemporalDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.TemporalOptions;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.api.VectorAggregationQueryBuilder;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.BBox;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Crosses;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Disjoint;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Intersects;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Overlaps;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialContains;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialEqualTo;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialFieldValue;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialLiteral;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialNotEqualTo;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Touches;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Within;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.Before;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.BeforeOrDuring;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.During;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.DuringOrAfter;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalBetween;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalEqualTo;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalFieldValue;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalLiteral;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TimeOverlaps;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.base.BaseQueryOptions;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.AttributeDimensionalityTypeProvider;
import org.locationtech.geowave.core.store.index.AttributeIndexOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.query.BaseQuery;
import org.locationtech.geowave.core.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.store.query.constraints.CustomQueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.ExplicitFilteredQuery;
import org.locationtech.geowave.core.store.query.constraints.FilteredEverythingQuery;
import org.locationtech.geowave.core.store.query.constraints.OptimalExpressionQuery;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.filter.ExpressionQueryFilter;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.locationtech.geowave.core.store.query.filter.expression.And;
import org.locationtech.geowave.core.store.query.filter.expression.Between;
import org.locationtech.geowave.core.store.query.filter.expression.ComparisonOperator;
import org.locationtech.geowave.core.store.query.filter.expression.Filter;
import org.locationtech.geowave.core.store.query.filter.expression.Or;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.text.Contains;
import org.locationtech.geowave.core.store.query.filter.expression.text.EndsWith;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextFieldValue;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveBasicVectorIT;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;
import jersey.repackaged.com.google.common.collect.Iterators;
import jersey.repackaged.com.google.common.collect.Sets;

@RunWith(GeoWaveITRunner.class)
public class ExpressionQueryIT extends AbstractGeoWaveBasicVectorIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExpressionQueryIT.class);
  private static final String TYPE_NAME = "testType";
  private static final String DEFAULT_GEOMETRY_FIELD = "geom";
  private static final String ALTERNATE_GEOMETRY_FIELD = "alt";
  private static final String POLYGON_FIELD = "poly";
  private static final String TIMESTAMP_FIELD = "Timestamp";
  private static final String LATITUDE_FIELD = "Latitude";
  private static final String LONGITUDE_FIELD = "Longitude";
  private static final String INTEGER_FIELD = "Integer";
  private static final String ID_FIELD = "ID";
  private static final String COMMENT_FIELD = "Comment";
  private static final SpatialFieldValue GEOM = SpatialFieldValue.of(DEFAULT_GEOMETRY_FIELD);
  private static final SpatialFieldValue ALT = SpatialFieldValue.of(ALTERNATE_GEOMETRY_FIELD);
  private static final SpatialFieldValue POLY = SpatialFieldValue.of(POLYGON_FIELD);
  private static final TemporalFieldValue TIMESTAMP = TemporalFieldValue.of(TIMESTAMP_FIELD);
  private static final NumericFieldValue LATITUDE = NumericFieldValue.of(LATITUDE_FIELD);
  private static final NumericFieldValue LONGITUDE = NumericFieldValue.of(LONGITUDE_FIELD);
  private static final NumericFieldValue INTEGER = NumericFieldValue.of(INTEGER_FIELD);
  private static final TextFieldValue ID = TextFieldValue.of(ID_FIELD);
  private static final TextFieldValue COMMENT = TextFieldValue.of(COMMENT_FIELD);
  private static final int TOTAL_FEATURES = 128; // Must be power of 2 for tests to pass
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
    LOGGER.warn("-----------------------------");
    LOGGER.warn("*                           *");
    LOGGER.warn("* RUNNING ExpressionQueryIT *");
    LOGGER.warn("*                           *");
    LOGGER.warn("----------------------------");
  }

  @AfterClass
  public static void reportTestFinish() {
    LOGGER.warn("------------------------------");
    LOGGER.warn("*                            *");
    LOGGER.warn("* FINISHED ExpressionQueryIT *");
    LOGGER.warn(
        "*       " + ((System.currentTimeMillis() - startMillis) / 1000) + "s elapsed.         *");
    LOGGER.warn("*                            *");
    LOGGER.warn("------------------------------");
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
    builder.add(ab.binding(Double.class).nillable(false).buildDescriptor(LATITUDE_FIELD));
    builder.add(ab.binding(Double.class).nillable(false).buildDescriptor(LONGITUDE_FIELD));
    builder.add(ab.binding(Integer.class).nillable(true).buildDescriptor(INTEGER_FIELD));
    builder.add(ab.binding(String.class).nillable(true).buildDescriptor(ID_FIELD));
    builder.add(ab.binding(String.class).nillable(true).buildDescriptor(COMMENT_FIELD));
    builder.add(ab.binding(Point.class).nillable(true).buildDescriptor(ALTERNATE_GEOMETRY_FIELD));
    builder.add(ab.binding(Polygon.class).nillable(true).buildDescriptor(POLYGON_FIELD));
    builder.setDefaultGeometry(DEFAULT_GEOMETRY_FIELD);

    final SimpleFeatureType featureType = builder.buildFeatureType();
    featureBuilder = new SimpleFeatureBuilder(featureType);

    final SimpleFeatureType sft = featureType;
    final GeotoolsFeatureDataAdapter<SimpleFeature> fda = SimpleIngest.createDataAdapter(sft);
    return fda;
  }

  private final String[] comment = new String[] {"AlphA", "Bravo", "Charlie", null};

  // Each default geometry lies along the line from -64, -64 to 63, 63, while the alternate
  // geometry lies along the line of -64, 64 to 63, -63. This ensures that the alternate geometry
  // lies in different quadrants of the coordinate system.
  private void ingestData(final DataStore dataStore) {
    try (Writer<Object> writer = dataStore.createWriter(TYPE_NAME)) {
      for (int i = 0; i < TOTAL_FEATURES; i++) {
        final double coordinate = i - (TOTAL_FEATURES / 2);
        featureBuilder.set(
            DEFAULT_GEOMETRY_FIELD,
            GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(coordinate, coordinate)));
        featureBuilder.set(TIMESTAMP_FIELD, new Date(i * ONE_DAY_MILLIS));
        featureBuilder.set(LATITUDE_FIELD, coordinate);
        featureBuilder.set(LONGITUDE_FIELD, coordinate);
        featureBuilder.set(INTEGER_FIELD, (int) coordinate);
        featureBuilder.set(ID_FIELD, Long.toHexString((long) (coordinate * 1000)));
        featureBuilder.set(COMMENT_FIELD, comment[i % 4]);
        featureBuilder.set(
            ALTERNATE_GEOMETRY_FIELD,
            (i % 2) == 1 ? GeometryUtils.GEOMETRY_FACTORY.createPoint(
                new Coordinate(coordinate, -coordinate)) : null);
        featureBuilder.set(
            POLYGON_FIELD,
            GeometryUtils.GEOMETRY_FACTORY.createPolygon(
                new Coordinate[] {
                    new Coordinate(coordinate - 1, coordinate - 1),
                    new Coordinate(coordinate - 1, coordinate + 1),
                    new Coordinate(coordinate + 1, coordinate + 1),
                    new Coordinate(coordinate + 1, coordinate - 1),
                    new Coordinate(coordinate - 1, coordinate - 1)}));
        writer.write(featureBuilder.buildFeature(Integer.toString(i)));
      }
    }
  }

  @Test
  public void testIndexSelection() {
    final DataStore ds = dataStore.createDataStore();

    final DataTypeAdapter<SimpleFeature> adapter = createDataAdapter();

    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    final Index spatialTemporalIndex =
        SpatialTemporalDimensionalityTypeProvider.createIndexFromOptions(
            new SpatialTemporalOptions());
    final Index temporalIndex =
        TemporalDimensionalityTypeProvider.createIndexFromOptions(new TemporalOptions());
    ds.addType(adapter, spatialIndex, spatialTemporalIndex, temporalIndex);
    final Index altIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, ALTERNATE_GEOMETRY_FIELD));
    final Index integerIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, INTEGER_FIELD));
    final Index commentIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, COMMENT_FIELD));
    final Index idIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, ID_FIELD));
    final Index latitudeIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, LATITUDE_FIELD));
    final Index longitudeIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, LONGITUDE_FIELD));

    ds.addIndex(
        TYPE_NAME,
        altIndex,
        integerIndex,
        commentIndex,
        idIndex,
        latitudeIndex,
        longitudeIndex);

    final PersistentAdapterStore adapterStore = dataStore.createAdapterStore();
    final InternalAdapterStore internalAdapterStore = dataStore.createInternalAdapterStore();
    final AdapterIndexMappingStore aimStore = dataStore.createAdapterIndexMappingStore();
    final IndexStore indexStore = dataStore.createIndexStore();
    final DataStatisticsStore statsStore = dataStore.createDataStatisticsStore();
    final InternalDataAdapter<?> internalAdapter =
        adapterStore.getAdapter(internalAdapterStore.getAdapterId(TYPE_NAME));

    // Ingest data
    ingestData(ds);

    /////////////////////////////////////////////////////
    // Basic BBOX on Alternate Geometry
    /////////////////////////////////////////////////////
    Query<SimpleFeature> query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            ALT.bbox(-64.5, 0.5, 0.5, 64.5)).build();

    QueryConstraints queryConstraints =
        assertBestIndex(
            internalAdapter,
            altIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    ExplicitFilteredQuery constraints = (ExplicitFilteredQuery) queryConstraints;
    List<QueryFilter> filters = constraints.createFilters(altIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    Filter filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof BBox);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Both bbox and comment are indexed, but comment
    // should result in fewer rows queried so that
    // should be the selected index
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            ALT.bbox(-64.5, -32.5, 32.5, 64.5).and(COMMENT.startsWith("b", true))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            commentIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof CustomQueryConstraints);
    CustomQueryConstraints<?> customConstraints = (CustomQueryConstraints<?>) queryConstraints;
    filters = customConstraints.createFilters(commentIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    // Comment predicate was exact so only the bbox filter should need to be performed
    assertTrue(filter instanceof BBox);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      int count = 0;
      while (iterator.hasNext()) {
        final SimpleFeature feature = iterator.next();
        assertEquals("Bravo", feature.getAttribute(COMMENT_FIELD));
        count++;
      }
      // 1/4 of entries match the comment predicate, but only 3/4 of those match the bounding box
      assertEquals(Math.round(TOTAL_FEATURES / 8 * 1.5), count);
    }

    /////////////////////////////////////////////////////
    // Both bbox and comment are indexed, but bbox should
    // result in fewer rows queried so that should be the
    // selected index
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            ALT.bbox(-64.5, 32.5, -32.5, 64.5).and(COMMENT.startsWith("b", true))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            altIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(altIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    // bbox is not exact, so it will still be part of the filter
    assertTrue(filter instanceof And);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 16, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Latitude is the most constraining
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            LATITUDE.isGreaterThan(5).and(
                LATITUDE.isLessThan(10),
                LONGITUDE.isGreaterThanOrEqualTo(7))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            latitudeIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(latitudeIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    // since the latitude fields are exact, only the longitude needs to be filtered later
    assertTrue(filter instanceof ComparisonOperator);
    Set<String> referencedFields = Sets.newHashSet();
    filter.addReferencedFields(referencedFields);
    assertEquals(1, referencedFields.size());
    assertTrue(referencedFields.contains(LONGITUDE_FIELD));

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(3, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Longitude is the most constraining
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            LONGITUDE.isGreaterThanOrEqualTo(5).and(
                LONGITUDE.isLessThan(10).or(LATITUDE.isLessThanOrEqualTo(15)))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            longitudeIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(longitudeIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    // The second half of the expression cannot be predetermined, so both sides of the Or need to be
    // present
    assertTrue(filter instanceof Or);
    referencedFields = Sets.newHashSet();
    filter.addReferencedFields(referencedFields);
    assertEquals(2, referencedFields.size());
    assertTrue(referencedFields.contains(LATITUDE_FIELD));
    assertTrue(referencedFields.contains(LONGITUDE_FIELD));

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(11, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Longitude is an exact range
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            LONGITUDE.isLessThan(-31.5).or(LONGITUDE.isGreaterThan(31.5))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            longitudeIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(longitudeIndex);
    // The constraints are exact, so there shouldn't be any additional filtering
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2 + 1, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Default geom only should select spatial index
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            GEOM.bbox(0.5, 0.5, 10.5, 10.5)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            spatialIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(spatialIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    // BBox isn't exact, so it should still be filtered
    assertTrue(filter instanceof BBox);
    referencedFields = Sets.newHashSet();
    filter.addReferencedFields(referencedFields);
    assertEquals(1, referencedFields.size());
    assertTrue(referencedFields.contains(DEFAULT_GEOMETRY_FIELD));

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(10, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Default geom and time should select spatial-
    // temporal index
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            GEOM.bbox(0.5, 0.5, 30.5, 30.5).and(
                TIMESTAMP.isBefore(new Date((long) (66 * ONE_DAY_MILLIS + 1))))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            spatialTemporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(spatialTemporalIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    // BBox isn't exact, and neither is timestamp in a binned temporal index
    assertTrue(filter instanceof And);
    referencedFields = Sets.newHashSet();
    filter.addReferencedFields(referencedFields);
    assertEquals(2, referencedFields.size());
    assertTrue(referencedFields.contains(DEFAULT_GEOMETRY_FIELD));
    assertTrue(referencedFields.contains(TIMESTAMP_FIELD));

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(2, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Only timestamp should use temporal index
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TIMESTAMP.isBefore(new Date((long) (66 * ONE_DAY_MILLIS + 1)))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(temporalIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    // Timestamp is not exact on temporal index because there could be ranges
    assertTrue(filter instanceof Before);
    referencedFields = Sets.newHashSet();
    filter.addReferencedFields(referencedFields);
    assertEquals(1, referencedFields.size());
    assertTrue(referencedFields.contains(TIMESTAMP_FIELD));

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(67, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Integer is more constraining, half of the ID
    // values end with 0
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            ID.endsWith("0").and(INTEGER.isBetween(10, 20))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            integerIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(integerIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    // Integer is exact, so only the string predicate should remain
    assertTrue(filter instanceof EndsWith);
    referencedFields = Sets.newHashSet();
    filter.addReferencedFields(referencedFields);
    assertEquals(1, referencedFields.size());
    assertTrue(referencedFields.contains(ID_FIELD));

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(6, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // ID is more constraining
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            ID.endsWith("a0").and(INTEGER.isBetween(0, 40))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            idIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);


    assertTrue(queryConstraints instanceof CustomQueryConstraints);
    customConstraints = (CustomQueryConstraints<?>) queryConstraints;
    filters = customConstraints.createFilters(commentIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    // ID constraint is exact, so only the integer predicate should remain
    assertTrue(filter instanceof Between);
    referencedFields = Sets.newHashSet();
    filter.addReferencedFields(referencedFields);
    assertEquals(1, referencedFields.size());
    assertTrue(referencedFields.contains(INTEGER_FIELD));

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(2, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Alternate geometry is 50% null, so it is more
    // constraining
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            GEOM.bbox(-30.5, -30.5, 30.5, 30.5).and(ALT.bbox(-30.5, -30.5, 30.5, 30.5))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            altIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(altIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    // Neither bbox is exact, so they will both be filtered
    assertTrue(filter instanceof And);
    referencedFields = Sets.newHashSet();
    filter.addReferencedFields(referencedFields);
    assertEquals(2, referencedFields.size());
    assertTrue(referencedFields.contains(DEFAULT_GEOMETRY_FIELD));
    assertTrue(referencedFields.contains(ALTERNATE_GEOMETRY_FIELD));

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(30, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Integer is more constraining
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            INTEGER.isLessThan(-60).and(LATITUDE.isLessThan(5))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            integerIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(integerIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    // Neither bbox is exact, so they will both be filtered
    assertTrue(filter instanceof ComparisonOperator);
    referencedFields = Sets.newHashSet();
    filter.addReferencedFields(referencedFields);
    assertEquals(1, referencedFields.size());
    assertTrue(referencedFields.contains(LATITUDE_FIELD));

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(4, Iterators.size(iterator));
    }
  }

  @Test
  public void testTextExpressionQueries() {
    final DataStore ds = dataStore.createDataStore();

    final DataTypeAdapter<SimpleFeature> adapter = createDataAdapter();

    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    ds.addType(adapter, spatialIndex);
    final Index commentIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, COMMENT_FIELD));
    final Index idIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, ID_FIELD));

    ds.addIndex(TYPE_NAME, commentIndex, idIndex);

    final PersistentAdapterStore adapterStore = dataStore.createAdapterStore();
    final InternalAdapterStore internalAdapterStore = dataStore.createInternalAdapterStore();
    final AdapterIndexMappingStore aimStore = dataStore.createAdapterIndexMappingStore();
    final IndexStore indexStore = dataStore.createIndexStore();
    final DataStatisticsStore statsStore = dataStore.createDataStatisticsStore();
    final InternalDataAdapter<?> internalAdapter =
        adapterStore.getAdapter(internalAdapterStore.getAdapterId(TYPE_NAME));

    // Ingest data
    ingestData(ds);

    /////////////////////////////////////////////////////
    // Starts With
    /////////////////////////////////////////////////////
    Query<SimpleFeature> query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(COMMENT.startsWith("Br")).build();

    QueryConstraints queryConstraints =
        assertBestIndex(
            internalAdapter,
            commentIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof CustomQueryConstraints);
    CustomQueryConstraints<?> customConstraints = (CustomQueryConstraints<?>) queryConstraints;
    List<QueryFilter> filters = customConstraints.createFilters(commentIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Starts With (ignore case)
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(COMMENT.startsWith("br", true)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            commentIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof CustomQueryConstraints);
    customConstraints = (CustomQueryConstraints<?>) queryConstraints;
    filters = customConstraints.createFilters(commentIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Ends With
    /////////////////////////////////////////////////////
    query = QueryBuilder.newBuilder(SimpleFeature.class).filter(COMMENT.endsWith("phA")).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            commentIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof CustomQueryConstraints);
    customConstraints = (CustomQueryConstraints<?>) queryConstraints;
    filters = customConstraints.createFilters(commentIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Ends With (ignore case)
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(COMMENT.endsWith("pha", true)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            commentIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof CustomQueryConstraints);
    customConstraints = (CustomQueryConstraints<?>) queryConstraints;
    filters = customConstraints.createFilters(commentIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Contains
    /////////////////////////////////////////////////////
    query = QueryBuilder.newBuilder(SimpleFeature.class).filter(COMMENT.contains("lph")).build();

    // Spatial index will be selected since contains is a full scan
    queryConstraints =
        assertBestIndex(
            internalAdapter,
            spatialIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof FilteredEverythingQuery);
    FilteredEverythingQuery everything = (FilteredEverythingQuery) queryConstraints;
    filters = everything.createFilters(spatialIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    Filter filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof Contains);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Contains (ignore case)
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(COMMENT.contains("al", true)).build();

    // Spatial index will be selected since contains is a full scan
    queryConstraints =
        assertBestIndex(
            internalAdapter,
            spatialIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof FilteredEverythingQuery);
    everything = (FilteredEverythingQuery) queryConstraints;
    filters = everything.createFilters(spatialIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof Contains);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Between
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(COMMENT.isBetween("A", "C")).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            commentIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof CustomQueryConstraints);
    customConstraints = (CustomQueryConstraints<?>) queryConstraints;
    filters = customConstraints.createFilters(commentIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof Between);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Between (ignore case)
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            COMMENT.isBetween("alpha", "bravo", true)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            commentIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof CustomQueryConstraints);
    customConstraints = (CustomQueryConstraints<?>) queryConstraints;
    filters = customConstraints.createFilters(commentIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof Between);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Greater Than
    /////////////////////////////////////////////////////
    query = QueryBuilder.newBuilder(SimpleFeature.class).filter(COMMENT.isGreaterThan("B")).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            commentIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof CustomQueryConstraints);
    customConstraints = (CustomQueryConstraints<?>) queryConstraints;
    filters = customConstraints.createFilters(commentIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof ComparisonOperator);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Greater Than (ignore case)
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            COMMENT.isGreaterThan("c", true)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            commentIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof CustomQueryConstraints);
    customConstraints = (CustomQueryConstraints<?>) queryConstraints;
    filters = customConstraints.createFilters(commentIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof ComparisonOperator);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Less Than
    /////////////////////////////////////////////////////
    query = QueryBuilder.newBuilder(SimpleFeature.class).filter(COMMENT.isLessThan("B")).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            commentIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof CustomQueryConstraints);
    customConstraints = (CustomQueryConstraints<?>) queryConstraints;
    filters = customConstraints.createFilters(commentIndex);
    // Less than can be an exact query and doesn't need filtering
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Less Than (ignore case)
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(COMMENT.isLessThan("c", true)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            commentIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof CustomQueryConstraints);
    customConstraints = (CustomQueryConstraints<?>) queryConstraints;
    filters = customConstraints.createFilters(commentIndex);
    // Less than can be an exact query and doesn't need filtering
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Greater Than Or Equal To
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            COMMENT.isGreaterThanOrEqualTo("B")).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            commentIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof CustomQueryConstraints);
    customConstraints = (CustomQueryConstraints<?>) queryConstraints;
    filters = customConstraints.createFilters(commentIndex);
    // Greater than or equal to can be an exact query and doesn't need filtering
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Greater Than Or Equal To (ignore case)
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            COMMENT.isGreaterThanOrEqualTo("c", true)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            commentIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof CustomQueryConstraints);
    customConstraints = (CustomQueryConstraints<?>) queryConstraints;
    filters = customConstraints.createFilters(commentIndex);
    // Greater than or equal to can be an exact query and doesn't need filtering
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Less Than Or Equal To
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            COMMENT.isLessThanOrEqualTo("B")).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            commentIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof CustomQueryConstraints);
    customConstraints = (CustomQueryConstraints<?>) queryConstraints;
    filters = customConstraints.createFilters(commentIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof ComparisonOperator);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 4, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Less Than Or Equal To (ignore case)
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            COMMENT.isLessThanOrEqualTo("bravo", true)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            commentIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof CustomQueryConstraints);
    customConstraints = (CustomQueryConstraints<?>) queryConstraints;
    filters = customConstraints.createFilters(commentIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof ComparisonOperator);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(iterator));
    }
  }

  @Test
  public void testNumericExpressionQueries() {
    final DataStore ds = dataStore.createDataStore();

    final DataTypeAdapter<SimpleFeature> adapter = createDataAdapter();

    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    ds.addType(adapter, spatialIndex);
    final Index integerIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, INTEGER_FIELD));
    final Index latitudeIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, LATITUDE_FIELD));
    final Index longitudeIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, LONGITUDE_FIELD));

    ds.addIndex(TYPE_NAME, integerIndex, latitudeIndex, longitudeIndex);

    final PersistentAdapterStore adapterStore = dataStore.createAdapterStore();
    final InternalAdapterStore internalAdapterStore = dataStore.createInternalAdapterStore();
    final AdapterIndexMappingStore aimStore = dataStore.createAdapterIndexMappingStore();
    final IndexStore indexStore = dataStore.createIndexStore();
    final DataStatisticsStore statsStore = dataStore.createDataStatisticsStore();
    final InternalDataAdapter<?> internalAdapter =
        adapterStore.getAdapter(internalAdapterStore.getAdapterId(TYPE_NAME));

    // Ingest data
    ingestData(ds);

    /////////////////////////////////////////////////////
    // Greater Than
    /////////////////////////////////////////////////////
    Query<SimpleFeature> query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(INTEGER.isGreaterThan(0)).build();

    QueryConstraints queryConstraints =
        assertBestIndex(
            internalAdapter,
            integerIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    ExplicitFilteredQuery constraints = (ExplicitFilteredQuery) queryConstraints;
    List<QueryFilter> filters = constraints.createFilters(integerIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2 - 1, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Less Than
    /////////////////////////////////////////////////////
    query = QueryBuilder.newBuilder(SimpleFeature.class).filter(LATITUDE.isLessThan(0)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            latitudeIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(latitudeIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Greater Than Or Equal To
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            INTEGER.isGreaterThanOrEqualTo(0)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            integerIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(integerIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Less Than
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            LONGITUDE.isLessThanOrEqualTo(0)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            longitudeIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(longitudeIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2 + 1, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Equal To
    /////////////////////////////////////////////////////
    query = QueryBuilder.newBuilder(SimpleFeature.class).filter(INTEGER.isEqualTo(12)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            integerIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(integerIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(1, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Not Equal To
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            INTEGER.isNotEqualTo(12).and(INTEGER.isNotEqualTo(8))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            integerIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(integerIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES - 2, Iterators.size(iterator));
    }
  }

  @Test
  public void testTemporalExpressionQueriesTemporalIndex() {
    final DataStore ds = dataStore.createDataStore();

    final DataTypeAdapter<SimpleFeature> adapter = createDataAdapter();

    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    final Index temporalIndex =
        TemporalDimensionalityTypeProvider.createIndexFromOptions(new TemporalOptions());
    ds.addType(adapter, spatialIndex, temporalIndex);

    final PersistentAdapterStore adapterStore = dataStore.createAdapterStore();
    final InternalAdapterStore internalAdapterStore = dataStore.createInternalAdapterStore();
    final AdapterIndexMappingStore aimStore = dataStore.createAdapterIndexMappingStore();
    final IndexStore indexStore = dataStore.createIndexStore();
    final DataStatisticsStore statsStore = dataStore.createDataStatisticsStore();
    final InternalDataAdapter<?> internalAdapter =
        adapterStore.getAdapter(internalAdapterStore.getAdapterId(TYPE_NAME));

    // Ingest data
    ingestData(ds);

    /////////////////////////////////////////////////////
    // After
    /////////////////////////////////////////////////////
    Query<SimpleFeature> query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TIMESTAMP.isAfter(new Date(ONE_DAY_MILLIS * (TOTAL_FEATURES / 2)))).build();

    QueryConstraints queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    ExplicitFilteredQuery constraints = (ExplicitFilteredQuery) queryConstraints;
    List<QueryFilter> filters = constraints.createFilters(temporalIndex);
    assertEquals(1, filters.size());
    Filter filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(
        filter instanceof org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.After);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2 - 1, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Before
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TIMESTAMP.isBefore(ONE_DAY_MILLIS * (TOTAL_FEATURES / 2))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(temporalIndex);
    assertEquals(1, filters.size());
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof Before);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // During or After
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TIMESTAMP.isDuringOrAfter(ONE_DAY_MILLIS * (TOTAL_FEATURES / 2))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(temporalIndex);
    assertEquals(1, filters.size());
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof DuringOrAfter);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Before or During
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TIMESTAMP.isBeforeOrDuring(ONE_DAY_MILLIS * (TOTAL_FEATURES / 2))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(temporalIndex);
    assertEquals(1, filters.size());
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof BeforeOrDuring);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2 + 1, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // During
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TIMESTAMP.isDuring(
                Interval.of(
                    Instant.ofEpochMilli(ONE_DAY_MILLIS * 5),
                    Instant.ofEpochMilli(ONE_DAY_MILLIS * 10)))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(temporalIndex);
    assertEquals(1, filters.size());
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof During);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(5, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Between
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TIMESTAMP.isBetween(
                Instant.ofEpochMilli(ONE_DAY_MILLIS * 5),
                Instant.ofEpochMilli(ONE_DAY_MILLIS * 10))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(temporalIndex);
    assertEquals(1, filters.size());
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof TemporalBetween);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(6, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Contains (inverse of During)
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TemporalLiteral.of(
                Interval.of(
                    Instant.ofEpochMilli(ONE_DAY_MILLIS * 5),
                    Instant.ofEpochMilli(ONE_DAY_MILLIS * 10))).contains(TIMESTAMP)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(temporalIndex);
    assertEquals(1, filters.size());
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof During);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(5, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Overlaps
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TIMESTAMP.overlaps(
                Interval.of(
                    Instant.ofEpochMilli(ONE_DAY_MILLIS * 5),
                    Instant.ofEpochMilli(ONE_DAY_MILLIS * 10)))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(temporalIndex);
    assertEquals(1, filters.size());
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof TimeOverlaps);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(5, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Equal To
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TIMESTAMP.isEqualTo(ONE_DAY_MILLIS * 12)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(temporalIndex);
    assertEquals(1, filters.size());
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof TemporalEqualTo);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(1, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Not Equal To
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TIMESTAMP.isNotEqualTo(ONE_DAY_MILLIS * 12).and(
                TIMESTAMP.isNotEqualTo(ONE_DAY_MILLIS * 8))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(temporalIndex);
    assertEquals(1, filters.size());
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof And);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES - 2, Iterators.size(iterator));
    }
  }


  @Test
  public void testTemporalExpressionQueriesAttributeIndex() {
    final DataStore ds = dataStore.createDataStore();

    final DataTypeAdapter<SimpleFeature> adapter = createDataAdapter();

    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    ds.addType(adapter, spatialIndex);
    final Index temporalIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, TIMESTAMP_FIELD));

    ds.addIndex(TYPE_NAME, temporalIndex);

    final PersistentAdapterStore adapterStore = dataStore.createAdapterStore();
    final InternalAdapterStore internalAdapterStore = dataStore.createInternalAdapterStore();
    final AdapterIndexMappingStore aimStore = dataStore.createAdapterIndexMappingStore();
    final IndexStore indexStore = dataStore.createIndexStore();
    final DataStatisticsStore statsStore = dataStore.createDataStatisticsStore();
    final InternalDataAdapter<?> internalAdapter =
        adapterStore.getAdapter(internalAdapterStore.getAdapterId(TYPE_NAME));

    // Ingest data
    ingestData(ds);

    /////////////////////////////////////////////////////
    // After
    /////////////////////////////////////////////////////
    Query<SimpleFeature> query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TIMESTAMP.isAfter(new Date(ONE_DAY_MILLIS * (TOTAL_FEATURES / 2)))).build();

    QueryConstraints queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    ExplicitFilteredQuery constraints = (ExplicitFilteredQuery) queryConstraints;
    List<QueryFilter> filters = constraints.createFilters(temporalIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2 - 1, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Before
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TIMESTAMP.isBefore(ONE_DAY_MILLIS * (TOTAL_FEATURES / 2))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(temporalIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // During or After
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TIMESTAMP.isDuringOrAfter(ONE_DAY_MILLIS * (TOTAL_FEATURES / 2))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(temporalIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Before or During
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TIMESTAMP.isBeforeOrDuring(ONE_DAY_MILLIS * (TOTAL_FEATURES / 2))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(temporalIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2 + 1, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // During
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TIMESTAMP.isDuring(
                Interval.of(
                    Instant.ofEpochMilli(ONE_DAY_MILLIS * 5),
                    Instant.ofEpochMilli(ONE_DAY_MILLIS * 10)))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(temporalIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(5, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Between
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TIMESTAMP.isBetween(
                Instant.ofEpochMilli(ONE_DAY_MILLIS * 5),
                Instant.ofEpochMilli(ONE_DAY_MILLIS * 10))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(temporalIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(6, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Contains (inverse of During)
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TemporalLiteral.of(
                Interval.of(
                    Instant.ofEpochMilli(ONE_DAY_MILLIS * 5),
                    Instant.ofEpochMilli(ONE_DAY_MILLIS * 10))).contains(TIMESTAMP)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(temporalIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(5, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Overlaps
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TIMESTAMP.overlaps(
                Interval.of(
                    Instant.ofEpochMilli(ONE_DAY_MILLIS * 5),
                    Instant.ofEpochMilli(ONE_DAY_MILLIS * 10)))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(temporalIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(5, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Equal To
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TIMESTAMP.isEqualTo(ONE_DAY_MILLIS * 12)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(temporalIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(1, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Not Equal To
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            TIMESTAMP.isNotEqualTo(ONE_DAY_MILLIS * 12).and(
                TIMESTAMP.isNotEqualTo(ONE_DAY_MILLIS * 8))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            temporalIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(temporalIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES - 2, Iterators.size(iterator));
    }
  }

  @Test
  public void testSpatialExpressionQueries() {
    final DataStore ds = dataStore.createDataStore();

    final DataTypeAdapter<SimpleFeature> adapter = createDataAdapter();

    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    ds.addType(adapter, spatialIndex);
    final Index altIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, ALTERNATE_GEOMETRY_FIELD));
    final Index polyIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, POLYGON_FIELD));

    ds.addIndex(TYPE_NAME, altIndex, polyIndex);

    final PersistentAdapterStore adapterStore = dataStore.createAdapterStore();
    final InternalAdapterStore internalAdapterStore = dataStore.createInternalAdapterStore();
    final AdapterIndexMappingStore aimStore = dataStore.createAdapterIndexMappingStore();
    final IndexStore indexStore = dataStore.createIndexStore();
    final DataStatisticsStore statsStore = dataStore.createDataStatisticsStore();
    final InternalDataAdapter<?> internalAdapter =
        adapterStore.getAdapter(internalAdapterStore.getAdapterId(TYPE_NAME));

    final Polygon boxPoly =
        GeometryUtils.GEOMETRY_FACTORY.createPolygon(
            new Coordinate[] {
                new Coordinate(-20.5, -20.5),
                new Coordinate(-20.5, 20.5),
                new Coordinate(20.5, 20.5),
                new Coordinate(20.5, -20.5),
                new Coordinate(-20.5, -20.5)});

    final Polygon boxPoly2 =
        GeometryUtils.GEOMETRY_FACTORY.createPolygon(
            new Coordinate[] {
                new Coordinate(-20, -20),
                new Coordinate(-20, 20),
                new Coordinate(20, 20),
                new Coordinate(20, -20),
                new Coordinate(-20, -20)});

    // Large diagonal line
    final LineString line =
        GeometryUtils.GEOMETRY_FACTORY.createLineString(
            new Coordinate[] {new Coordinate(-20.5, -20.5), new Coordinate(20.5, 20.5)});

    // Ingest data
    ingestData(ds);

    /////////////////////////////////////////////////////
    // Basic BBOX
    /////////////////////////////////////////////////////
    Query<SimpleFeature> query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            GEOM.bbox(0.5, 0.5, 64.5, 64.5)).build();

    QueryConstraints queryConstraints =
        assertBestIndex(
            internalAdapter,
            spatialIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    ExplicitFilteredQuery constraints = (ExplicitFilteredQuery) queryConstraints;
    List<QueryFilter> filters = constraints.createFilters(spatialIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    Filter filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof BBox);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2 - 1, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Loose BBOX
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            GEOM.bboxLoose(0.5, 0.5, 64.5, 64.5)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            spatialIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(spatialIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES / 2 - 1, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Intersects
    /////////////////////////////////////////////////////
    query = QueryBuilder.newBuilder(SimpleFeature.class).filter(ALT.intersects(boxPoly)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            altIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(altIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof Intersects);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(20, Iterators.size(iterator));
    }


    /////////////////////////////////////////////////////
    // Loose Intersects
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(ALT.intersectsLoose(boxPoly)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            altIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(altIndex);
    assertEquals(0, filters.size());

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(20, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Disjoint
    /////////////////////////////////////////////////////
    query = QueryBuilder.newBuilder(SimpleFeature.class).filter(GEOM.disjoint(boxPoly)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            spatialIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof FilteredEverythingQuery);
    FilteredEverythingQuery everything = (FilteredEverythingQuery) queryConstraints;
    filters = everything.createFilters(spatialIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof Disjoint);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES - 41, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Loose Disjoint
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(GEOM.disjointLoose(boxPoly)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            spatialIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof FilteredEverythingQuery);
    everything = (FilteredEverythingQuery) queryConstraints;
    filters = everything.createFilters(spatialIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof Disjoint);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES - 41, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Crosses
    /////////////////////////////////////////////////////
    query = QueryBuilder.newBuilder(SimpleFeature.class).filter(POLY.crosses(line)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            polyIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(polyIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof Crosses);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(43, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Overlaps
    /////////////////////////////////////////////////////
    query = QueryBuilder.newBuilder(SimpleFeature.class).filter(POLY.overlaps(boxPoly)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            polyIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(polyIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof Overlaps);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      // it overlaps 2 polygons in each corner
      assertEquals(4, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Contains
    /////////////////////////////////////////////////////
    query = QueryBuilder.newBuilder(SimpleFeature.class).filter(GEOM.contains(boxPoly)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            spatialIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(spatialIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof SpatialContains);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertFalse(iterator.hasNext());
    }

    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            SpatialLiteral.of(boxPoly).contains(GEOM)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            spatialIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(spatialIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof SpatialContains);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(41, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Touches
    /////////////////////////////////////////////////////
    query = QueryBuilder.newBuilder(SimpleFeature.class).filter(POLY.touches(boxPoly2)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            polyIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(polyIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof Touches);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(2, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // Within
    /////////////////////////////////////////////////////
    query = QueryBuilder.newBuilder(SimpleFeature.class).filter(GEOM.within(boxPoly)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            spatialIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(spatialIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof Within);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(41, Iterators.size(iterator));
    }

    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            SpatialLiteral.of(boxPoly).within(GEOM)).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            spatialIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(spatialIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof Within);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertFalse(iterator.hasNext());
    }

    /////////////////////////////////////////////////////
    // EqualTo
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            GEOM.isEqualTo(
                GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1)))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            spatialIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    constraints = (ExplicitFilteredQuery) queryConstraints;
    filters = constraints.createFilters(spatialIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof SpatialEqualTo);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(1, Iterators.size(iterator));
    }

    /////////////////////////////////////////////////////
    // NotEqualTo
    /////////////////////////////////////////////////////
    query =
        QueryBuilder.newBuilder(SimpleFeature.class).filter(
            GEOM.isNotEqualTo(
                GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1)))).build();

    queryConstraints =
        assertBestIndex(
            internalAdapter,
            spatialIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof FilteredEverythingQuery);
    everything = (FilteredEverythingQuery) queryConstraints;
    filters = everything.createFilters(spatialIndex);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0) instanceof ExpressionQueryFilter);
    filter = ((ExpressionQueryFilter<?>) filters.get(0)).getFilter();
    assertTrue(filter instanceof SpatialNotEqualTo);

    // Query data
    try (CloseableIterator<SimpleFeature> iterator = ds.query(query)) {
      assertTrue(iterator.hasNext());
      assertEquals(TOTAL_FEATURES - 1, Iterators.size(iterator));
    }
  }

  @Test
  public void testAggregations() {
    final DataStore ds = dataStore.createDataStore();

    final DataTypeAdapter<SimpleFeature> adapter = createDataAdapter();

    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    ds.addType(adapter, spatialIndex);

    final Index latitudeIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            ds,
            new AttributeIndexOptions(TYPE_NAME, LATITUDE_FIELD, "latitudeIndex"));

    ds.addIndex(TYPE_NAME, latitudeIndex);

    final PersistentAdapterStore adapterStore = dataStore.createAdapterStore();
    final InternalAdapterStore internalAdapterStore = dataStore.createInternalAdapterStore();
    final AdapterIndexMappingStore aimStore = dataStore.createAdapterIndexMappingStore();
    final IndexStore indexStore = dataStore.createIndexStore();
    final DataStatisticsStore statsStore = dataStore.createDataStatisticsStore();
    final InternalDataAdapter<?> internalAdapter =
        adapterStore.getAdapter(internalAdapterStore.getAdapterId(TYPE_NAME));

    // Ingest data
    ingestData(ds);

    /////////////////////////////////////////////////////
    // No Filter
    /////////////////////////////////////////////////////
    VectorAggregationQueryBuilder<FieldNameParam, Long> queryBuilder =
        VectorAggregationQueryBuilder.newBuilder();
    AggregationQuery<FieldNameParam, Long, SimpleFeature> query =
        queryBuilder.aggregate(
            TYPE_NAME,
            new VectorCountAggregation(new FieldNameParam(ALTERNATE_GEOMETRY_FIELD))).build();

    Long result = ds.aggregate(query);
    assertEquals(TOTAL_FEATURES / 2, result.longValue());

    /////////////////////////////////////////////////////
    // Filter latitude
    /////////////////////////////////////////////////////
    queryBuilder = VectorAggregationQueryBuilder.newBuilder();
    query =
        queryBuilder.aggregate(
            TYPE_NAME,
            new VectorCountAggregation(new FieldNameParam(ALTERNATE_GEOMETRY_FIELD))).filter(
                LATITUDE.isGreaterThan(0)).build();

    final QueryConstraints queryConstraints =
        assertBestIndex(
            internalAdapter,
            latitudeIndex,
            query,
            adapterStore,
            internalAdapterStore,
            aimStore,
            indexStore,
            statsStore);

    assertTrue(queryConstraints instanceof ExplicitFilteredQuery);
    final ExplicitFilteredQuery constraints = (ExplicitFilteredQuery) queryConstraints;
    final List<QueryFilter> filters = constraints.createFilters(latitudeIndex);
    assertEquals(0, filters.size());

    result = ds.aggregate(query);
    assertEquals(TOTAL_FEATURES / 4, result.longValue());
  }

  private QueryConstraints assertBestIndex(
      final InternalDataAdapter<?> adapter,
      final Index bestIndex,
      final BaseQuery<?, ?> query,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final AdapterIndexMappingStore aimStore,
      final IndexStore indexStore,
      final DataStatisticsStore statsStore) {
    assertTrue(query.getQueryConstraints() instanceof OptimalExpressionQuery);
    final OptimalExpressionQuery queryConstraints =
        (OptimalExpressionQuery) query.getQueryConstraints();
    @SuppressWarnings("rawtypes")
    List<Pair<Index, List<InternalDataAdapter<?>>>> optimalIndices =
        queryConstraints.determineBestIndices(
            query instanceof Query
                ? new BaseQueryOptions((Query) query, adapterStore, internalAdapterStore)
                : new BaseQueryOptions(
                    (AggregationQuery) query,
                    adapterStore,
                    internalAdapterStore),
            new InternalDataAdapter<?>[] {adapter},
            aimStore,
            indexStore,
            statsStore);

    assertEquals(1, optimalIndices.size());
    final Pair<Index, List<InternalDataAdapter<?>>> indexAdapterPair = optimalIndices.get(0);
    assertEquals(bestIndex, indexAdapterPair.getKey());
    assertEquals(1, indexAdapterPair.getValue().size());
    assertEquals(adapter, indexAdapterPair.getValue().get(0));
    final QueryConstraints retVal =
        queryConstraints.createQueryConstraints(
            adapter,
            bestIndex,
            aimStore.getMapping(adapter.getAdapterId(), bestIndex.getName()));
    return retVal;
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStore;
  }
}
