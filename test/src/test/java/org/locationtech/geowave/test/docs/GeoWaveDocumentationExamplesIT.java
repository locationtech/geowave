/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.docs;

import java.util.Date;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.index.api.SpatialIndexBuilder;
import org.locationtech.geowave.core.geotime.index.api.SpatialTemporalIndexBuilder;
import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import org.locationtech.geowave.core.geotime.store.query.api.VectorAggregationQueryBuilder;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryConstraintsFactory;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic.BoundingBoxValue;
import org.locationtech.geowave.core.geotime.store.statistics.SpatialTemporalStatisticQueryBuilder;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.StatisticQuery;
import org.locationtech.geowave.core.store.api.StatisticQueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.statistics.query.FieldStatisticQueryBuilder;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveIT;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveDocumentationExamplesIT extends AbstractGeoWaveIT {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeoWaveDocumentationExamplesIT.class);

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
    LOGGER.warn("------------------------------------------");
    LOGGER.warn("*                                        *");
    LOGGER.warn("* RUNNING GeoWaveDocumentationExamplesIT *");
    LOGGER.warn("*                                        *");
    LOGGER.warn("------------------------------------------");
  }

  @AfterClass
  public static void reportTestFinish() {
    LOGGER.warn("-------------------------------------------");
    LOGGER.warn("*                                         *");
    LOGGER.warn("* FINISHED GeoWaveDocumentationExamplesIT *");
    LOGGER.warn(
        "*                "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.             *");
    LOGGER.warn("*                                         *");
    LOGGER.warn("-------------------------------------------");
  }

  @Test
  public void testExamples() throws Exception {
    // !!IMPORTANT!! If this test has to be updated, update the associated programmatic API example
    // in the dev guide!
    StoreFactoryOptions options = dataStore.getFactoryOptions();

    DataStore myStore = DataStoreFactory.createDataStore(options);

    // --------------------------------------------------------------------
    // Create Indices Example !! See Note at Top of Test
    // --------------------------------------------------------------------
    // Spatial Index
    SpatialIndexBuilder spatialIndexBuilder = new SpatialIndexBuilder();
    spatialIndexBuilder.setCrs("EPSG:4326");
    Index spatialIndex = spatialIndexBuilder.createIndex();

    // Spatial-temporal Index
    SpatialTemporalIndexBuilder spatialTemporalIndexBuilder = new SpatialTemporalIndexBuilder();
    spatialTemporalIndexBuilder.setCrs("EPSG:3857");
    spatialTemporalIndexBuilder.setPeriodicity(Unit.MONTH);
    Index spatialTemporalIndex = spatialTemporalIndexBuilder.createIndex();
    // --------------------------------------------------------------------

    // --------------------------------------------------------------------
    // Add Indices Example !! See Note at Top of Test
    // --------------------------------------------------------------------
    // Add the spatial and spatial-temporal indices
    myStore.addIndex(spatialIndex);
    myStore.addIndex(spatialTemporalIndex);
    // --------------------------------------------------------------------

    // --------------------------------------------------------------------
    // Ingest Example !! See Note at Top of Test
    // --------------------------------------------------------------------
    // Create a point feature type
    SimpleFeatureTypeBuilder pointTypeBuilder = new SimpleFeatureTypeBuilder();
    AttributeTypeBuilder attributeBuilder = new AttributeTypeBuilder();
    pointTypeBuilder.setName("TestPointType");
    pointTypeBuilder.add(
        attributeBuilder.binding(Point.class).nillable(false).buildDescriptor("the_geom"));
    pointTypeBuilder.add(
        attributeBuilder.binding(Date.class).nillable(false).buildDescriptor("date"));
    SimpleFeatureType pointType = pointTypeBuilder.buildFeatureType();

    // Create a feature builder
    SimpleFeatureBuilder pointFeatureBuilder = new SimpleFeatureBuilder(pointType);

    // Create an adapter for point type
    FeatureDataAdapter pointTypeAdapter = new FeatureDataAdapter(pointType);

    // Add the point type to the data store in the spatial index
    myStore.addType(pointTypeAdapter, spatialIndex);

    // Create a writer to ingest data
    try (Writer<SimpleFeature> writer = myStore.createWriter(pointTypeAdapter.getTypeName())) {
      // Write some features to the data store
      GeometryFactory factory = new GeometryFactory();
      pointFeatureBuilder.set("the_geom", factory.createPoint(new Coordinate(1, 1)));
      pointFeatureBuilder.set("date", new Date());
      writer.write(pointFeatureBuilder.buildFeature("feature1"));

      pointFeatureBuilder.set("the_geom", factory.createPoint(new Coordinate(5, 5)));
      pointFeatureBuilder.set("date", new Date());
      writer.write(pointFeatureBuilder.buildFeature("feature2"));

      pointFeatureBuilder.set("the_geom", factory.createPoint(new Coordinate(-5, -5)));
      pointFeatureBuilder.set("date", new Date());
      writer.write(pointFeatureBuilder.buildFeature("feature3"));
    }
    // --------------------------------------------------------------------

    // --------------------------------------------------------------------
    // Query Data Example !! See Note at Top of Test
    // --------------------------------------------------------------------
    // Create the query builder and constraints factory
    VectorQueryBuilder queryBuilder = VectorQueryBuilder.newBuilder();
    VectorQueryConstraintsFactory constraintsFactory = queryBuilder.constraintsFactory();

    // Use the constraints factory to create a bounding box constraint
    queryBuilder.constraints(constraintsFactory.cqlConstraints("BBOX(the_geom, -1, -1, 6, 6)"));

    // Only query data from the point type
    queryBuilder.addTypeName(pointTypeAdapter.getTypeName());

    // Build the query
    Query<SimpleFeature> query = queryBuilder.build();

    // Execute the query
    try (CloseableIterator<SimpleFeature> features = myStore.query(query)) {
      // Iterate through the results
      while (features.hasNext()) {
        SimpleFeature feature = features.next();
        // Do something with the feature
      }
    }
    // --------------------------------------------------------------------
    // Verify example
    try (CloseableIterator<SimpleFeature> features = myStore.query(queryBuilder.build())) {
      // Iterate through the results
      int featureCount = 0;
      while (features.hasNext()) {
        features.next();
        featureCount++;
        // Do something with the feature
      }
      Assert.assertEquals(2, featureCount);
    }

    // --------------------------------------------------------------------
    // Aggregation Example !! See Note at Top of Test
    // --------------------------------------------------------------------
    // Create the aggregation query builder
    VectorAggregationQueryBuilder<Persistable, Object> aggregationQueryBuilder =
        VectorAggregationQueryBuilder.newBuilder();

    // Use the constraints factory from the previous example to create a bounding box constraint
    aggregationQueryBuilder.constraints(
        constraintsFactory.cqlConstraints("BBOX(the_geom, -1, -1, 6, 6)"));

    // Configure the query to use a count aggregation on the desired type
    aggregationQueryBuilder.count(pointTypeAdapter.getTypeName());

    // Create the aggregation query
    AggregationQuery<Persistable, Object, SimpleFeature> aggregationQuery =
        aggregationQueryBuilder.build();

    // Perform the aggregation
    long count = (Long) myStore.aggregate(aggregationQuery);
    // --------------------------------------------------------------------
    // Verify example
    Assert.assertEquals(2, count);

    // --------------------------------------------------------------------
    // Statistics Example !! See Note at Top of Test
    // --------------------------------------------------------------------
    // Create the statistics query builder for the BoundingBoxStatistic
    FieldStatisticQueryBuilder<BoundingBoxValue, Envelope> builder =
        SpatialTemporalStatisticQueryBuilder.bbox();

    // Specify the type name
    builder.typeName(pointTypeAdapter.getTypeName());

    // Create the bounding box statistics query
    StatisticQuery<BoundingBoxValue, Envelope> bboxQuery = builder.build();

    // Aggregate the statistic into a single result
    BoundingBoxValue bboxStatValue = myStore.aggregateStatistics(bboxQuery);

    // Get the value
    Envelope bbox = bboxStatValue.getValue();
    // --------------------------------------------------------------------
    // Verify example
    Assert.assertEquals(-5.0, bbox.getMinX(), 0.0001);
    Assert.assertEquals(-5.0, bbox.getMinY(), 0.0001);
    Assert.assertEquals(5.0, bbox.getMaxX(), 0.0001);
    Assert.assertEquals(5.0, bbox.getMaxY(), 0.0001);
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStore;
  }
}
