/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.examples.aggregation.binning;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.query.aggregate.SpatialSimpleFeatureBinningStrategy;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.AggregationQueryBuilder;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.store.query.aggregate.FieldSumAggregation;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * This class provides an example of how to create a binned aggregation for your data. You may want
 * to use a binned aggregation if you need to sort your data into buckets and process the buckets
 * individually. <p> This example counts the population after grouping the data by geohash, by S3,
 * and by S2.
 */
public class SpatialBinningAggregationExample {
  public static void main(final String[] args) {
    // this example shows binning using geohashes but it can easily use Google's S2 or Uber's H3 as
    // well for spatial binning
    final SimpleFeatureType featureType = getSimpleFeatureType();
    // Points (to be ingested into GeoWave Data Store)
    final List<SimpleFeature> cannedFeatures =
        ImmutableList.of(
            buildSimpleFeature(featureType, "Loc1", new Coordinate(-77.0352, 38.8895), 12),
            buildSimpleFeature(featureType, "Loc2", new Coordinate(-77.0366, 38.8977), 13),
            buildSimpleFeature(featureType, "Loc3", new Coordinate(-76.8644, 38.9078), 8),
            buildSimpleFeature(featureType, "Loc4", new Coordinate(-76.350677, 38.9641511), 15),
            buildSimpleFeature(featureType, "Loc5", new Coordinate(-77.3384112, 38.416091), 7),
            buildSimpleFeature(featureType, "Loc6", new Coordinate(-67.0352, 28.8895), 3),
            buildSimpleFeature(featureType, "Loc7", new Coordinate(-67.0366, 28.8977), 99),
            buildSimpleFeature(featureType, "Loc8", new Coordinate(-66.8644, 28.9078), 0),
            buildSimpleFeature(featureType, "Loc9", new Coordinate(-66.350677, 28.9641511), 1),
            buildSimpleFeature(featureType, "Loc10", new Coordinate(-67.3384112, 28.416091), 23));

    final Index index =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    final DataStore dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());
    final FeatureDataAdapter adapter = new FeatureDataAdapter(featureType);

    // Ingest cannedFeatures into the DataStore.
    dataStore.addType(adapter, index);
    try (Writer<SimpleFeature> indexWriter = dataStore.createWriter(adapter.getTypeName())) {
      for (final SimpleFeature sf : cannedFeatures) {
        indexWriter.write(sf);
      }
    }

    // calculate the population count for each precision from 1 to 6.
    // a geohash like g5c is a hash that is contained by the geohash g5,
    // which is contained by the geohash g.
    final Map<ByteArray, BigDecimal> allResults = Maps.newHashMapWithExpectedSize(100);
    for (int i = 6; i > 0; i--) {
      // execute a binned Aggregation, return the results
      // -1 maxBins means no max.
      allResults.putAll(
          executeBinningAggregation(i, index.getName(), adapter.getTypeName(), dataStore, -1));
    }

    System.out.printf(
        "Results for precision 1-6: %s%n",
        Arrays.toString(
            allResults.entrySet().stream().map(
                e -> Pair.of(
                    SpatialBinningType.GEOHASH.binToString(e.getKey().getBytes()),
                    e.getValue())).map(p -> p.getKey() + "=" + p.getValue()).toArray(
                        String[]::new)));

    System.out.printf(
        "Results just for precision 6: %s%n",
        Arrays.toString(
            allResults.entrySet().stream().filter((e) -> e.getKey().getBytes().length == 6).map(
                e -> Pair.of(
                    SpatialBinningType.GEOHASH.binToString(e.getKey().getBytes()),
                    e.getValue())).map(p -> p.getKey() + "=" + p.getValue()).toArray(
                        String[]::new)));

    // when maxBins is used, it will simply drop any new data that comes in.
    final Map<ByteArray, BigDecimal> maxed =
        executeBinningAggregation(8, index.getName(), adapter.getTypeName(), dataStore, 5);
    System.out.printf(
        "Results limited to the first 5 bins: %s%n",
        Arrays.toString(
            maxed.entrySet().stream().map(
                e -> Pair.of(
                    SpatialBinningType.GEOHASH.binToString(e.getKey().getBytes()),
                    e.getValue())).map(p -> p.getKey() + "=" + p.getValue()).toArray(
                        String[]::new)));

  }

  /**
   * This method creates a binning aggregation that groups the data in the dataStore by the given
   * precision, and sums all of the entries in the group.
   *
   * @param precision The geohash precision to use during binning.
   * @param indexName The index to query
   * @param typeName The name of the registered type adapter to use for serialization purposes.
   * @param dataStore where we have stored the data that we will aggregate.
   * @return Aggregated and computed data. Each entry has a key that is the geohash, and a value
   *         that is the population in that geohash.
   */
  private static Map<ByteArray, BigDecimal> executeBinningAggregation(
      final int precision,
      final String indexName,
      final String typeName,
      final DataStore dataStore,
      final int maxBins) {
    final AggregationQueryBuilder<FieldNameParam, BigDecimal, SimpleFeature, ?> queryBuilder =
        AggregationQueryBuilder.newBuilder();

    queryBuilder.indexName(indexName);
    // Use `.count` instead of `aggregate` if you simply want to count the amount of rows
    // queryBuilder.count("geometry");
    // aggregate uses a provided aggregation to form data.
    queryBuilder.aggregate(typeName, new FieldSumAggregation(new FieldNameParam("population")));
    // `.bin` uses the current aggregation (the VectorSumAggregation in this case),
    // but adds a binning strategy on top of it.
    // each bin uses a fresh aggregation, so there is no contamination between aggregations.
    // P here is BinningAggregationOptions<FieldNameParam, SimpleFeature> But Java lets us elide it.

    // NOTE: here's where SpatialBinningType could instead be Google's S2 or Uber's H3 if desired
    final AggregationQuery<?, Map<ByteArray, BigDecimal>, SimpleFeature> agg =
        queryBuilder.buildWithBinningStrategy(
            new SpatialSimpleFeatureBinningStrategy(SpatialBinningType.GEOHASH, precision, true),
            maxBins);

    // Aggregate the data in the dataStore with the AggregationQuery.
    return dataStore.aggregate(agg);
  }

  /**
   * A helper that constructs the SimpleFeatureType used in this example.
   */
  private static SimpleFeatureType getSimpleFeatureType() {
    final String name = "ExampleSimpleFeatureType";
    final SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
    final AttributeTypeBuilder atBuilder = new AttributeTypeBuilder();
    sftBuilder.setName(name);
    // the location name isn't used in this example, its just here for show!
    sftBuilder.add(atBuilder.binding(String.class).nillable(false).buildDescriptor("locationName"));
    // this is used for the grouping (the `.bin` call).
    sftBuilder.add(atBuilder.binding(Geometry.class).nillable(false).buildDescriptor("geometry"));
    // this is the field that is summed in each group, as defined by the `.aggregate` call.
    sftBuilder.add(atBuilder.binding(Integer.class).nillable(false).buildDescriptor("population"));

    return sftBuilder.buildFeatureType();
  }

  /**
   * Just a helper method to create a SimpleFeature to the specifications used in this example.
   */
  private static SimpleFeature buildSimpleFeature(
      final SimpleFeatureType featureType,
      final String locationName,
      final Coordinate coordinate,
      final int population) {
    final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(featureType);
    builder.set("locationName", locationName);
    builder.set("geometry", GeometryUtils.GEOMETRY_FACTORY.createPoint(coordinate));
    builder.set("population", population);

    return builder.buildFeature(locationName);
  }
}
