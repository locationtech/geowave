/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin.heatmap;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.locationtech.geowave.adapter.vector.plugin.GeoWaveDataStoreComponents;
import org.locationtech.geowave.adapter.vector.plugin.GeoWaveFeatureCollection;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.geotime.store.query.aggregate.SpatialSimpleFeatureBinningStrategy;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.TDigestNumericHistogram;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.AggregationQueryBuilder;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.store.query.aggregate.FieldSumAggregation;
import org.locationtech.geowave.core.store.query.aggregate.OptimalCountAggregation;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;

/**
 * Methods for HeatMap aggregation queries.
 *
 * @author M. Zagorski <br>
 * @apiNote Date: 3-25-2022 <br>
 *
 * @apiNote Changelog: <br>
 *
 */
public class HeatMapAggregations {

  public static String SUM_AGGR = "sum_aggr";
  public static String CNT_AGGR = "cnt_aggr";


  /**
   * Builds the field sum aggregation query and returns a SimpleFeatureCollection.
   *
   * @param components {GeoWaveDataStoreComponents} The base components of the dataset.
   * @param jtsBounds {Geometry} The geometry representing the bounds of the GeoServer map viewer
   *        extent.
   * @param geohashPrec {Integer} The Geohash precision to use for binning.
   * @param weightAttr {String} The name of the field in the dataset to which the query is applied.
   * @return {SimpleFeatureCollection} Returns a SimpleFeatureCollection of spatial bin centroids
   *         attributed with the aggregation value of their bin.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static SimpleFeatureCollection buildFieldSumAggrQuery(
      final GeoWaveDataStoreComponents components,
      final QueryConstraints queryConstraints,
      final Geometry jtsBounds,
      final Integer geohashPrec,
      final String weightAttr) {

    // Initialize empty SimpleFeature list
    final List<SimpleFeature> newSimpleFeatures = new ArrayList<>();

    // Initialize new query builder
    final AggregationQueryBuilder<FieldNameParam, BigDecimal, SimpleFeature, ?> queryBuilder =
        AggregationQueryBuilder.newBuilder();

    // Add spatial constraint to optimize the datastore query
    queryBuilder.constraints(queryConstraints);

    // Set up the aggregate
    queryBuilder.aggregate(
        components.getAdapter().getTypeName(),
        new FieldSumAggregation(new FieldNameParam(weightAttr)));

    // Set the index name from the data store
    final Index[] indices = components.getDataStore().getIndices();
    final String indexName = indices[0].getName();
    queryBuilder.indexName(indexName);

    // Build the query with binning strategy
    final AggregationQuery<?, Map<ByteArray, BigDecimal>, SimpleFeature> agg =
        queryBuilder.buildWithBinningStrategy(
            new SpatialSimpleFeatureBinningStrategy(SpatialBinningType.GEOHASH, geohashPrec, true),
            -1);

    // Apply aggregate query to the datastore and get the results
    final Map<ByteArray, BigDecimal> results = components.getDataStore().aggregate(agg);

    // Loop over results and create new SimpleFeature using the centroid of the spatial bin
    for (final Entry<ByteArray, BigDecimal> entry : results.entrySet()) {
      final ByteArray geoHashId = entry.getKey();
      final BigDecimal weightValBigDec = entry.getValue();
      final Double weightVal = weightValBigDec.doubleValue();

      final SimpleFeature simpFeature =
          HeatMapUtils.buildSimpleFeature(
              GeoWaveFeatureCollection.getHeatmapFeatureType(),
              geoHashId,
              weightVal,
              geohashPrec,
              weightAttr,
              SUM_AGGR);

      newSimpleFeatures.add(simpFeature);
    }

    // Add the new simple features to the SimpleFeatureCollection
    final SimpleFeatureCollection newFeatures = DataUtilities.collection(newSimpleFeatures);

    return newFeatures;
  }


  /**
   * Builds the count aggregation query and returns a SimpleFeatureCollection.
   *
   * @param components {GeoWaveDataStoreComponents} The base components of the dataset.
   * @param jtsBounds {Geometry} The geometry representing the bounds of the GeoServer map viewer
   *        extent.
   * @param geohashPrec {Integer} The Geohash precision to use for binning.
   * @param weightAttr {String} The name of the field in the dataset to which the query is applied.
   * @return {SimpleFeatureCollection} Returns a SimpleFeatureCollection of spatial bin centroids
   *         attributed with the aggregation value of their bin.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static SimpleFeatureCollection buildCountAggrQuery(
      // TDigestNumericHistogram histogram,
      final GeoWaveDataStoreComponents components,
      final QueryConstraints queryConstraints,
      final Geometry jtsBounds,
      final Integer geohashPrec,
      final String weightAttr) {

    // Initialize empty SimpleFeature list
    final List<SimpleFeature> newSimpleFeatures = new ArrayList<>();

    // Initialize new query builder
    final AggregationQueryBuilder<FieldNameParam, Long, SimpleFeature, ?> queryBuilder =
        AggregationQueryBuilder.newBuilder();

    // Add spatial constraint to optimize the datastore query
    queryBuilder.constraints(queryConstraints);

    // Set up the aggregation based on the name of the geometry field
    queryBuilder.aggregate(
        components.getAdapter().getTypeName(),
        new OptimalCountAggregation.FieldCountAggregation(
            new FieldNameParam(HeatMapUtils.getGeometryFieldName(components))));

    // Set the index name from the data store
    final Index[] indices = components.getDataStore().getIndices();
    final String indexName = indices[0].getName();
    queryBuilder.indexName(indexName);

    // Build the query with binning strategy
    final AggregationQuery<?, Map<ByteArray, Long>, SimpleFeature> agg =
        queryBuilder.buildWithBinningStrategy(
            new SpatialSimpleFeatureBinningStrategy(SpatialBinningType.GEOHASH, geohashPrec, true),
            -1);

    // Apply aggregate query to the datastore and get the results
    final Map<ByteArray, Long> results = components.getDataStore().aggregate(agg);

    // Loop over results and create new SimpleFeatures using the centroid of the spatial bin
    for (final Entry<ByteArray, Long> entry : results.entrySet()) {
      final ByteArray geoHashId = entry.getKey();
      final Long weightValLong = entry.getValue();
      final Double weightVal = weightValLong.doubleValue();

      final SimpleFeature simpFeature =
          HeatMapUtils.buildSimpleFeature(
              // histogram,
              GeoWaveFeatureCollection.getHeatmapFeatureType(),
              geoHashId,
              weightVal,
              geohashPrec,
              weightAttr,
              CNT_AGGR);

      newSimpleFeatures.add(simpFeature);
    }

    // Add the new simple features to SimpleFeatureCollection
    final SimpleFeatureCollection newFeatures = DataUtilities.collection(newSimpleFeatures);

    return newFeatures;
  }

}
