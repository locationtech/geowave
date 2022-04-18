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
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.geotime.store.query.aggregate.SpatialSimpleFeatureBinningStrategy;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.AggregationQueryBuilder;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.store.query.aggregate.FieldSumAggregation;
import org.locationtech.geowave.core.store.query.aggregate.OptimalCountAggregation;
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
   * @param geohashPrec {Integer} The Geohash precision to use for binning.
   * @param weightAttr {String} The name of the field in the dataset to which the query is applied.
   * @return {SimpleFeatureCollection} Returns a SimpleFeatureCollection of spatial bin centroids
   *         attributed with the aggregation value of their bin.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static SimpleFeatureCollection buildFieldSumAggrQuery(
      GeoWaveDataStoreComponents components,
      Integer geohashPrec,
      String weightAttr) {

    // Initialize empty SimpleFeature list
    List<SimpleFeature> newSimpleFeatures = new ArrayList<>();

    // Initialize new query builder
    final AggregationQueryBuilder<FieldNameParam, BigDecimal, SimpleFeature, ?> queryBuilder =
        AggregationQueryBuilder.newBuilder();

    // Set up the aggregate
    queryBuilder.aggregate(
        components.getAdapter().getTypeName(),
        new FieldSumAggregation(new FieldNameParam(weightAttr)));

    // Set the index name from the data store
    Index[] indices = components.getDataStore().getIndices();
    String indexName = indices[0].getName();
    queryBuilder.indexName(indexName);

    // Build the query with binning strategy
    final AggregationQuery<?, Map<ByteArray, BigDecimal>, SimpleFeature> agg =
        queryBuilder.buildWithBinningStrategy(
            new SpatialSimpleFeatureBinningStrategy(SpatialBinningType.GEOHASH, geohashPrec, true),
            -1);

    // Apply aggregate query to the datastore and get the results
    Map<ByteArray, BigDecimal> results =
        (Map<ByteArray, BigDecimal>) components.getDataStore().aggregate(agg);

    // Loop over results and create new SimpleFeature using the centroid of the spatial bin
    for (Entry<ByteArray, BigDecimal> entry : results.entrySet()) {
      ByteArray geoHashId = entry.getKey();
      BigDecimal weightValBigDec = entry.getValue();
      Double weightVal = weightValBigDec.doubleValue();

      SimpleFeature simpFeature =
          HeatMapUtils.buildSimpleFeature(
              components.getAdapter().getFeatureType(),
              geoHashId,
              weightVal,
              geohashPrec,
              weightAttr,
              SUM_AGGR);
      
      //TODO: turn the following into logger output?
//      Object ghID = simpFeature.getAttribute("geoHashId");
//      Object val = simpFeature.getAttribute(weightAttr);
//      System.out.println("\t\tGH ID: " + ghID + " VAL: " + val);

      newSimpleFeatures.add(simpFeature);
    }

    // Add the new simple features to the SimpleFeatureCollection
    SimpleFeatureCollection newFeatures = DataUtilities.collection(newSimpleFeatures);

    return newFeatures;
  }

  
  /**
   * Builds the count aggregation query and returns a SimpleFeatureCollection.
   * 
   * @param components {GeoWaveDataStoreComponents} The base components of the dataset.
   * @param geohashPrec {Integer} The Geohash precision to use for binning.
   * @param weightAttr {String} The name of the field in the dataset to which the query is applied.
   * @return {SimpleFeatureCollection} Returns a SimpleFeatureCollection of spatial bin centroids
   *         attributed with the aggregation value of their bin.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static SimpleFeatureCollection buildCountAggrQuery(
      GeoWaveDataStoreComponents components,
      Integer geohashPrec,
      String weightAttr) {

    // Initialize empty SimpleFeature list
    List<SimpleFeature> newSimpleFeatures = new ArrayList<>();

    // Initialize new query builder
    final AggregationQueryBuilder<FieldNameParam, Long, SimpleFeature, ?> queryBuilder =
        AggregationQueryBuilder.newBuilder();

    // Set up the aggregation based on the name of the geometry field
    queryBuilder.aggregate(
        components.getAdapter().getTypeName(),
        new OptimalCountAggregation.FieldCountAggregation(
            new FieldNameParam(HeatMapUtils.getGeometryFieldName(components))));

    // Set the index name from the data store
    Index[] indices = components.getDataStore().getIndices();
    String indexName = indices[0].getName();
    queryBuilder.indexName(indexName);

    // Build the query with binning strategy
    final AggregationQuery<?, Map<ByteArray, Long>, SimpleFeature> agg =
        queryBuilder.buildWithBinningStrategy(
            new SpatialSimpleFeatureBinningStrategy(SpatialBinningType.GEOHASH, geohashPrec, true),
            -1);

    // Apply aggregate query to the datastore and get the results
    Map<ByteArray, Long> results = (Map<ByteArray, Long>) components.getDataStore().aggregate(agg);

    // Loop over results and create new SimpleFeatures using the centroid of the spatial bin
    for (Entry<ByteArray, Long> entry : results.entrySet()) {
      ByteArray geoHashId = entry.getKey();
      Long weightValLong = entry.getValue();
      Double weightVal = weightValLong.doubleValue();

      SimpleFeature simpFeature =
          HeatMapUtils.buildSimpleFeature(
              components.getAdapter().getFeatureType(),
              geoHashId,
              weightVal,
              geohashPrec,
              weightAttr,
              CNT_AGGR);

      //TODO: turn the following into logger output?
//      Object ghID = simpFeature.getAttribute("geohashId");
//      Object cntAggr = simpFeature.getAttribute(weightAttr);
//      System.out.println("\tGEOHASH ID: " + ghID + " COUNT AGGR: " + cntAggr);

      newSimpleFeatures.add(simpFeature);
    }

    // Add the new simple features to SimpleFeatureCollection
    SimpleFeatureCollection newFeatures = DataUtilities.collection(newSimpleFeatures);

    return newFeatures;
  }

}
