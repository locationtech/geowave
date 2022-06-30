/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin.heatmap;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.locationtech.geowave.adapter.vector.plugin.GeoWaveDataStoreComponents;
import org.locationtech.geowave.adapter.vector.plugin.GeoWaveFeatureCollection;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.geotime.store.statistics.binning.SpatialFieldValueBinningStrategy;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.DataTypeStatistic;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.core.store.statistics.field.NumericStatsStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericStatsStatistic.NumericStatsValue;
import org.locationtech.geowave.core.store.statistics.field.Stats;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;

/**
 * Methods for HeatMap statistics queries. <br>
 *
 * @author M. Zagorski <br>
 * @apiNote Date: 3-25-2022 <br>
 *
 * @apiNote Changelog: <br>
 *
 */
public class HeatMapStatistics {

  public static String SUM_STATS = "sum_stats";
  public static String CNT_STATS = "cnt_stats";
  public static String GEOHASH_STR = "geohash";


  /**
   * Get the SimpleFeatures with count statistics.
   * 
   * @param components {GeoWaveDataStoreComponents} The GeoWave datastore components.
   * @param typeName {String} The type name of the feature type from components.
   * @param weightAttr {String} The data field name being processed in the statistics.
   * @param geohashPrec {Integer} The Geohash precision for binning purposes.
   * @param jtsBounds {Geometry} The JTS geometry representing the bounds of the data.
   * @return {List<SimpleFeature>} Returns an array list of Simple Features.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static List<SimpleFeature> getSimpleFeaturesWithCountStatistics(
      GeoWaveDataStoreComponents components,
      String typeName,
      String weightAttr,
      Integer geohashPrec,
      Geometry jtsBounds) {

    // Initialize empty SimpleFeature list
    List<SimpleFeature> newSimpleFeatures = new ArrayList<>();

    // Get all data type statistics from the datastore
    DataTypeStatistic<?>[] stats = components.getDataStore().getDataTypeStatistics(typeName);

    for (DataTypeStatistic stat : stats) {

      // Get the tag for the statistic
      String statTag = stat.getTag();

      // Only proceed if the tag contains "geohash"
      if (statTag.contains(GEOHASH_STR)) {

        // Get the statistic Geohash precision from the tag
        Integer statGeohashPrec = Integer.valueOf(statTag.split("-")[3]);

        // Find out if the statistic precision matches the geohash precision
        Boolean matchPrec = statGeohashPrec.equals(geohashPrec);

        // Continue if a count statistic and an instance of spatial field value binning strategy
        if ((stat.getStatisticType() == CountStatistic.STATS_TYPE)
            && (stat.getBinningStrategy() instanceof SpatialFieldValueBinningStrategy)
            && matchPrec) {

          // Get the spatial binning strategy
          SpatialFieldValueBinningStrategy spatialBinningStrategy =
              (SpatialFieldValueBinningStrategy) stat.getBinningStrategy();

          // Continue only if spatial binning strategy type is GEOHASH
          if (spatialBinningStrategy.getType() == SpatialBinningType.GEOHASH) {

            DataTypeStatistic<CountValue> geohashCount = stat;

            // Create new SimpleFeatures from the GeoHash centroid, add the statistic as attribute
            try (CloseableIterator<Pair<ByteArray, Long>> it =
                components.getDataStore().getBinnedStatisticValues(
                    geohashCount,
                    BinConstraints.ofObject(jtsBounds))) {

              // Iterate over all bins and build the SimpleFeature list
              while (it.hasNext()) {
                final Pair<ByteArray, Long> pair = it.next();
                ByteArray geohashId = pair.getLeft();
                Long weightValLong = pair.getRight();
                Double weightVal = weightValLong.doubleValue();

                SimpleFeature simpFeature =
                    HeatMapUtils.buildSimpleFeature(
                        GeoWaveFeatureCollection.getHeatmapFeatureType(),
                        geohashId,
                        weightVal,
                        geohashPrec,
                        weightAttr,
                        CNT_STATS);

                newSimpleFeatures.add(simpFeature);
              }
            }
            break;
          }
        }
      }
    }

    return newSimpleFeatures;
  }


  /**
   * Builds the count statistics query and returns a SimpleFeatureCollection.
   * 
   * @param components {GeoWaveDataStoreComponents} The base components of the dataset.
   * @param jtsBounds {Geometry} The geometry representing the bounds of the GeoServer map viewer
   *        extent.
   * @param geohashPrec {Integer} The Geohash precision to use for binning.
   * @param weightAttr {String} The name of the field in the dataset to which the query is applied.
   * @param createStats {Boolean} User-specified preference to build and calculate the statistics if
   *        they do not exist in the datastore (otherwise, the query will default to the equivalent
   *        aggregation query).
   * @return {SimpleFeatureCollection} Returns a SimpleFeatureCollection of spatial bin centroids
   *         attributed with the aggregation value of their bin.
   */
  public static SimpleFeatureCollection buildCountStatsQuery(
      final GeoWaveDataStoreComponents components,
      final QueryConstraints queryConstraints,
      final Geometry jtsBounds,
      final Integer geohashPrec,
      final String weightAttr,
      final Boolean createStats) {

    // Get type name
    final String typeName = components.getFeatureType().getTypeName();
    // Note - Another way to get the typeName: String typeName =
    // components.getAdapter().getTypeName();

    // Get the simple features with count statistics
    List<SimpleFeature> newSimpleFeatures =
        getSimpleFeaturesWithCountStatistics(
            components,
            typeName,
            weightAttr,
            geohashPrec,
            jtsBounds);

    // Add the new simple features to SimpleFeatureCollection (ok if empty at this point in time)
    SimpleFeatureCollection newFeatures = DataUtilities.collection(newSimpleFeatures);

    // Only proceed if newFeatures is empty (requested statistics do not exist in datastore)
    if (newFeatures.size() == 0) {

      // Add GeoHash field count statistic to datastore and render it when createStats is true
      if (createStats) {
        addGeoHashCountStatisticToDataStore(components, typeName, geohashPrec);

        newSimpleFeatures =
            getSimpleFeaturesWithCountStatistics(
                components,
                typeName,
                weightAttr,
                geohashPrec,
                jtsBounds);

        newFeatures = DataUtilities.collection(newSimpleFeatures);
      } else {

        // Default to the count aggregation query for rendered results
        newFeatures =
            HeatMapAggregations.buildCountAggrQuery(
                components,
                queryConstraints,
                jtsBounds,
                geohashPrec,
                weightAttr);
      }
    }

    return newFeatures;
  }


  /**
   * Programmatically add a GeoHash count statistic to the DataStore. This should only be done once
   * as needed. The count is the number of instance geometries per GeoHash grid cell.
   * 
   * @param components {GeoWaveDataStoreComponents} The base components of the dataset.
   * @param typeName {String} The name of the data layer or dataset.
   * @param geohashPrec {Integer} The Geohash precision to use for binning.
   */
  private static void addGeoHashCountStatisticToDataStore(
      GeoWaveDataStoreComponents components,
      String typeName,
      Integer geohashPrec) {

    // Set up the count statistic
    final CountStatistic geohashCount = new CountStatistic(typeName);

    // Set a tag for information purposes
    String tagStr = "count-stat-geohash-" + geohashPrec;
    geohashCount.setTag(tagStr);

    // Set up spatial binning strategy
    final SpatialFieldValueBinningStrategy geohashSpatialBinning =
        new SpatialFieldValueBinningStrategy(HeatMapUtils.getGeometryFieldName(components));

    // Set the type to GeoHash
    geohashSpatialBinning.setType(SpatialBinningType.GEOHASH);

    // Set the GeoHash precision
    geohashSpatialBinning.setPrecision(geohashPrec);

    // Set the binning strategy
    geohashCount.setBinningStrategy(geohashSpatialBinning);

    // Add statistics to datastore
    components.getDataStore().addStatistic(geohashCount);
  }


  /**
   * Get the SimpleFeatures with field statistics.
   * 
   * @param components {GeoWaveDataStoreComponents} The GeoWave datastore components.
   * @param typeName {String} The type name of the feature type from components.
   * @param weightAttr {String} The data field name being processed in the statistics.
   * @param geohashPrec {Integer} The Geohash precision for binning purposes.
   * @param jtsBounds {Geometry} The JTS geometry representing the bounds of the data.
   * @return {List<SimpleFeature>} Returns an array list of Simple Features.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static List<SimpleFeature> getSimpleFeaturesWithFieldStatistics(
      GeoWaveDataStoreComponents components,
      String typeName,
      String weightAttr,
      Integer geohashPrec,
      Geometry jtsBounds) {

    // Initialize empty SimpleFeature list
    List<SimpleFeature> newSimpleFeatures = new ArrayList<>();

    // Get all data type statistics from the datastore
    FieldStatistic<?>[] stats = components.getDataStore().getFieldStatistics(typeName, weightAttr);

    for (FieldStatistic stat : stats) {

      // Get the tag for the statistic
      final String statTag = stat.getTag();

      // Only proceed if the tag contains "geohash"
      if (statTag.contains(GEOHASH_STR)) {

        // Get the stored Geohash precision from the tag
        final Integer statGeohashPrec = Integer.valueOf(statTag.split("-")[3]);

        // Find out if the statistic precision matches the geohash precision
        final Boolean matchPrec = statGeohashPrec.equals(geohashPrec);

        // Continue if a field sum statistic and an instance of spatial field value binning strategy
        if ((stat.getStatisticType() == NumericStatsStatistic.STATS_TYPE)
            && (stat.getBinningStrategy() instanceof SpatialFieldValueBinningStrategy)
            && matchPrec) {

          // Get the spatial binning strategy
          final SpatialFieldValueBinningStrategy spatialBinningStrategy =
              (SpatialFieldValueBinningStrategy) stat.getBinningStrategy();

          // Continue only if spatial binning strategy type is GEOHASH
          if (spatialBinningStrategy.getType() == SpatialBinningType.GEOHASH) {

            final FieldStatistic<NumericStatsValue> geohashNumeric = stat;

            // Create new SimpleFeatures from the GeoHash centroid and add the statistic and other
            try (CloseableIterator<Pair<ByteArray, Stats>> it =
                components.getDataStore().getBinnedStatisticValues(
                    geohashNumeric,
                    BinConstraints.ofObject(jtsBounds))) {

              // Iterate over all bins and build the SimpleFeature list
              while (it.hasNext()) {
                final Pair<ByteArray, Stats> pair = it.next();
                final ByteArray geoHashId = pair.getLeft();
                final Double fieldSum = pair.getRight().sum();

                // KEEP THIS - Other types of field statistics:
                // Long fieldCount = pair.getRight().count();
                // Double fieldMean = pair.getRight().mean();
                // Double fieldMax = pair.getRight().max();
                // Double fieldMin = pair.getRight().min();

                final SimpleFeature simpFeature =
                    HeatMapUtils.buildSimpleFeature(
                        GeoWaveFeatureCollection.getHeatmapFeatureType(),
                        geoHashId,
                        fieldSum, // TODO: this could be made dynamic
                        geohashPrec,
                        weightAttr,
                        SUM_STATS);

                newSimpleFeatures.add(simpFeature);
              }
            }
            break;
          }
        }
      }
    }
    return newSimpleFeatures;
  }


  /**
   * Builds the field statistics query and returns a SimpleFeatureCollection.
   * 
   * @param components {GeoWaveDataStoreComponents} The base components of the dataset.
   * @param jtsBounds {Geometry} The geometry representing the bounds of the GeoServer map viewer
   *        extent.
   * @param geohashPrec {Integer} The Geohash precision to use for binning.
   * @param weightAttr {String} The name of the field in the dataset to which the query is applied.
   * @param createStats {Boolean} User-specified preference to build and calculate the statistics if
   *        they do not exist in the datastore (otherwise, the query will default to the equivalent
   *        aggregation query).
   * @return {SimpleFeatureCollection} Returns a SimpleFeatureCollection of spatial bin centroids
   *         attributed with the aggregation value of their bin.
   */
  public static SimpleFeatureCollection buildFieldStatsQuery(
      final GeoWaveDataStoreComponents components,
      final QueryConstraints queryConstraints,
      final Geometry jtsBounds,
      final Integer geohashPrec,
      final String weightAttr,
      final Boolean createStats) {

    // Get type name
    final String typeName = components.getFeatureType().getTypeName();

    // Get the simple features if the statistics already exist in the datastore
    List<SimpleFeature> newSimpleFeatures =
        getSimpleFeaturesWithFieldStatistics(
            components,
            typeName,
            weightAttr,
            geohashPrec,
            jtsBounds);

    // Add the new simple features to SimpleFeatureCollection (ok if empty at this point in time)
    SimpleFeatureCollection newFeatures = DataUtilities.collection(newSimpleFeatures);

    // Only proceed if newFeatures is empty (requested statistics do not exist in datastore)
    if (newFeatures.size() == 0) {

      // Add GeoHash field statistic to datastore and render it when createStats is true
      if (createStats) {
        addGeoHashFieldStatisticsToDataStore(components, typeName, geohashPrec, weightAttr);

        newSimpleFeatures =
            getSimpleFeaturesWithFieldStatistics(
                components,
                typeName,
                weightAttr,
                geohashPrec,
                jtsBounds);

        newFeatures = DataUtilities.collection(newSimpleFeatures);

      } else {
        // Default to the count aggregation query for rendered results
        newFeatures =
            HeatMapAggregations.buildFieldSumAggrQuery(
                components,
                queryConstraints,
                jtsBounds,
                geohashPrec,
                weightAttr);
      }
    }

    return newFeatures;
  }


  /**
   * Programmatically add a GeoHash field statistic to the DataStore. This should only be done once
   * as needed. The default statistic is sum, but could be count, mean, max, or min of the selected
   * numeric field.
   *
   * @param components {GeoWaveDataStoreComponents} The base components of the dataset.
   * @param typeName {String} The name of the data layer or dataset.
   * @param geohashPrec {Integer} The Geohash precision to use for binning.
   * @param weightAttr {String} The name of the field in the dataset to which the query is applied.
   */
  private static void addGeoHashFieldStatisticsToDataStore(
      final GeoWaveDataStoreComponents components,
      final String typeName,
      final Integer geohashPrec,
      final String weightAttr) {

    // Set up the field statistic
    final NumericStatsStatistic geohashFieldStat = new NumericStatsStatistic(typeName, weightAttr);

    // Set a tag for information purposes
    String tagStr = "field-stat-geohash-" + geohashPrec;
    geohashFieldStat.setTag(tagStr);

    // Set up spatial binning strategy
    final SpatialFieldValueBinningStrategy geohashSpatialBinning =
        new SpatialFieldValueBinningStrategy(
            components.getFeatureType().getGeometryDescriptor().getLocalName());

    // Set the type to GeoHash
    geohashSpatialBinning.setType(SpatialBinningType.GEOHASH);

    // Set the GeoHash precision
    geohashSpatialBinning.setPrecision(geohashPrec);

    // Set the binning strategy
    geohashFieldStat.setBinningStrategy(geohashSpatialBinning);

    // Add statistics to datastore
    components.getDataStore().addStatistic(geohashFieldStat);
  }

}
