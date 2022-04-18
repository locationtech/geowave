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
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.geowave.adapter.vector.plugin.GeoWaveDataStoreComponents;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.geotime.store.query.aggregate.SpatialBinningStrategy;
import org.locationtech.geowave.core.geotime.store.statistics.binning.SpatialFieldValueBinningStrategy;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataTypeStatistic;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticBinningStrategy;
import org.locationtech.geowave.core.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.core.store.statistics.field.NumericStatsStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericStatsStatistic.NumericStatsValue;
import org.locationtech.geowave.core.store.statistics.field.Stats;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;

/**
 * Methods for HeatMap statistics queries.
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

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static SimpleFeatureCollection buildCountStatsQuery(
      GeoWaveDataStoreComponents components,
      Integer geohashPrec,
      String weightAttr,
      Boolean createStats) {
    System.out.println("STATS - STARTING buildCountStatsQuery");

    System.out.println("\tWEIGHT ATTRIBUTE: " + weightAttr);
    System.out.println("\tCREATE STATS: " + createStats);

    // components.getDataStore().recalcStatistic(null);
    // components.getDataStore().exists(Statistic.get("Geohash-binning")); //HOW TO DO THIS?

    // input an Envelop instead of geohashPrec
    // NEW INPUT: output width and height in pixels, envelope, pixels/grid cell
    // find the size of grid cell in decimal degrees (there is a helper to find size in
    // decimal degrees)
    // MATH: (width in pixels / (pixels/gridcell)) width in decimal degrees (unit: grid cells)
    // THEN plug in line 50
    // do math in both width and height. Take the product of width x height = TARGET tot
    // number of grid cells.

    // // Get total cell counts for each GeoHash precision
    // int holdAbsDiff = 0;
    // Map<Integer, Integer> geoHashPrecGridCnt = new HashMap();
    // for (int i = startInt; i <= endInt; i++) {
    // System.out.println("\tGEOHASH PREC: " + i);
    //// ByteArray[] arrayOfHashes = SpatialBinningType.GEOHASH.getSpatialBins(jtsBounds, i);
    // int cntCellsAtPrec = (SpatialBinningType.GEOHASH.getSpatialBins(jtsBounds, i)).length;
    //// int cntCellsAtPrec = arrayOfHashes.length;
    // int absDiff = Math.abs(cntCellsAtPrec - totCellsTarget);
    // System.out.println("\tABS DIFF: " + absDiff);
    // geoHashPrecGridCnt.put(absDiff, i);
    // }

    // // Sort the absolute difference values
    // List<Integer> absDiffVals = new ArrayList(geoHashPrecGridCnt.keySet());
    // Collections.sort(absDiffVals);
    // System.out.println("\tABS DIFF VALS SORTED: " + absDiffVals);
    //
    // // Get the closest cell count match and corresponding GeoHash precision
    // int geohashPrec1 = geoHashPrecGridCnt.get(absDiffVals.get(0));
    // System.out.println("\tIDEAL GEOHASH PREC: " + geohashPrec1);

    // Remove all statistics from the data store for now
    // components.getStatsStore().removeAll();
    // components.getDataStore().remove

    // Initialize empty SimpleFeature list
    List<SimpleFeature> newSimpleFeatures = new ArrayList<>();

    // Get type name
    String typeName = components.getFeatureType().getTypeName();
    System.out.println("\tADAPTER TYPE NAME: " + components.getAdapter().getTypeName());
    System.out.println("\tFEATURE TYPE NAME: " + typeName);

    // Get all data type statistics from the datastore
    DataTypeStatistic<?>[] stats = components.getDataStore().getDataTypeStatistics(typeName);

    System.out.println("\tSTATS CNT IN DATASTORE: " + stats.length);

    int cntCountStatsGeoHash = 0;

    for (DataTypeStatistic stat : stats) {

      String statTag = stat.getTag();
      System.out.println("\tSTAT TAG: " + statTag);

      if (statTag.contains(GEOHASH_STR)) {
        Integer statGeohashPrec = Integer.valueOf(statTag.split("-")[3]);
        System.out.println("\tSTAT GEOHASH PREC FROM TAG: " + statGeohashPrec);

        // Find out if the statistic precision matches the geohash precision
        // Boolean matchPrec = (statGeohashPrec == geohashPrec);
        Boolean matchPrec = statGeohashPrec.equals(geohashPrec);
        System.out.println("\tSTAT GEOHASH TAG MATCHES PREC: " + matchPrec);

        // Continue if a count statistic and an instance of spatial field value binning strategy
        if (stat.getStatisticType() == CountStatistic.STATS_TYPE
            && stat.getBinningStrategy() instanceof SpatialFieldValueBinningStrategy
            && matchPrec) {

          // Get the spatial binning strategy
          SpatialFieldValueBinningStrategy spatialBinningStrategy =
              (SpatialFieldValueBinningStrategy) stat.getBinningStrategy();

          // Continue only if spatial binning strategy type is GEOHASH
          if (spatialBinningStrategy.getType() == SpatialBinningType.GEOHASH) {
            cntCountStatsGeoHash++;

            DataTypeStatistic<CountValue> geohashCount = stat;

            // Create new SimpleFeatures from the GeoHash centroid and add the statistics and other
            // information
            // results for that GeoHash cell
            try (CloseableIterator<Pair<ByteArray, Long>> it =
                components.getDataStore().getBinnedStatisticValues(geohashCount)) {

              // Iterate over all bins and build the SimpleFeature list
              while (it.hasNext()) {
                final Pair<ByteArray, Long> pair = it.next();
                System.out.println(
                    String.format(
                        "STATS - Count: %d, Bin: %s, Bin Geometry: %s",
                        pair.getRight(),
                        spatialBinningStrategy.binToString(pair.getLeft()),
                        spatialBinningStrategy.getType().getBinGeometry(
                            pair.getLeft(),
                            geohashPrec)));
                ByteArray geoHashId = pair.getLeft();
                Long weightValLong = pair.getRight();
                Double weightVal = weightValLong.doubleValue();

                SimpleFeature simpFeature =
                    HeatMapUtils.buildSimpleFeature(
                        components.getAdapter().getFeatureType(),
                        geoHashId,
                        weightVal,
                        geohashPrec,
                        weightAttr,
                        CNT_STATS);
                System.out.println("\tSTATS - SIMPLE FEATURE: " + simpFeature);
                Object ghID = simpFeature.getAttribute("geohashId");
                Object cntStat = simpFeature.getAttribute(weightAttr);
                System.out.println("\tGEOHASH ID: " + ghID + " CNT STAT: " + cntStat);

                newSimpleFeatures.add(simpFeature);
              }
              // Close the iterator
              it.close();
            }
            break;
          }
        }
      }
    }

    // Add the new simple features to SimpleFeatureCollection (ok if empty at this point in time)
    SimpleFeatureCollection newFeatures = DataUtilities.collection(newSimpleFeatures);
    System.out.println("\tSIZE OF NEW SIMPLE FEATURE COLLECTION INIT: " + newFeatures.size());

    System.out.println("\tcntCountStatsGeoHash: " + cntCountStatsGeoHash);
    if (cntCountStatsGeoHash == 0) { // TODO: change this to if newFeatures = 0 or is empty
      // return aggr version of statistics
      System.out.println(
          "THERE ARE NO GEOHASH COUNT STATISTICS IN THE DATASTORE - ADDING THEM NOW!");

      // Add the GeoHash count statistic to the datastore so that next time it is available
      if (createStats) {
        System.out.println("\tCREATING STATS - count");
        addGeoHashCountStatisticToDataStore(components, typeName, geohashPrec);
      }

      // In the meantime, default to the count aggregation query for rendered results
      newFeatures = HeatMapAggregations.buildCountAggrQuery(components, geohashPrec, weightAttr);
    }

    System.out.println("\tNEW SIMPLE FEATURE CNT: " + newSimpleFeatures.size());
    System.out.println("\tSIZE OF NEW SIMPLE FEATURE COLLECTION: " + newFeatures.size());
    System.out.println("\tDONE WITH COUNT STATISTICS!");

    return newFeatures;
  }

  /**
   * Programmatically add a GeoHash count statistic to the DataStore. This should only be done once
   * as needed. The count is the number of instance geometries per GeoHash grid cell.
   */
  private static void addGeoHashCountStatisticToDataStore(
      GeoWaveDataStoreComponents components,
      String typeName,
      Integer geohashPrec) {

    System.out.println("HEATMAP STATS - STARTING addGeoHashCountStatisticToDataStore");
    System.out.println("\ttypeName: " + typeName);
    System.out.println("\tgeohashPrec: " + geohashPrec);

    // Set up the count statistic
    final CountStatistic geohashCount = new CountStatistic(typeName);

    // Set a tag for information purposes
    String tagStr = "count-stat-geohash-" + geohashPrec;
    System.out.println("\tTAG STRING: " + tagStr);
    geohashCount.setTag(tagStr);
    // geohashCount.setTag("Geohash-binning-count-stat");
    System.out.println("\tgeohashCount2: " + geohashCount.getDescription());

    // Set up spatial binning strategy
    final SpatialFieldValueBinningStrategy geohashSpatialBinning =
        new SpatialFieldValueBinningStrategy(HeatMapUtils.getGeometryFieldName(components));

    System.out.println(
        "\tGEOM LOCAL NAME: " + components.getFeatureType().getGeometryDescriptor().getLocalName());
    System.out.println("\tgeohashSpatialBinning1: " + geohashSpatialBinning.getDescription());

    // Set the type to GeoHash
    geohashSpatialBinning.setType(SpatialBinningType.GEOHASH);
    System.out.println("\tgeohashSpatialBinning2: " + geohashSpatialBinning.getStrategyName());

    // Set the GeoHash precision
    System.out.println("\tGEOHASH PRECISION: " + geohashPrec);
    geohashSpatialBinning.setPrecision(geohashPrec);
    System.out.println("\tgeohashSpatialBinning3: " + geohashSpatialBinning.getPrecision());

    // Set the binning strategy
    geohashCount.setBinningStrategy(geohashSpatialBinning);
    System.out.println("\tgeohashCount3: " + geohashCount);

    // Add statistics to datastore
    components.getDataStore().addStatistic(geohashCount);
    System.out.println("\tDONE ADDING COUNT STATISTICS TO DATASTORE");
  }


  @SuppressWarnings({"rawtypes", "unchecked"})
  public static SimpleFeatureCollection buildFieldStatsQuery(
      GeoWaveDataStoreComponents components,
      Integer geohashPrec,
      String weightAttr,
      Boolean createStats) {
    System.out.println("STATS - STARTING buildFieldStatsQuery");

    System.out.println("\tCREATE STATS: " + createStats);

    // components.getDataStore().recalcStatistic(null);

    // Initialize empty SimpleFeature list
    List<SimpleFeature> newSimpleFeatures = new ArrayList<>();

    // Get type name
    String typeName = components.getFeatureType().getTypeName();
    System.out.println("\tADAPTER TYPE NAME: " + components.getAdapter().getTypeName());
    System.out.println("\tFEATURE TYPE NAME: " + typeName);

    // Get all data type statistics from the datastore
    FieldStatistic<?>[] stats = components.getDataStore().getFieldStatistics(typeName, weightAttr);
    System.out.println("\tSTATS CNT IN DATASTORE: " + stats.length);

    int cntFieldStats = 0;

    for (FieldStatistic stat : stats) {
      System.out.println("\tITER OVER STATS - STAT: " + stat.getDescription());
      System.out.println("\tITER OVER STATS - STAT TYPE: " + stat.getStatisticType());
      System.out.println("\tITER OVER STATS - STAT BIN STRATEGY: " + stat.getBinningStrategy());
      System.out.println("\tITER OVER STATS - STAT TAG: " + stat.getTag());

      String statTag = stat.getTag();
      System.out.println("\tSTAT TAG: " + statTag);

      if (statTag.contains(GEOHASH_STR)) {
        Integer statGeohashPrec = Integer.valueOf(statTag.split("-")[3]);
        System.out.println("\tSTAT GEOHASH PREC FROM TAG: " + statGeohashPrec);

        // Find out if the statistic precision matches the geohash precision
        // Boolean matchPrec = (statGeohashPrec == geohashPrec);
        Boolean matchPrec = statGeohashPrec.equals(geohashPrec);
        System.out.println("\tSTAT GEOHASH TAG MATCHES PREC: " + matchPrec);

        // Continue if a field sum statistic and an instance of spatial field value binning strategy
        if (stat.getStatisticType() == NumericStatsStatistic.STATS_TYPE
            && stat.getBinningStrategy() instanceof SpatialFieldValueBinningStrategy
            && matchPrec) {

          System.out.println("\tNUMERIC STATS EXISTS IN DATASTORE!");

          // Get the spatial binning strategy
          SpatialFieldValueBinningStrategy spatialBinningStrategy =
              (SpatialFieldValueBinningStrategy) stat.getBinningStrategy();

          // Continue only if spatial binning strategy type is GEOHASH
          if (spatialBinningStrategy.getType() == SpatialBinningType.GEOHASH) {
            cntFieldStats++;

            FieldStatistic<NumericStatsValue> geohashNumeric = stat;

            // Create new SimpleFeatures from the GeoHash centroid and add the statistics and other
            // information
            // results for that GeoHash cell
            try (CloseableIterator<Pair<ByteArray, Stats>> it =
                components.getDataStore().getBinnedStatisticValues(geohashNumeric)) {

              // Iterate over all bins and build the SimpleFeature list
              while (it.hasNext()) {
                final Pair<ByteArray, Stats> pair = it.next();
                ByteArray geoHashId = pair.getLeft();
                Double fieldSum = pair.getRight().sum();
                Long fieldCount = pair.getRight().count();
                Double fieldMean = pair.getRight().mean();
                Double fieldMax = pair.getRight().max();
                Double fieldMin = pair.getRight().min();
                System.out.println("\tGEOHASH ID: " + geoHashId);
                System.out.println("\tFIELD SUM: " + fieldSum);
                System.out.println("\tFIELD COUNT: " + fieldCount);
                System.out.println("\tFIELD MEAN: " + fieldMean);
                System.out.println("\tFIELD MAX: " + fieldMax);
                System.out.println("\tFIELD MIN: " + fieldMin);

                SimpleFeature simpFeature =
                    HeatMapUtils.buildSimpleFeature(
                        components.getAdapter().getFeatureType(),
                        geoHashId,
                        fieldSum, // TODO: make the field stats method user dynamic (input from
                                  // heatmap
                                  // sld)
                        geohashPrec,
                        weightAttr,
                        SUM_STATS);
                System.out.println("\tSTATS - SIMPLE FEATURE: " + simpFeature);
                Object ghID = simpFeature.getAttribute("geoHashId");
                Object val = simpFeature.getAttribute(weightAttr);
                System.out.println("\tSTATS - GH ID: " + ghID + " VAL: " + val);

                newSimpleFeatures.add(simpFeature);
              }
              // Close the iterator
              it.close();
            }
            break;
          }
        }
      }
    }

    // Add the new simple features to SimpleFeatureCollection (ok if empty at this point in time)
    SimpleFeatureCollection newFeatures = DataUtilities.collection(newSimpleFeatures);
    System.out.println("\tSIZE OF NEW SIMPLE FEATURE COLLECTION INIT: " + newFeatures.size());

    System.out.println("\tcntFieldSumStats: " + cntFieldStats);
    if (cntFieldStats == 0) { // TODO: can replace with newFeatures.size() == 0, etc.
      // return aggr version of statistics
      System.out.println(
          "THERE ARE NO GEOHASH FIELD SUM STATISTICS IN THE DATASTORE - ADDING THEM NOW!");

      // Add the GeoHash count statistic to the datastore so that next time it is available
      if (createStats) {
        System.out.println("\tCREATING STATS - field stats");
        addGeoHashFieldStatisticsToDataStore(components, typeName, geohashPrec, weightAttr);
      }

      // In the meantime, default to the count aggregation query for rendered results
      newFeatures = HeatMapAggregations.buildFieldSumAggrQuery(components, geohashPrec, weightAttr);
    }

    System.out.println("\tNEW SIMPLE FEATURE CNT: " + newSimpleFeatures.size());
    System.out.println("\tSIZE OF NEW SIMPLE FEATURE COLLECTION: " + newFeatures.size());
    System.out.println("\tDONE WITH FIELD STATISTICS!");

    return newFeatures;
  }

  /**
   * Programmatically add a GeoHash count statistic to the DataStore. This should only be done once
   * as needed. The count is the number of instance geometries per GeoHash grid cell.
   */
  private static void addGeoHashFieldStatisticsToDataStore(
      GeoWaveDataStoreComponents components,
      String typeName,
      Integer geohashPrec,
      String weightAttr) {

    System.out.println("HEATMAP STATS - STARTING addGeoHashFieldStatisticsToDataStore");
    System.out.println("\ttypeName: " + typeName);
    System.out.println("\tgeohashPrec: " + geohashPrec);

    // Set up the field statistic
    final NumericStatsStatistic geohashFieldStat = new NumericStatsStatistic(typeName, weightAttr);

    System.out.println("\tgeohashFieldStat1: " + geohashFieldStat.getDescription());

    // Set a tag for information purposes
    String tagStr = "field-stat-geohash-" + geohashPrec;
    System.out.println("\tTAG STRING: " + tagStr);
    geohashFieldStat.setTag(tagStr);
    System.out.println("\tgeohashFieldStat2: " + geohashFieldStat.getDescription());

    // Set up spatial binning strategy
    final SpatialFieldValueBinningStrategy geohashSpatialBinning =
        new SpatialFieldValueBinningStrategy(
            components.getFeatureType().getGeometryDescriptor().getLocalName());
    System.out.println(
        "\tGEOM LOCAL NAME: " + components.getFeatureType().getGeometryDescriptor().getLocalName());
    System.out.println("\tgeohashSpatialBinning1: " + geohashSpatialBinning.getDescription());

    // Set the type to GeoHash
    geohashSpatialBinning.setType(SpatialBinningType.GEOHASH);
    System.out.println("\tgeohashSpatialBinning2: " + geohashSpatialBinning.getStrategyName());

    // Set the GeoHash precision
    System.out.println("\tGEOHASH PRECISION: " + geohashPrec);
    geohashSpatialBinning.setPrecision(geohashPrec);
    System.out.println("\tgeohashSpatialBinning3: " + geohashSpatialBinning.getPrecision());

    // Set the binning strategy
    geohashFieldStat.setBinningStrategy(geohashSpatialBinning);
    System.out.println("\tgeohashFieldStat3: " + geohashFieldStat);

    // Add statistics to datastore
    components.getDataStore().addStatistic(geohashFieldStat);
    System.out.println("\tDONE ADDING FIELD STATISTICS TO DATASTORE");
  }

}
