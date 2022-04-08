/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.spatial;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.locationtech.geowave.analytic.spark.GeoWaveIndexedRDD;
import org.locationtech.geowave.analytic.spark.GeoWaveRDD;
import org.locationtech.geowave.analytic.spark.RDDUtils;
import org.locationtech.geowave.analytic.spark.sparksql.udf.GeomFunction;
import org.locationtech.geowave.analytic.spark.spatial.JoinOptions.BuildSide;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalOptions;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.HierarchicalNumericIndexStrategy.SubStrategy;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.sfc.SFCFactory.SFCType;
import org.locationtech.geowave.core.index.sfc.tiered.SingleTierSubStrategy;
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import jersey.repackaged.com.google.common.collect.Maps;
import scala.Tuple2;

public class TieredSpatialJoin extends JoinStrategy {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  private static final Logger LOGGER = LoggerFactory.getLogger(TieredSpatialJoin.class);

  // Combined matching pairs
  private JavaPairRDD<GeoWaveInputKey, ByteArray> combinedResults = null;
  private final List<JavaPairRDD<GeoWaveInputKey, ByteArray>> tierMatches = Lists.newArrayList();

  private double bufferDistance = 0.0;

  public TieredSpatialJoin() {}

  @Override
  public void join(
      final SparkSession spark,
      final GeoWaveIndexedRDD leftRDD,
      final GeoWaveIndexedRDD rightRDD,
      final GeomFunction predicate) throws InterruptedException, ExecutionException {
    // Get SparkContext from session
    final SparkContext sc = spark.sparkContext();
    final JavaSparkContext javaSC = JavaSparkContext.fromSparkContext(sc);

    final NumericIndexStrategy leftStrategy = leftRDD.getIndexStrategy().getValue();
    final NumericIndexStrategy rightStrategy = rightRDD.getIndexStrategy().getValue();

    // Check if either dataset supports the join
    TieredSFCIndexStrategy tieredStrategy = null;
    // Determine if either strategy needs to be reindexed to support join algorithm
    boolean reindexLeft = false;
    boolean reindexRight = false;
    final boolean leftSupport = supportsJoin(leftStrategy);
    final boolean rightSupport = supportsJoin(rightStrategy);
    if (leftSupport && rightSupport) {
      if (leftStrategy.equals(rightStrategy)) {
        // Both strategies match we don't have to reindex
        tieredStrategy = (TieredSFCIndexStrategy) leftStrategy;
      } else {
        // Join build side determines what side we will build strategy off of when strategies
        // support but don't match
        if (getJoinOptions().getJoinBuildSide() == JoinOptions.BuildSide.LEFT) {
          reindexRight = true;
          tieredStrategy = (TieredSFCIndexStrategy) leftStrategy;
        } else {
          reindexLeft = true;
          tieredStrategy = (TieredSFCIndexStrategy) rightStrategy;
        }
      }
    } else if (leftSupport) {
      reindexRight = true;
      tieredStrategy = (TieredSFCIndexStrategy) leftStrategy;

    } else if (rightSupport) {
      reindexLeft = true;
      tieredStrategy = (TieredSFCIndexStrategy) rightStrategy;

    } else {
      tieredStrategy = (TieredSFCIndexStrategy) createDefaultStrategy(leftStrategy);
      if (tieredStrategy == null) {
        tieredStrategy = (TieredSFCIndexStrategy) createDefaultStrategy(rightStrategy);
      }
      if (tieredStrategy == null) {
        LOGGER.error(
            "Cannot create default strategy from either provided strategy. Datasets cannot be joined.");
        return;
      }
      reindexLeft = true;
      reindexRight = true;
    }

    // Pull information and broadcast strategy used for join
    final SubStrategy[] tierStrategies = tieredStrategy.getSubStrategies();
    final int tierCount = tierStrategies.length;
    // Create broadcast variable for indexing strategy
    // Cast is safe because we must be instance of TieredSFCIndexStrategy to support join.
    final Broadcast<TieredSFCIndexStrategy> broadcastStrategy =
        (Broadcast<TieredSFCIndexStrategy>) RDDUtils.broadcastIndexStrategy(sc, tieredStrategy);

    final Broadcast<GeomFunction> geomPredicate = javaSC.broadcast(predicate);

    // If needed reindex one of the strategies we will wrap the buffer operation into the reindex
    // operation
    // Otherwise we buffer based off the buildside of the join.
    setBufferAmount(predicate.getBufferAmount());

    // Reindex if necessary and get RDD of indexed Geometry
    JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> leftIndex = null;
    JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> rightIndex = null;
    if (reindexLeft && reindexRight) {
      leftRDD.reindex(broadcastStrategy);
      rightRDD.reindex(broadcastStrategy);
    } else if (reindexLeft) {
      leftRDD.reindex(broadcastStrategy);
    } else if (reindexRight) {
      rightRDD.reindex(broadcastStrategy);
    }

    if (joinOpts.getJoinBuildSide() == BuildSide.LEFT) {
      rightIndex = rightRDD.getIndexedGeometryRDD(bufferDistance, true);
      leftIndex = leftRDD.getIndexedGeometryRDD();
    } else {
      leftIndex = leftRDD.getIndexedGeometryRDD(bufferDistance, true);
      rightIndex = rightRDD.getIndexedGeometryRDD();
    }

    final int leftPartCount = leftIndex.getNumPartitions();
    final int rightPartCount = rightIndex.getNumPartitions();
    final int highestPartCount = (leftPartCount > rightPartCount) ? leftPartCount : rightPartCount;
    final int largePartitionerCount = (int) (1.5 * highestPartCount);
    final HashPartitioner partitioner = new HashPartitioner(largePartitionerCount);

    final JavaFutureAction<List<Byte>> leftFuture =
        leftIndex.setName("LeftIndex").keys().map(t -> t.getBytes()[0]).distinct(4).collectAsync();
    final JavaFutureAction<List<Byte>> rightFuture =
        rightIndex.setName("RightIndex").keys().map(t -> t.getBytes()[0]).distinct(
            4).collectAsync();

    // Get the result of future
    final List<Byte> rightDataTiers = Lists.newArrayList(rightFuture.get());

    // Sort tiers highest to lowest and collect information.
    final Byte[] rightTierArr = rightDataTiers.toArray(new Byte[0]);
    Arrays.sort(rightTierArr);
    final int rightTierCount = rightTierArr.length;

    final List<Byte> leftDataTiers = Lists.newArrayList(leftFuture.get());
    final Byte[] leftTierArr = leftDataTiers.toArray(new Byte[0]);
    Arrays.sort(leftTierArr);
    final int leftTierCount = leftTierArr.length;

    // Determine if there are common higher tiers for whole dataset on either side.
    final byte highestLeftTier = leftTierArr[leftTierArr.length - 1];
    final byte highestRightTier = rightTierArr[rightTierArr.length - 1];
    // Find a common run of higher tiers
    Byte[] commonLeftTiers = ArrayUtils.EMPTY_BYTE_OBJECT_ARRAY;
    Byte[] commonRightTiers = ArrayUtils.EMPTY_BYTE_OBJECT_ARRAY;
    boolean skipMapCreate = false;
    if (leftTierArr[0] > highestRightTier) {
      // Whole left dataset is higher tiers than right
      commonLeftTiers = leftTierArr;
      skipMapCreate = true;
    } else if (rightTierArr[0] > highestLeftTier) {
      // Whole right dataset is higher tiers than left
      commonRightTiers = rightTierArr;
      skipMapCreate = true;
    }

    LOGGER.debug("Tier Count: " + tierCount);
    LOGGER.debug("Left Tier Count: " + leftTierCount + " Right Tier Count: " + rightTierCount);
    LOGGER.debug("Left Tiers: " + leftDataTiers);
    LOGGER.debug("Right Tiers: " + rightDataTiers);

    Map<Byte, HashSet<Byte>> rightReprojectMap = new HashMap<>();
    Map<Byte, HashSet<Byte>> leftReprojectMap = new HashMap<>();
    final HashSet<Byte> sharedTiers = Sets.newHashSetWithExpectedSize(tierCount / 2);
    if (!skipMapCreate) {
      leftReprojectMap = createReprojectMap(leftTierArr, rightTierArr, sharedTiers);
      rightReprojectMap = createReprojectMap(rightTierArr, leftTierArr, sharedTiers);
    }

    JavaRDD<Tuple2<GeoWaveInputKey, Geometry>> commonRightRDD = null;
    final boolean commonRightExist = commonRightTiers != ArrayUtils.EMPTY_BYTE_OBJECT_ARRAY;
    if (commonRightExist) {
      commonRightRDD =
          rightRDD.getGeoWaveRDD().getRawRDD().filter(
              t -> t._2.getDefaultGeometry() != null).mapValues(
                  (Function<SimpleFeature, Geometry>) t -> {
                    return (Geometry) t.getDefaultGeometry();
                  }).distinct(largePartitionerCount).rdd().toJavaRDD();
    }

    JavaRDD<Tuple2<GeoWaveInputKey, Geometry>> commonLeftRDD = null;

    final boolean commonLeftExist = commonLeftTiers != ArrayUtils.EMPTY_BYTE_OBJECT_ARRAY;
    if (commonLeftExist) {
      commonLeftRDD =
          leftRDD.getGeoWaveRDD().getRawRDD().filter(
              t -> t._2.getDefaultGeometry() != null).mapValues(
                  (Function<SimpleFeature, Geometry>) t -> {
                    return (Geometry) t.getDefaultGeometry();
                  }).distinct(largePartitionerCount).rdd().toJavaRDD();
    }

    // Iterate through left tiers. Joining higher right and same level tiers
    for (final Byte leftTierId : leftDataTiers) {
      final HashSet<Byte> higherRightTiers = leftReprojectMap.get(leftTierId);

      JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> leftTier = null;
      final boolean higherTiersExist = ((higherRightTiers != null) && !higherRightTiers.isEmpty());
      final boolean sameTierExist = sharedTiers.contains(leftTierId);

      if (commonRightExist || higherTiersExist || sameTierExist) {
        leftTier = filterTier(leftIndex, leftTierId);

      } else {
        // No tiers to compare against this tier
        continue;
      }

      // Check for same tier existence on both sides and join without reprojection.
      if (sameTierExist) {
        final JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> rightTier =
            rightIndex.filter(t -> t._1().getBytes()[0] == leftTierId);

        final JavaPairRDD<GeoWaveInputKey, ByteArray> finalMatches =
            joinAndCompareTiers(leftTier, rightTier, geomPredicate, highestPartCount, partitioner);
        addMatches(finalMatches);
      }

      // Join against higher common tiers for this dataset
      JavaRDD<Tuple2<GeoWaveInputKey, Geometry>> rightTiers = null;
      if (commonRightExist) {
        rightTiers = commonRightRDD;
      } else if (higherTiersExist) {
        final Broadcast<HashSet<Byte>> higherBroadcast = javaSC.broadcast(higherRightTiers);
        rightTiers =
            prepareForReproject(
                rightIndex.filter(t -> higherBroadcast.value().contains(t._1().getBytes()[0])),
                largePartitionerCount);
      }

      if (rightTiers != null) {
        final JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> reprojected =
            reprojectToTier(
                rightTiers,
                leftTierId,
                broadcastStrategy,
                getBufferAmount(BuildSide.RIGHT),
                partitioner);

        final JavaPairRDD<GeoWaveInputKey, ByteArray> finalMatches =
            joinAndCompareTiers(
                leftTier,
                reprojected,
                geomPredicate,
                highestPartCount,
                partitioner);

        addMatches(finalMatches);
      }
    }

    for (final Byte rightTierId : rightDataTiers) {

      final HashSet<Byte> higherLeftTiers = rightReprojectMap.get(rightTierId);
      JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> rightTier = null;
      final boolean higherLeftExist = ((higherLeftTiers != null) && !higherLeftTiers.isEmpty());
      if (commonLeftExist || higherLeftExist) {
        rightTier = rightIndex.filter(t -> t._1().getBytes()[0] == rightTierId);
      } else {
        // No tiers to compare against this tier
        continue;
      }

      JavaPairRDD<GeoWaveInputKey, ByteArray> finalMatches = null;
      JavaRDD<Tuple2<GeoWaveInputKey, Geometry>> leftTiers = null;
      if (commonLeftExist) {
        leftTiers = commonLeftRDD;
      } else {
        final Broadcast<HashSet<Byte>> higherBroadcast = javaSC.broadcast(higherLeftTiers);
        leftTiers =
            prepareForReproject(
                leftIndex.filter(t -> higherBroadcast.value().contains(t._1.getBytes()[0])),
                largePartitionerCount);
      }

      final JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> reprojected =
          reprojectToTier(
              leftTiers,
              rightTierId,
              broadcastStrategy,
              getBufferAmount(BuildSide.LEFT),
              partitioner);

      finalMatches =
          joinAndCompareTiers(reprojected, rightTier, geomPredicate, highestPartCount, partitioner);

      addMatches(finalMatches);
    }

    // Remove duplicates between tiers
    combinedResults =
        javaSC.union(
            (JavaPairRDD[]) (ArrayUtils.add(
                tierMatches.toArray(new JavaPairRDD[tierMatches.size()]),
                combinedResults)));
    combinedResults = combinedResults.reduceByKey((id1, id2) -> id1);

    combinedResults =
        combinedResults.setName("CombinedJoinResults").persist(StorageLevel.MEMORY_ONLY_SER());
    // Force evaluation of RDD at the join function call.
    // Otherwise it doesn't actually perform work until something is called
    // on left/right joined.
    // Wish there was a better way to force evaluation of rdd safely.
    // isEmpty() triggers take(1) which shouldn't involve a shuffle.
    combinedResults.isEmpty();

    // Join against original dataset to give final joined rdds on each side, and cache results so we
    // don't recalculate
    if (getJoinOptions().isNegativePredicate()) {
      setLeftResults(
          new GeoWaveRDD(
              leftRDD.getGeoWaveRDD().getRawRDD().subtractByKey(combinedResults).cache()));
      setRightResults(
          new GeoWaveRDD(
              rightRDD.getGeoWaveRDD().getRawRDD().subtractByKey(combinedResults).cache()));
    } else {
      setLeftResults(
          new GeoWaveRDD(
              leftRDD.getGeoWaveRDD().getRawRDD().join(combinedResults).mapToPair(
                  t -> new Tuple2<>(t._1(), t._2._1())).cache()));
      setRightResults(
          new GeoWaveRDD(
              rightRDD.getGeoWaveRDD().getRawRDD().join(combinedResults).mapToPair(
                  t -> new Tuple2<>(t._1(), t._2._1())).cache()));
    }

    leftIndex.unpersist();
    rightIndex.unpersist();
  }

  private Map<Byte, HashSet<Byte>> createReprojectMap(
      final Byte[] buildSide,
      final Byte[] testSide,
      final HashSet<Byte> sharedTiers) {
    final Map<Byte, HashSet<Byte>> resultMap = Maps.newHashMap();
    final int testLastIndex = testSide.length;
    for (final Byte tierLeft : buildSide) {
      final int firstGreater = Arrays.binarySearch(testSide, tierLeft);

      if (firstGreater >= 0) {
        // Found in array
        sharedTiers.add(tierLeft);
      }

      final int insertionPoint = Math.abs(firstGreater);
      if (insertionPoint >= testLastIndex) {
        // Not present in array, and none greater than this value
        continue;
      }

      // There is at least one value greater than the current copy it and
      // add to map
      final HashSet<Byte> higherTiers =
          Sets.newHashSet(Arrays.copyOfRange(testSide, insertionPoint, testLastIndex));
      resultMap.put(tierLeft, higherTiers);
    }
    return resultMap;
  }

  private void setBufferAmount(final double bufferAmount) {
    bufferDistance = bufferAmount;
  }

  private double getBufferAmount(final BuildSide testSide) {
    return (joinOpts.getJoinBuildSide() != testSide) ? bufferDistance : 0.0;
  }

  @Override
  public boolean supportsJoin(final NumericIndexStrategy indexStrategy) {
    return (indexStrategy != null)
        && indexStrategy.getClass().isInstance(TieredSFCIndexStrategy.class);
  }

  @Override
  public NumericIndexStrategy createDefaultStrategy(final NumericIndexStrategy indexStrategy) {
    if (SpatialTemporalDimensionalityTypeProvider.isSpatialTemporal(indexStrategy)) {
      final SpatialTemporalOptions options = new SpatialTemporalOptions();
      return TieredSFCIndexFactory.createFullIncrementalTieredStrategy(
          SpatialTemporalDimensionalityTypeProvider.SPATIAL_TEMPORAL_DIMENSIONS,
          new int[] {
              options.getBias().getSpatialPrecision(),
              options.getBias().getSpatialPrecision(),
              options.getBias().getTemporalPrecision()},
          SFCType.HILBERT,
          options.getMaxDuplicates());
    } else if (SpatialDimensionalityTypeProvider.isSpatial(indexStrategy)) {
      return TieredSFCIndexFactory.createFullIncrementalTieredStrategy(
          SpatialDimensionalityTypeProvider.SPATIAL_DIMENSIONS,
          new int[] {
              SpatialDimensionalityTypeProvider.LONGITUDE_BITS,
              SpatialDimensionalityTypeProvider.LATITUDE_BITS},
          SFCType.HILBERT);
    }

    return null;
  }

  private void addMatches(final JavaPairRDD<GeoWaveInputKey, ByteArray> finalMatches) {
    if (combinedResults == null) {
      combinedResults = finalMatches;
    } else {
      tierMatches.add(finalMatches);
    }
  }

  private JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> filterTier(
      final JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> indexedRDD,
      final byte tierId) {
    return indexedRDD.filter(v1 -> v1._1().getBytes()[0] == tierId);
  }

  private JavaRDD<Tuple2<GeoWaveInputKey, Geometry>> prepareForReproject(
      final JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> indexedRDD,
      final int numPartitions) {
    return indexedRDD.values().distinct(numPartitions);
  }

  private JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> reprojectToTier(
      final JavaRDD<Tuple2<GeoWaveInputKey, Geometry>> higherTiers,
      final byte targetTierId,
      final Broadcast<TieredSFCIndexStrategy> broadcastStrategy,
      final double bufferDistance,
      final HashPartitioner partitioner) {
    return higherTiers.flatMapToPair(
        (PairFlatMapFunction<Tuple2<GeoWaveInputKey, Geometry>, ByteArray, Tuple2<GeoWaveInputKey, Geometry>>) t -> {
          final TieredSFCIndexStrategy index = broadcastStrategy.value();
          final SubStrategy[] strategies = index.getSubStrategies();
          SingleTierSubStrategy useStrat = null;
          for (final SubStrategy strat : strategies) {
            final SingleTierSubStrategy tierStrat =
                (SingleTierSubStrategy) strat.getIndexStrategy();
            if (targetTierId == tierStrat.tier) {
              useStrat = tierStrat;
              break;
            }
          }
          final Geometry geom = t._2;
          final Envelope internalEnvelope = geom.getEnvelopeInternal();
          internalEnvelope.expandBy(bufferDistance);
          final MultiDimensionalNumericData boundsRange =
              GeometryUtils.getBoundsFromEnvelope(internalEnvelope);

          InsertionIds insertIds = useStrat.getInsertionIds(boundsRange, 80);

          if (bufferDistance == 0.0) {
            insertIds = RDDUtils.trimIndexIds(insertIds, geom, index);
          }

          final List<Tuple2<ByteArray, Tuple2<GeoWaveInputKey, Geometry>>> reprojected =
              Lists.newArrayListWithCapacity(insertIds.getSize());
          for (final byte[] id : insertIds.getCompositeInsertionIds()) {
            final Tuple2<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> indexPair =
                new Tuple2<>(new ByteArray(id), t);
            reprojected.add(indexPair);
          }
          return reprojected.iterator();
        }).partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK_SER());
  }

  private JavaPairRDD<GeoWaveInputKey, ByteArray> joinAndCompareTiers(
      final JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> leftTier,
      final JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> rightTier,
      final Broadcast<GeomFunction> geomPredicate,
      final int highestPartitionCount,
      final HashPartitioner partitioner) {
    // Cogroup groups on same tier ByteArrayId and pairs them into Iterable
    // sets.
    JavaPairRDD<ByteArray, Tuple2<Iterable<Tuple2<GeoWaveInputKey, Geometry>>, Iterable<Tuple2<GeoWaveInputKey, Geometry>>>> joinedTiers =
        leftTier.cogroup(rightTier, partitioner);

    // Filter only the pairs that have data on both sides, bucket strategy
    // should have been accounted for by this point.
    // We need to go through the pairs and test each feature against each
    // other
    // End with a combined RDD for that tier.
    joinedTiers =
        joinedTiers.filter(t -> t._2._1.iterator().hasNext() && t._2._2.iterator().hasNext());

    final JavaPairRDD<GeoWaveInputKey, ByteArray> finalMatches =
        joinedTiers.flatMapValues(
            (FlatMapFunction<Tuple2<Iterable<Tuple2<GeoWaveInputKey, Geometry>>, Iterable<Tuple2<GeoWaveInputKey, Geometry>>>, GeoWaveInputKey>) t -> {
              final GeomFunction predicate = geomPredicate.value();

              final HashSet<GeoWaveInputKey> results = Sets.newHashSet();
              for (final Tuple2<GeoWaveInputKey, Geometry> leftTuple : t._1) {
                for (final Tuple2<GeoWaveInputKey, Geometry> rightTuple : t._2) {
                  if (predicate.call(leftTuple._2, rightTuple._2)) {
                    results.add(leftTuple._1);
                    results.add(rightTuple._1);
                  }
                }
              }
              return results.iterator();
            }).mapToPair(Tuple2::swap).reduceByKey(partitioner, (id1, id2) -> id1).persist(
                StorageLevel.MEMORY_ONLY_SER());

    return finalMatches;
  }
}
