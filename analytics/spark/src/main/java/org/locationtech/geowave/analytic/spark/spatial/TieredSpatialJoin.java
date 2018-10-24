/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
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
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialTemporalOptions;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.HierarchicalNumericIndexStrategy.SubStrategy;
import org.locationtech.geowave.core.index.sfc.SFCFactory.SFCType;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.sfc.tiered.SingleTierSubStrategy;
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import jersey.repackaged.com.google.common.collect.Maps;
import scala.Tuple2;

public class TieredSpatialJoin extends
		JoinStrategy
{

	private final static Logger LOGGER = LoggerFactory.getLogger(TieredSpatialJoin.class);

	// Combined matching pairs
	private JavaPairRDD<GeoWaveInputKey, ByteArray> combinedResults = null;
	private List<JavaPairRDD<GeoWaveInputKey, ByteArray>> tierMatches = Lists.newArrayList();

	private double bufferDistance = 0.0;

	public TieredSpatialJoin() {}

	@Override
	public void join(
			SparkSession spark,
			GeoWaveIndexedRDD leftRDD,
			GeoWaveIndexedRDD rightRDD,
			GeomFunction predicate)
			throws InterruptedException,
			ExecutionException {
		// Get SparkContext from session
		SparkContext sc = spark.sparkContext();
		JavaSparkContext javaSC = JavaSparkContext.fromSparkContext(sc);
		
		NumericIndexStrategy leftStrategy = leftRDD.getIndexStrategy().getValue();
		NumericIndexStrategy rightStrategy = rightRDD.getIndexStrategy().getValue();

		//Check if either dataset supports the join
		TieredSFCIndexStrategy tieredStrategy = null;
		//Determine if either strategy needs to be reindexed to support join algorithm
		boolean reindexLeft = false;
		boolean reindexRight = false;
		boolean leftSupport = supportsJoin(leftStrategy);
		boolean rightSupport = supportsJoin(rightStrategy);
		if(leftSupport && rightSupport) {
			if(leftStrategy.equals(rightStrategy)){
				//Both strategies match we don't have to reindex
				tieredStrategy = (TieredSFCIndexStrategy) leftStrategy;
			} else {
				//Join build side determines what side we will build strategy off of when strategies support but don't match
				if(this.getJoinOptions().getJoinBuildSide() == JoinOptions.BuildSide.LEFT) {
					reindexRight = true;
					tieredStrategy = (TieredSFCIndexStrategy) leftStrategy;
				} else {
					reindexLeft = true;
					tieredStrategy = (TieredSFCIndexStrategy) rightStrategy;
				}
				
			}
		} else if(leftSupport) {
			reindexRight = true;
			tieredStrategy = (TieredSFCIndexStrategy) leftStrategy;
			
		} else if(rightSupport) {
			reindexLeft = true;
			tieredStrategy = (TieredSFCIndexStrategy) rightStrategy;
			
		} else {
			tieredStrategy = (TieredSFCIndexStrategy) createDefaultStrategy(leftStrategy);
			if(tieredStrategy == null) {
				tieredStrategy = (TieredSFCIndexStrategy) createDefaultStrategy(rightStrategy);
			}
			if(tieredStrategy == null) {
				LOGGER.error("Cannot create default strategy from either provided strategy. Datasets cannot be joined.");
				return;
			}
			reindexLeft = true;
			reindexRight = true;
		}

		// Pull information and broadcast strategy used for join
		SubStrategy[] tierStrategies = tieredStrategy.getSubStrategies();
		int tierCount = tierStrategies.length;
		// Create broadcast variable for indexing strategy
		// Cast is safe because we must be instance of TieredSFCIndexStrategy to support join.
		Broadcast<TieredSFCIndexStrategy> broadcastStrategy = (Broadcast<TieredSFCIndexStrategy>)RDDUtils.broadcastIndexStrategy(
				sc,
				tieredStrategy);
		
		Broadcast<GeomFunction> geomPredicate = javaSC.broadcast(
				predicate);
		
		// If needed reindex one of the strategies we will wrap the buffer operation into the reindex operation
		// Otherwise we buffer based off the buildside of the join.
		this.setBufferAmount(predicate.getBufferAmount());
		
		//Reindex if necessary and get RDD of indexed Geometry
		JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> leftIndex = null;
		JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> rightIndex = null;
		if(reindexLeft && reindexRight) {
			leftRDD.reindex(broadcastStrategy);
			rightRDD.reindex(broadcastStrategy);
		} else if(reindexLeft) {
			leftRDD.reindex(broadcastStrategy);
		} else if(reindexRight) {
			rightRDD.reindex(broadcastStrategy);
		}
		
		if(joinOpts.getJoinBuildSide() == BuildSide.LEFT) {
			rightIndex = rightRDD.getIndexedGeometryRDD(bufferDistance, true);
			leftIndex = leftRDD.getIndexedGeometryRDD();
		} else {
			leftIndex = leftRDD.getIndexedGeometryRDD(bufferDistance, true);
			rightIndex = rightRDD.getIndexedGeometryRDD();
		}
		
		int leftPartCount = leftIndex.getNumPartitions();
		int rightPartCount = rightIndex.getNumPartitions();
		int highestPartCount = (leftPartCount > rightPartCount) ? leftPartCount : rightPartCount;
		int largePartitionerCount = (int) (1.5 * highestPartCount);
		HashPartitioner partitioner = new HashPartitioner(largePartitionerCount);
		
		JavaFutureAction<List<Byte>> leftFuture = leftIndex
				.setName("LeftIndex")
				.keys()
				.map(t -> t.getBytes()[0])
				.distinct(4)
				.collectAsync();
		JavaFutureAction<List<Byte>> rightFuture = rightIndex
				.setName("RightIndex")
				.keys()
				.map(t -> t.getBytes()[0])
				.distinct(4)
				.collectAsync();

		//Get the result of future
		List<Byte> rightDataTiers = Lists.newArrayList(
				rightFuture.get());
		
		//Sort tiers highest to lowest and collect information.
		Byte[] rightTierArr = rightDataTiers.toArray(new Byte[0]);
		Arrays.sort(rightTierArr);
		int rightTierCount = rightTierArr.length;
	
		List<Byte> leftDataTiers = Lists.newArrayList(
				leftFuture.get());
		Byte[] leftTierArr =  leftDataTiers.toArray(new Byte[0]);
		Arrays.sort(leftTierArr);
		int leftTierCount = leftTierArr.length;
		
		//Determine if there are common higher tiers for whole dataset on either side.
		byte highestLeftTier = leftTierArr[leftTierArr.length - 1];
		byte highestRightTier = rightTierArr[rightTierArr.length - 1];
		//Find a common run of higher tiers
		Byte[] commonLeftTiers = ArrayUtils.EMPTY_BYTE_OBJECT_ARRAY;
		Byte[] commonRightTiers = ArrayUtils.EMPTY_BYTE_OBJECT_ARRAY;
		boolean skipMapCreate = false;
		if(leftTierArr[0] > highestRightTier) {
			//Whole left dataset is higher tiers than right
			 commonLeftTiers = leftTierArr;
			 skipMapCreate = true;
		} else if( rightTierArr[0] > highestLeftTier) {
			//Whole right dataset is higher tiers than left
			commonRightTiers = rightTierArr;
			skipMapCreate = true;
		}
		
		LOGGER.debug(
				"Tier Count: " + tierCount);
		LOGGER.debug(
				"Left Tier Count: " + leftTierCount + " Right Tier Count: " + rightTierCount);
		LOGGER.debug(
				"Left Tiers: " + leftDataTiers);
		LOGGER.debug(
				"Right Tiers: " + rightDataTiers);

		Map<Byte, HashSet<Byte>> rightReprojectMap = new HashMap<Byte, HashSet<Byte>>();
		Map<Byte, HashSet<Byte>> leftReprojectMap = new HashMap<Byte, HashSet<Byte>>();
		HashSet<Byte> sharedTiers = Sets.newHashSetWithExpectedSize(tierCount / 2);
		if(!skipMapCreate) {
			leftReprojectMap = this.createReprojectMap(leftTierArr, rightTierArr, sharedTiers);
			rightReprojectMap = this.createReprojectMap(rightTierArr, leftTierArr, sharedTiers);
		}

		JavaRDD<Tuple2<GeoWaveInputKey, Geometry>> commonRightRDD = null;
		boolean commonRightExist = commonRightTiers != ArrayUtils.EMPTY_BYTE_OBJECT_ARRAY;
		if(commonRightExist) {
			commonRightRDD = rightRDD.getGeoWaveRDD().getRawRDD().filter(t -> t._2.getDefaultGeometry() != null).mapValues((Function<SimpleFeature, Geometry>) t -> {				
				return (Geometry) t.getDefaultGeometry();
			}).distinct(largePartitionerCount).rdd().toJavaRDD();
		}

		JavaRDD<Tuple2<GeoWaveInputKey, Geometry>> commonLeftRDD = null;

		boolean commonLeftExist = commonLeftTiers != ArrayUtils.EMPTY_BYTE_OBJECT_ARRAY;
		if(commonLeftExist) {
			commonLeftRDD = leftRDD.getGeoWaveRDD().getRawRDD().filter(t -> t._2.getDefaultGeometry() != null).mapValues((Function<SimpleFeature, Geometry>) t -> {				
				return (Geometry) t.getDefaultGeometry();
			}).distinct(largePartitionerCount).rdd().toJavaRDD();
		}
		
		//Iterate through left tiers. Joining higher right and same level tiers
		for (Byte leftTierId : leftDataTiers) {
			HashSet<Byte> higherRightTiers = leftReprojectMap.get(
					leftTierId);

			JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> leftTier = null;
			boolean higherTiersExist = (higherRightTiers != null && !higherRightTiers.isEmpty());
			boolean sameTierExist = sharedTiers.contains(leftTierId);

			if(commonRightExist || higherTiersExist || sameTierExist ) {
				leftTier = this.filterTier(
						leftIndex,
						leftTierId);
				
			} else {
				//No tiers to compare against this tier
				continue;
			}
			
			//Check for same tier existence on both sides and join without reprojection.
			if (sameTierExist) {
				JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> rightTier = rightIndex.filter(t -> t._1().getBytes()[0] == leftTierId);

				JavaPairRDD<GeoWaveInputKey, ByteArray> finalMatches = this.joinAndCompareTiers(
						leftTier,
						rightTier,
						geomPredicate,
						highestPartCount,
						partitioner);
				this.addMatches(finalMatches);
			}
			
			//Join against higher common tiers for this dataset
			JavaRDD<Tuple2<GeoWaveInputKey, Geometry>> rightTiers = null;
			if(commonRightExist) {
				rightTiers = commonRightRDD;
			}
			else if (higherTiersExist) {
				Broadcast<HashSet<Byte>> higherBroadcast = javaSC.broadcast(higherRightTiers);
				rightTiers = this.prepareForReproject(rightIndex.filter( t -> higherBroadcast.value().contains(t._1().getBytes()[0]) ), largePartitionerCount);
			}
			
			if(rightTiers != null) {
				JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> reprojected = this.reprojectToTier(
						rightTiers,
						leftTierId,
						broadcastStrategy,
						getBufferAmount(BuildSide.RIGHT),
	                    partitioner);
	
				JavaPairRDD<GeoWaveInputKey, ByteArray> finalMatches = this.joinAndCompareTiers(
						leftTier,
						reprojected,
						geomPredicate,
						highestPartCount,
						partitioner);
				
				this.addMatches(
						finalMatches);
			}
			
		}

		
		for (Byte rightTierId : rightDataTiers) {

			HashSet<Byte> higherLeftTiers = rightReprojectMap.get(
					rightTierId);
			JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> rightTier = null;
			boolean higherLeftExist = (higherLeftTiers != null && !higherLeftTiers.isEmpty());
			if(commonLeftExist || higherLeftExist) {
				rightTier = rightIndex.filter( t -> t._1().getBytes()[0] == rightTierId );
			} else {
				//No tiers to compare against this tier
				continue;
			}
			
			JavaPairRDD<GeoWaveInputKey, ByteArray> finalMatches = null;
			JavaRDD<Tuple2<GeoWaveInputKey, Geometry>> leftTiers = null;
			if(commonLeftExist) {
				leftTiers = commonLeftRDD;
			} else {
				Broadcast<HashSet<Byte>> higherBroadcast = javaSC.broadcast(higherLeftTiers);
				leftTiers = this.prepareForReproject(leftIndex.filter( t -> higherBroadcast.value().contains(t._1.getBytes()[0])),largePartitionerCount);
			}
			
			JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> reprojected = this.reprojectToTier(
					leftTiers,
					rightTierId,
					broadcastStrategy,
					getBufferAmount(BuildSide.LEFT),
                    partitioner);
			
			finalMatches = this.joinAndCompareTiers(
					reprojected,
					rightTier,
					geomPredicate,
					highestPartCount,
					partitioner);
			
			this.addMatches(
					finalMatches);
		}
		
		// Remove duplicates between tiers
		this.combinedResults = javaSC.union(this.combinedResults, tierMatches);
		this.combinedResults = this.combinedResults.reduceByKey((id1,id2 ) -> id1);

		this.combinedResults = this.combinedResults.setName("CombinedJoinResults").persist(StorageLevel.MEMORY_ONLY_SER());
		// Force evaluation of RDD at the join function call.
		// Otherwise it doesn't actually perform work until something is called
		// on left/right joined.
		// Wish there was a better way to force evaluation of rdd safely.
		// isEmpty() triggers take(1) which shouldn't involve a shuffle.
		this.combinedResults.isEmpty();

		// Join against original dataset to give final joined rdds on each side, and cache results so we don't recalculate
		if(this.getJoinOptions().isNegativePredicate()) {
			this.setLeftResults(new GeoWaveRDD(leftRDD.getGeoWaveRDD().getRawRDD().subtractByKey(this.combinedResults).cache()));
			this.setRightResults(new GeoWaveRDD(rightRDD.getGeoWaveRDD().getRawRDD().subtractByKey(this.combinedResults).cache()));
		} else {
		this.setLeftResults(
				 new GeoWaveRDD(leftRDD.getGeoWaveRDD().getRawRDD().join(
						 this.combinedResults).mapToPair(
								t -> new Tuple2<GeoWaveInputKey, SimpleFeature>(
										t._1(),
										t._2._1())).cache()));
		this.setRightResults(
				new GeoWaveRDD(rightRDD.getGeoWaveRDD().getRawRDD().join(
						this.combinedResults).mapToPair(
								t -> new Tuple2<GeoWaveInputKey, SimpleFeature>(
										t._1(),
										t._2._1())).cache()));
		}

		leftIndex.unpersist();
		rightIndex.unpersist();
	}

	private Map<Byte, HashSet<Byte>> createReprojectMap(
			Byte[] buildSide,
			Byte[] testSide,
			HashSet<Byte> sharedTiers ) {
		Map<Byte, HashSet<Byte>> resultMap = Maps.newHashMap();
		int testLastIndex = testSide.length;
		for (Byte tierLeft : buildSide) {
			int firstGreater = Arrays.binarySearch(
					testSide,
					tierLeft);

			if (firstGreater >= 0) {
				// Found in array
				sharedTiers.add(tierLeft);
			}

			int insertionPoint = Math.abs(firstGreater);
			if (insertionPoint >= testLastIndex) {
				// Not present in array, and none greater than this value
				continue;
			}

			// There is at least one value greater than the current copy it and
			// add to map
			HashSet<Byte> higherTiers = Sets.newHashSet(Arrays.copyOfRange(
					testSide,
					insertionPoint,
					testLastIndex));
			resultMap.put(
					tierLeft,
					higherTiers);
		}
		return resultMap;
	}

	private void setBufferAmount(
			double bufferAmount ) {
		this.bufferDistance = bufferAmount;
	}

	private double getBufferAmount(
			BuildSide testSide ) {
		return (joinOpts.getJoinBuildSide() != testSide) ? bufferDistance : 0.0;
	}

	public boolean supportsJoin(
			NumericIndexStrategy indexStrategy ) {
		return indexStrategy != null && indexStrategy.getClass().isInstance(
				TieredSFCIndexStrategy.class);

	}

	public NumericIndexStrategy createDefaultStrategy(
			NumericIndexStrategy indexStrategy ) {
		if (SpatialDimensionalityTypeProvider.isSpatial(indexStrategy)) {
			return TieredSFCIndexFactory.createFullIncrementalTieredStrategy(
					SpatialDimensionalityTypeProvider.SPATIAL_DIMENSIONS,
					new int[] {
						SpatialDimensionalityTypeProvider.LONGITUDE_BITS,
						SpatialDimensionalityTypeProvider.LATITUDE_BITS
					},
					SFCType.HILBERT);
		}
		else if (SpatialTemporalDimensionalityTypeProvider.isSpatialTemporal(indexStrategy)) {
			SpatialTemporalOptions options = new SpatialTemporalOptions();
			return TieredSFCIndexFactory.createFullIncrementalTieredStrategy(
					SpatialTemporalDimensionalityTypeProvider.SPATIAL_TEMPORAL_DIMENSIONS,
					new int[] {
						options.getBias().getSpatialPrecision(),
						options.getBias().getSpatialPrecision(),
						options.getBias().getTemporalPrecision()
					},
					SFCType.HILBERT,
					options.getMaxDuplicates());
		}

		return null;
	}

	private void addMatches(
			JavaPairRDD<GeoWaveInputKey, ByteArray> finalMatches ) {
		if (combinedResults == null) {
			combinedResults = finalMatches;
		}
		else {
			tierMatches.add(finalMatches);
		}
	}

	private JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> filterTier(
			JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> indexedRDD,
			byte tierId ) {
		return indexedRDD.filter(
				v1 -> v1._1().getBytes()[0] == tierId);
	}

	private JavaRDD<Tuple2<GeoWaveInputKey, Geometry>> prepareForReproject(
			JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> indexedRDD,
			int numPartitions ) {
		return indexedRDD.values().distinct(
				numPartitions);
	}

	private JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> reprojectToTier(
			JavaRDD<Tuple2<GeoWaveInputKey, Geometry>> higherTiers,
			byte targetTierId,
			Broadcast<TieredSFCIndexStrategy> broadcastStrategy,
			double bufferDistance,
			HashPartitioner partitioner ) {
		return higherTiers
				.flatMapToPair(
						(PairFlatMapFunction<Tuple2<GeoWaveInputKey, Geometry>, ByteArray, Tuple2<GeoWaveInputKey, Geometry>>) t -> {

                            TieredSFCIndexStrategy index = broadcastStrategy.value();
                            SubStrategy[] strategies = index.getSubStrategies();
                            SingleTierSubStrategy useStrat = null;
                            for (SubStrategy strat : strategies) {
                                SingleTierSubStrategy tierStrat = (SingleTierSubStrategy) strat.getIndexStrategy();
                                if (targetTierId == tierStrat.tier) {
                                    useStrat = tierStrat;
                                    break;
                                }
                            }
                            Geometry geom = t._2;
                            Envelope internalEnvelope = geom.getEnvelopeInternal();
                            internalEnvelope.expandBy(bufferDistance);
                            MultiDimensionalNumericData boundsRange = GeometryUtils
                                    .getBoundsFromEnvelope(internalEnvelope);

                            InsertionIds insertIds = useStrat.getInsertionIds(
                                    boundsRange,
                                    80);

                            if (bufferDistance == 0.0) {
                                insertIds = RDDUtils.trimIndexIds(
                                        insertIds,
                                        geom,
                                        index);
                            }

                            List<Tuple2<ByteArray, Tuple2<GeoWaveInputKey, Geometry>>> reprojected = Lists
                                    .newArrayListWithCapacity(insertIds.getSize());
							for (ByteArray id : insertIds.getCompositeInsertionIds()) {
								Tuple2<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> indexPair = new Tuple2<ByteArray, Tuple2<GeoWaveInputKey, Geometry>>(
										id,
										t);
								reprojected.add(indexPair);
							}
                            return reprojected.iterator();
                        }).partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK_SER());
	}

	private JavaPairRDD<GeoWaveInputKey, ByteArray> joinAndCompareTiers(
			JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> leftTier,
			JavaPairRDD<ByteArray, Tuple2<GeoWaveInputKey, Geometry>> rightTier,
			Broadcast<GeomFunction> geomPredicate,
			int highestPartitionCount, 
			HashPartitioner partitioner ) {
		// Cogroup groups on same tier ByteArrayId and pairs them into Iterable
		// sets.
		JavaPairRDD<ByteArray, Tuple2<Iterable<Tuple2<GeoWaveInputKey, Geometry>>, Iterable<Tuple2<GeoWaveInputKey, Geometry>>>> joinedTiers = leftTier
				.cogroup(
						rightTier,
						partitioner);
		
		
		// Filter only the pairs that have data on both sides, bucket strategy
		// should have been accounted for by this point.
		// We need to go through the pairs and test each feature against each
		// other
		// End with a combined RDD for that tier.
		joinedTiers = joinedTiers.filter(
				t -> t._2._1.iterator().hasNext() && t._2._2.iterator().hasNext());

		
		JavaPairRDD<GeoWaveInputKey, ByteArray> finalMatches = joinedTiers.flatMapValues((Function<Tuple2<Iterable<Tuple2<GeoWaveInputKey, Geometry>>, Iterable<Tuple2<GeoWaveInputKey, Geometry>>>, Iterable<GeoWaveInputKey>>) t -> {
            GeomFunction predicate = geomPredicate.value();

            HashSet<GeoWaveInputKey> results = Sets.newHashSet();
            for (Tuple2<GeoWaveInputKey, Geometry> leftTuple : t._1) {
                for (Tuple2<GeoWaveInputKey, Geometry> rightTuple : t._2) {
                    if (predicate.call(
                            leftTuple._2,
                            rightTuple._2)) {
                        results.add(leftTuple._1);
                        results.add(rightTuple._1);
                    }
                }
            }
            return results;
        }).mapToPair(Tuple2::swap).reduceByKey(partitioner,(id1, id2) -> id1).persist(StorageLevel.MEMORY_ONLY_SER());
		
		return finalMatches;
	}
}
