/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.analytic.spark.spatial;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import breeze.util.Iterators;
import mil.nga.giat.geowave.analytic.spark.GeoWaveIndexedRDD;
import mil.nga.giat.geowave.analytic.spark.GeoWaveRDD;
import mil.nga.giat.geowave.analytic.spark.RDDUtils;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomFunction;
import mil.nga.giat.geowave.analytic.spark.spatial.JoinOptions.BuildSide;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy.SubStrategy;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.index.sfc.tiered.SingleTierSubStrategy;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import scala.Tuple2;
import scala.reflect.ClassTag;

public class TieredSpatialJoin extends
		JoinStrategy
{

	private final static Logger LOGGER = LoggerFactory.getLogger(TieredSpatialJoin.class);

	// Combined matching pairs
	private JavaPairRDD<GeoWaveInputKey, Geometry> combinedResults = null;

	private List<Byte> leftDataTiers = new ArrayList<>();
	private List<Byte> rightDataTiers = new ArrayList<>();

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
		
		Broadcast<NumericIndexStrategy> broadcastLeft = leftRDD.getIndexStrategy();
		Broadcast<NumericIndexStrategy> broadcastRight = rightRDD.getIndexStrategy();
		NumericIndexStrategy leftStrategy = broadcastLeft.getValue();
		NumericIndexStrategy rightStrategy = broadcastRight.getValue();

		//Check if either dataset supports the join
		TieredSFCIndexStrategy tieredStrategy = null;
		boolean reindexLeft = false;
		boolean reindexRight = false;
		if(supportsJoin(leftStrategy) && supportsJoin(rightStrategy)) {
			if(leftStrategy.equals(rightStrategy)) {
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
			
		} else if(supportsJoin(leftStrategy)) {
			reindexRight = true;
			tieredStrategy = (TieredSFCIndexStrategy) leftStrategy;
			
		} else if(supportsJoin(rightStrategy)) {
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

		SubStrategy[] tierStrategies = tieredStrategy.getSubStrategies();
		int tierCount = tierStrategies.length;
		
		ClassTag<SingleTierSubStrategy> singleTierClassTag = scala.reflect.ClassTag$.MODULE$.apply(
				SingleTierSubStrategy.class);

		ClassTag<GeomFunction> geomFuncClassTag = scala.reflect.ClassTag$.MODULE$.apply(
				predicate.getClass());
		// Create broadcast variable for indexing strategy
		// Cast is safe because we must be instance of TieredSFCIndexStrategy to support join.
		Broadcast<TieredSFCIndexStrategy> broadcastStrategy = (Broadcast<TieredSFCIndexStrategy>)RDDUtils.broadcastIndexStrategy(
				sc,
				tieredStrategy);
		
		Broadcast<GeomFunction> geomPredicate = sc.broadcast(
				predicate,
				geomFuncClassTag);
		// If needed reindex one of the strategies we will wrap the buffer operation into the reindex operation
		// Otherwise we buffer based off the buildside of the join.
		double bufferDistance = predicate.getBufferAmount();
		JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> leftIndex = null;
		JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> rightIndex = null;
		if(reindexLeft && reindexRight) {
			leftRDD.reindex(broadcastStrategy);
			rightRDD.reindex(broadcastStrategy);
			if(joinOpts.getJoinBuildSide() == BuildSide.LEFT) {
				rightIndex = rightRDD.getIndexedGeometryRDD(bufferDistance);
				leftIndex = leftRDD.getIndexedGeometryRDD();
			} else {
				leftIndex = leftRDD.getIndexedGeometryRDD(bufferDistance);
				rightIndex = rightRDD.getIndexedGeometryRDD();
			}
		} else if(reindexLeft) {
			leftRDD.reindex(broadcastStrategy);
			leftIndex = leftRDD.getIndexedGeometryRDD(bufferDistance);
			rightIndex = rightRDD.getIndexedGeometryRDD();
		} else if(reindexRight) {
			rightRDD.reindex(broadcastStrategy);
			rightIndex = rightRDD.getIndexedGeometryRDD(bufferDistance);
			leftIndex = leftRDD.getIndexedGeometryRDD();
		} else {
			leftIndex = leftRDD.getIndexedGeometryRDD();
			rightIndex = rightRDD.getIndexedGeometryRDD();
			if(joinOpts.getJoinBuildSide() == BuildSide.LEFT) {
				rightIndex = this.bufferData(rightIndex, broadcastStrategy, bufferDistance);
			} else {
				leftIndex = this.bufferData(leftIndex, broadcastStrategy, bufferDistance);
			}
		}
		
		leftIndex.cache();
		rightIndex.cache();
		JavaFutureAction<List<Byte>> leftFuture = leftIndex
				.keys()
				.map(
						id -> id.getBytes()[0])
				.distinct(
						1)
				.collectAsync();
		JavaFutureAction<List<Byte>> rightFuture = rightIndex
				.keys()
				.map(
						id -> id.getBytes()[0])
				.distinct(
						1)
				.collectAsync();

		rightDataTiers = Lists.newArrayList(
				rightFuture.get());
		Collections.sort(
				rightDataTiers,
				Collections.reverseOrder());
		leftDataTiers = Lists.newArrayList(
				leftFuture.get());
		Collections.sort(
				leftDataTiers,
				Collections.reverseOrder());

		int leftTierCount = leftDataTiers.size();
		int rightTierCount = rightDataTiers.size();

		LOGGER.debug(
				"Tier Count: " + tierCount);
		LOGGER.debug(
				"Left Tier Count: " + leftTierCount + " Right Tier Count: " + rightTierCount);
		LOGGER.debug(
				"Left Tiers: " + leftDataTiers);
		LOGGER.debug(
				"Right Tiers: " + rightDataTiers);

		Map<Byte, HashSet<Byte>> rightReprojectMap = new HashMap<Byte, HashSet<Byte>>();
		Map<Byte, ArrayList<Byte>> leftReprojectMap = new HashMap<Byte, ArrayList<Byte>>();
		for (Byte tierLeft : leftDataTiers) {
			ArrayList<Byte> higherTiers = new ArrayList<Byte>();
			for (Byte tierRight : rightDataTiers) {
				if (tierRight > tierLeft) {
					higherTiers.add(
							tierRight);
				}
				else if (tierRight < tierLeft) {
					HashSet<Byte> set = rightReprojectMap.get(
							tierRight);
					if (set == null) {
						set = new HashSet<Byte>();
						rightReprojectMap.put(
								tierRight,
								set);
					}
					set.add(
							tierLeft);
				}
			}

			if (!higherTiers.isEmpty()) {
				leftReprojectMap.put(
						tierLeft,
						higherTiers);
			}
		}

		int leftPartCount = leftIndex.getNumPartitions();
		int rightPartCount = rightIndex.getNumPartitions();
		int highestPartCount = (leftPartCount > rightPartCount) ? leftPartCount : rightPartCount;
		for (Byte leftTierId : leftDataTiers) {
			ArrayList<Byte> higherRightTiers = leftReprojectMap.get(
					leftTierId);

			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> leftTier = null;
			JavaPairRDD<GeoWaveInputKey, Geometry> finalMatches = null;

			if (higherRightTiers != null) {
				if (leftTier == null) {
					leftTier = this.filterTier(
							leftIndex,
							leftTierId);
				}
				//Filter and broadcast tier sub-strategy
				SubStrategy targetStrategy = tierStrategies[leftTierId];
				SingleTierSubStrategy tierStrategy = (SingleTierSubStrategy) targetStrategy.getIndexStrategy();
				if(tierStrategy.tier != leftTierId) {
					LOGGER.warn("My assumption was wrong! Expected Tier= " + leftTierId + " current tier= " + tierStrategy.tier);
				}
				Broadcast<SingleTierSubStrategy> broadcastedTier = sc.broadcast(tierStrategy, singleTierClassTag);
				
				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> rightTiers = this.filterTiers(
						rightIndex,
						higherRightTiers);
				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> reproject = this.reprojectToTier(
						rightTiers,
						leftTierId,
						broadcastedTier,
                        bufferDistance);
				finalMatches = this.joinAndCompareTiers(
						leftTier,
						reproject,
						geomPredicate,
						highestPartCount);
				this.addMatches(
						finalMatches);
				
				broadcastedTier.unpersist();
			}

			if (rightDataTiers.contains(
					leftTierId)) {
				if (leftTier == null) {
					leftTier = this.filterTier(
							leftIndex,
							leftTierId);
					leftTier.cache();
				}

				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> rightTier = this.filterTier(
						rightIndex,
						leftTierId);
				finalMatches = this.joinAndCompareTiers(
						leftTier,
						rightTier,
						geomPredicate,
						highestPartCount);
				this.addMatches(
						finalMatches);

			}
		}

		for (Byte rightTierId : rightDataTiers) {

			HashSet<Byte> higherLeftTiers = rightReprojectMap.get(
					rightTierId);

			if (higherLeftTiers != null) {
				
				//Filter and broadcast tier sub-strategy
				SubStrategy targetStrategy = tierStrategies[rightTierId];
				SingleTierSubStrategy tierStrategy = (SingleTierSubStrategy) targetStrategy.getIndexStrategy();
				if(tierStrategy.tier != rightTierId) {
					LOGGER.warn("My assumption was wrong! Expected Tier= " + rightTierId + " current tier= " + tierStrategy.tier);
				}
				
				Broadcast<SingleTierSubStrategy> broadcastedTier = sc.broadcast(tierStrategy, singleTierClassTag);

				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> rightTier = this.filterTier(
						rightIndex,
						rightTierId);

				ArrayList<Byte> leftTierIds = Lists.newArrayList(
						higherLeftTiers);
				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> leftTiers = this.filterTiers(
						leftIndex,
						leftTierIds);

				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> reproject = this.reprojectToTier(
						leftTiers,
						rightTierId,
						broadcastedTier,
                        bufferDistance);
				JavaPairRDD<GeoWaveInputKey, Geometry> finalMatches = this.joinAndCompareTiers(
						reproject,
						rightTier,
						geomPredicate,
						highestPartCount);
				this.addMatches(
						finalMatches);

				broadcastedTier.unpersist();
			}

		}
		// Remove duplicates between tiers
		this.combinedResults = this.combinedResults.reduceByKey(
				(
						id1,
						id2 ) -> id1);

		this.combinedResults.cache();
		// Force evaluation of RDD at the join function call.
		// Otherwise it doesn't actually perform work until something is called
		// on left/right joined.
		// Wish there was a better way to force evaluation of rdd safely.
		// isEmpty() triggers take(1) which shouldn't involve a shuffle like
		// count()
		this.combinedResults.isEmpty();

		// Join against original dataset to give final joined rdds on each side
		if(this.getJoinOptions().isNegativePredicate()) {
			this.setLeftResults(new GeoWaveRDD(leftRDD.getGeoWaveRDD().getRawRDD().subtractByKey(this.combinedResults)));
			this.setRightResults(new GeoWaveRDD(rightRDD.getGeoWaveRDD().getRawRDD().subtractByKey(this.combinedResults)));
		} else {
		this.setLeftResults(
				 new GeoWaveRDD(leftRDD.getGeoWaveRDD().getRawRDD().join(
						this.combinedResults).mapToPair(
								t -> new Tuple2<GeoWaveInputKey, SimpleFeature>(
										t._1(),
										t._2._1()))));
		this.setRightResults(
				new GeoWaveRDD(rightRDD.getGeoWaveRDD().getRawRDD().join(
						this.combinedResults).mapToPair(
								t -> new Tuple2<GeoWaveInputKey, SimpleFeature>(
										t._1(),
										t._2._1()))));
		}

		// Finally mark the final joined set on each side as cached so it
		// doesn't recalculate work.
		leftJoined.getRawRDD().cache();
		rightJoined.getRawRDD().cache();
	}

	public boolean supportsJoin(
			NumericIndexStrategy indexStrategy ) {
		if (indexStrategy != null && indexStrategy.getClass().isInstance(
				TieredSFCIndexStrategy.class)) {
			return true;
		}

		return false;
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
			JavaPairRDD<GeoWaveInputKey, Geometry> finalMatches ) {
		if (this.combinedResults == null) {
			this.combinedResults = finalMatches;
		}
		else {
			this.combinedResults = this.combinedResults.union(finalMatches);
		}
	}

	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> bufferData(
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> data,
			Broadcast<TieredSFCIndexStrategy> broadcastStrategy,
			double bufferDistance ) {
		JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> indexedData = data
				.flatMapToPair(new PairFlatMapFunction<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>, ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>() {
					@Override
					public Iterator<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>> call(
							Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> t )
							throws Exception {

						// Flattened output array.
						List<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>> result = new ArrayList<>();

						Geometry geom = t._2._2;
						if (geom == null) {
							return result.iterator();
						}

						Envelope internalEnvelope = geom.getEnvelopeInternal();
						NumericRange xRange = new NumericRange(
								internalEnvelope.getMinX() - bufferDistance,
								internalEnvelope.getMaxX() + bufferDistance);
						NumericRange yRange = new NumericRange(
								internalEnvelope.getMinY() - bufferDistance,
								internalEnvelope.getMaxY() + bufferDistance);

						NumericData[] boundsRange = {
							xRange,
							yRange
						};

						// Convert the data to how the api expects and index
						// using strategy above
						BasicNumericDataset convertedBounds = new BasicNumericDataset(
								boundsRange);
						InsertionIds insertIds = broadcastStrategy.value().getInsertionIds(
								convertedBounds);

						// Sometimes the result can span more than one row/cell
						// of a tier
						// When we span more than one row each individual get
						// added as a separate output pair
						// TODO should this use composite IDs or just the sort
						// keys
						for (Iterator<ByteArrayId> iter = insertIds.getCompositeInsertionIds().iterator(); iter
								.hasNext();) {
							ByteArrayId id = iter.next();
							// Id decomposes to byte array of Tier, Bin, SFC
							// (Hilbert in this case) id)
							// There may be value in decomposing the id and
							// storing tier + sfcIndex as a tuple key of the new
							// RDD
							Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> indexPair = new Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>(
									id,
									t._2);
							result.add(indexPair);
						}

						return result.iterator();
					}

				});
		return indexedData;
	}

	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> filterTier(
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> indexedData,
			byte tierId ) {
		return indexedData.filter(
				v1 -> v1._1().getBytes()[0] == tierId);
	}

	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> filterTiers(
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> indexedData,
			List<Byte> tierIds ) {
		return indexedData.filter(
				v1 -> tierIds.contains(
						v1._1().getBytes()[0]));
	}

	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> reprojectToTier(
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> higherRightTiers,
			byte targetTierId,
			Broadcast<SingleTierSubStrategy> broadcastedTier,
			double bufferDistance ) {
		return higherRightTiers
				.flatMapToPair(new PairFlatMapFunction<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>, ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>() {

					@Override
					public Iterator<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>> call(
							Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> t )
							throws Exception {

						SingleTierSubStrategy targetStrategy = broadcastedTier.value();

						Geometry geom = t._2._2;

						NumericRange xRange = new NumericRange(
								geom.getEnvelopeInternal().getMinX() - bufferDistance,
								geom.getEnvelopeInternal().getMaxX() + bufferDistance);
						NumericRange yRange = new NumericRange(
								geom.getEnvelopeInternal().getMinY() - bufferDistance,
								geom.getEnvelopeInternal().getMaxY() + bufferDistance);
						NumericData[] boundsRange = {
							xRange,
							yRange
						};
						// Convert the data to how the api expects and index
						// using strategy above
						BasicNumericDataset convertedBounds = new BasicNumericDataset(
								boundsRange);
						InsertionIds insertIds = targetStrategy.getInsertionIds(convertedBounds);

						// When we span more than one row each individual
						// get added as a separate output pair
						// TODO should this use composite IDs or just the
						// sort keys
						if (insertIds.isEmpty()) {
							return Collections.emptyIterator();
						}
						List<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>> reprojected = Lists
								.newArrayListWithCapacity(insertIds.getSize());
						for (Iterator<ByteArrayId> iter = insertIds.getCompositeInsertionIds().iterator(); iter
								.hasNext();) {
							ByteArrayId id = iter.next();
							// Id decomposes to byte array of Tier, Bin, SFC
							// (Hilbert in this case) id)
							// There may be value in decomposing the id and
							// storing tier + sfcIndex as a tuple key of new
							// RDD
							Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> indexPair = new Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>(
									id,
									t._2);
							reprojected.add(indexPair);
						}
						return reprojected.iterator();
					}

				});
	}

	private JavaPairRDD<GeoWaveInputKey, Geometry> joinAndCompareTiers(
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> leftTier,
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> rightTier,
			Broadcast<GeomFunction> geomPredicate,
			int highestPartitionCount ) {
		// Cogroup groups on same tier ByteArrayId and pairs them into Iterable
		// sets.
		JavaPairRDD<ByteArrayId, Tuple2<Iterable<Tuple2<GeoWaveInputKey, Geometry>>, Iterable<Tuple2<GeoWaveInputKey, Geometry>>>> joinedTiers = leftTier
				.cogroup(
						rightTier,
						highestPartitionCount);
		// Filter only the pairs that have data on both sides, bucket strategy
		// should have been accounted for by this point.
		// We need to go through the pairs and test each feature against each
		// other
		// End with a combined RDD for that tier.
		joinedTiers = joinedTiers.filter(
				t -> t._2._1.iterator().hasNext() && t._2._2.iterator().hasNext());
		JavaPairRDD<GeoWaveInputKey, Geometry> finalTierMatches = joinedTiers.flatMapToPair(
				new PairFlatMapFunction<Tuple2<ByteArrayId, Tuple2<Iterable<Tuple2<GeoWaveInputKey, Geometry>>, Iterable<Tuple2<GeoWaveInputKey, Geometry>>>>, GeoWaveInputKey, Geometry>() {

					@Override
					public Iterator<Tuple2<GeoWaveInputKey, Geometry>> call(
							Tuple2<ByteArrayId, Tuple2<Iterable<Tuple2<GeoWaveInputKey, Geometry>>, Iterable<Tuple2<GeoWaveInputKey, Geometry>>>> t )
							throws Exception {

						GeomFunction predicate = geomPredicate.value();

						ArrayList<Tuple2<GeoWaveInputKey, Geometry>> resultSet = Lists.newArrayList();
						for (Tuple2<GeoWaveInputKey, Geometry> leftTuple : t._2._1) {
							for (Tuple2<GeoWaveInputKey, Geometry> rightTuple : t._2._2) {
								if (predicate.call(
										leftTuple._2,
										rightTuple._2)) {
									resultSet.add(
											leftTuple);
									resultSet.add(
											rightTuple);
								}
							}
						}
						return resultSet.iterator();
					}
				});

		finalTierMatches = finalTierMatches.reduceByKey(
				(
						id1,
						id2 ) -> id1,
				highestPartitionCount);
		finalTierMatches.cache();

		return finalTierMatches;
	}
}
