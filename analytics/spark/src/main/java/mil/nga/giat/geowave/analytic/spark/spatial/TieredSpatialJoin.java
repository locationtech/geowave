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

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.geometry.BoundingBox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomFunction;
import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy.SubStrategy;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.index.sfc.tiered.SingleTierSubStrategy;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.xz.XZHierarchicalIndexStrategy;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import scala.Tuple2;
import scala.reflect.ClassTag;

public class TieredSpatialJoin extends
		JoinStrategy
{

	private final static Logger LOGGER = LoggerFactory.getLogger(
			TieredSpatialJoin.class);

	// Combined matching pairs
	private JavaPairRDD<GeoWaveInputKey, Geometry> combinedResults = null;

	private List<Byte> leftDataTiers = new ArrayList<>();
	private List<Byte> rightDataTiers = new ArrayList<>();

	public TieredSpatialJoin() {}

	@Override
	public void join(
			SparkSession spark,
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> leftRDD,
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> rightRDD,
			GeomFunction predicate,
			NumericIndexStrategy indexStrategy )
			throws InterruptedException,
			ExecutionException {
		// Get SparkContext from session
		SparkContext sc = spark.sparkContext();

		// TODO: Revisit how strategy is applied to join. I don't like how the
		// default strategy is created
		// as it doesn't consider SpatialTemporal (not handled currently anyway)
		// and default strategy shouldn't be created at this point
		// it should be abstracted out to store level (SpatialJoinRunner), but
		// since this is currently the only interface available
		// this solution maintains join working across strategies without
		// creating too much throwaway code
		// for something that will be handled naturally as the interface is
		// fleshed out further.
		HierarchicalNumericIndexStrategy tieredStrategy = null;
		if (indexStrategy == null || !indexStrategy.getClass().isInstance(
				HierarchicalNumericIndexStrategy.class)) {
			LOGGER.warn(
					"Hierarchial index strategy must be provided for tiered join. Using default tiered index strategy");
			tieredStrategy = this.createDefaultStrategy();
		}
		else if (indexStrategy.getClass().isInstance(
				XZHierarchicalIndexStrategy.class)) {
			LOGGER.warn(
					"XZOrder index strategy not compatible with tiered join. Using default tiered index strategy");
			tieredStrategy = this.createDefaultStrategy();
		}
		else {
			tieredStrategy = (HierarchicalNumericIndexStrategy) indexStrategy;
		}

		SubStrategy[] tierStrategies = tieredStrategy.getSubStrategies();
		int tierCount = tierStrategies.length;
		ClassTag<HierarchicalNumericIndexStrategy> tieredClassTag = scala.reflect.ClassTag$.MODULE$.apply(
				tieredStrategy.getClass());

		ClassTag<GeomFunction> geomFuncClassTag = scala.reflect.ClassTag$.MODULE$.apply(
				predicate.getClass());
		// Create broadcast variable for indexing strategy
		Broadcast<HierarchicalNumericIndexStrategy> broadcastStrategy = sc.broadcast(
				tieredStrategy,
				tieredClassTag);
		Broadcast<GeomFunction> geomPredicate = sc.broadcast(
				predicate,
				geomFuncClassTag);

		// TODO: Buffering of geometry and indexing should be moved out to steps
		// before actual join logic.
		// Technically this is worst case scenario where we reindex and buffer
		// both sets of data.
		double bufferDistance = predicate.getBufferAmount();

		// Generate Index RDDs for each set of data.
		JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> leftIndex = this.indexData(
				leftRDD,
				broadcastStrategy,
				bufferDistance);
		leftIndex.cache();
		JavaFutureAction<List<Byte>> leftFuture = leftIndex
				.keys()
				.map(
						id -> id.getBytes()[0])
				.distinct(
						1)
				.collectAsync();

		JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> rightIndex = this.indexData(
				rightRDD,
				broadcastStrategy,
				bufferDistance);
		rightIndex.cache();
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

		LOGGER.warn(
				"Tier Count: " + tierCount);
		LOGGER.warn(
				"Left Tier Count: " + leftTierCount + " Right Tier Count: " + rightTierCount);
		LOGGER.warn(
				"Left Tiers: " + leftDataTiers);
		LOGGER.warn(
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
				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> rightTiers = this.filterTiers(
						rightIndex,
						higherRightTiers);
				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> reproject = this.reprojectToTier(
						rightTiers,
						leftTierId,
						broadcastStrategy);
				finalMatches = this.joinAndCompareTiers(
						leftTier,
						reproject,
						geomPredicate,
						highestPartCount);
				this.addMatches(
						finalMatches);
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
						broadcastStrategy);
				JavaPairRDD<GeoWaveInputKey, Geometry> finalMatches = this.joinAndCompareTiers(
						reproject,
						rightTier,
						geomPredicate,
						highestPartCount);
				this.addMatches(
						finalMatches);

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
		this.setLeftResults(
				leftRDD.join(
						this.combinedResults).mapToPair(
								t -> new Tuple2<GeoWaveInputKey, SimpleFeature>(
										t._1(),
										t._2._1())));
		this.setRightResults(
				rightRDD.join(
						this.combinedResults).mapToPair(
								t -> new Tuple2<GeoWaveInputKey, SimpleFeature>(
										t._1(),
										t._2._1())));

		// Finally mark the final joined set on each side as cached so it
		// doesn't recalculate work.
		leftJoined.cache();
		rightJoined.cache();
	}

	// TODO: This doesn't consider SpatialTemporal
	private TieredSFCIndexStrategy createDefaultStrategy() {
		final NumericDimensionDefinition[] SPATIAL_DIMENSIONS = new NumericDimensionDefinition[] {
			new LongitudeDefinition(),
			new LatitudeDefinition(
					true)
			// just use the same range for latitude to make square sfc values in
			// decimal degrees (EPSG:4326)
		};
		final int LONGITUDE_BITS = 31;
		final int LATITUDE_BITS = 31;
		return TieredSFCIndexFactory.createFullIncrementalTieredStrategy(
				SPATIAL_DIMENSIONS,
				new int[] {
					// TODO this is only valid for 2D coordinate
					// systems, again consider the possibility of being
					// flexible enough to handle n-dimensions
					LONGITUDE_BITS,
					LATITUDE_BITS
				},
				SFCType.HILBERT);
	}

	private void addMatches(
			JavaPairRDD<GeoWaveInputKey, Geometry> finalMatches ) {
		if (this.combinedResults == null) {
			this.combinedResults = finalMatches;
		}
		else {
			this.combinedResults = this.combinedResults.union(
					finalMatches);
		}
	}

	// TODO: Move to separate class/function that can be applied per rdd.
	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> indexData(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> data,
			Broadcast<HierarchicalNumericIndexStrategy> broadcastStrategy,
			double bufferDistance ) {
		// Flat map is used because each pair can potentially yield 1+ output
		// rows within rdd.
		// Instead of storing whole feature on index maybe just output Key +
		// Bounds
		JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> indexedData = data.flatMapToPair(
				new PairFlatMapFunction<Tuple2<GeoWaveInputKey, SimpleFeature>, ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>() {
					@Override
					public Iterator<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>> call(
							Tuple2<GeoWaveInputKey, SimpleFeature> t )
							throws Exception {

						// Flattened output array.
						List<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>> result = new ArrayList<>();

						// Pull feature to index from tuple
						SimpleFeature inputFeature = t._2;

						Geometry geom = (Geometry) inputFeature.getDefaultGeometry();
						if (geom == null) {
							return result.iterator();
						}
						// Extract bounding box from input feature
						BoundingBox bounds = inputFeature.getBounds();
						NumericRange xRange = new NumericRange(
								bounds.getMinX() - bufferDistance,
								bounds.getMaxX() + bufferDistance);
						NumericRange yRange = new NumericRange(
								bounds.getMinY() - bufferDistance,
								bounds.getMaxY() + bufferDistance);

						if (bounds.isEmpty()) {
							Envelope internalEnvelope = geom.getEnvelopeInternal();
							xRange = new NumericRange(
									internalEnvelope.getMinX() - bufferDistance,
									internalEnvelope.getMaxX() + bufferDistance);
							yRange = new NumericRange(
									internalEnvelope.getMinY() - bufferDistance,
									internalEnvelope.getMaxY() + bufferDistance);

						}

						NumericData[] boundsRange = {
							xRange,
							yRange
						};

						// Convert the data to how the api expects and index
						// using strategy above
						BasicNumericDataset convertedBounds = new BasicNumericDataset(
								boundsRange);
						List<ByteArrayId> insertIds = broadcastStrategy.value().getInsertionIds(
								convertedBounds);

						// Sometimes the result can span more than one row/cell
						// of a tier
						// When we span more than one row each individual get
						// added as a separate output pair
						for (Iterator<ByteArrayId> iter = insertIds.iterator(); iter.hasNext();) {
							ByteArrayId id = iter.next();
							// Id decomposes to byte array of Tier, Bin, SFC
							// (Hilbert in this case) id)
							// There may be value in decomposing the id and
							// storing tier + sfcIndex as a tuple key of the new
							// RDD
							Tuple2<GeoWaveInputKey, Geometry> valuePair = new Tuple2<>(
									t._1,
									geom);
							Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> indexPair = new Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>(
									id,
									valuePair);
							result.add(
									indexPair);
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
			Broadcast<HierarchicalNumericIndexStrategy> broadcastStrategy ) {
		return higherRightTiers.flatMapToPair(
				new PairFlatMapFunction<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>, ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>() {

					@Override
					public Iterator<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>> call(
							Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> t )
							throws Exception {

						List<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>> reprojected = new ArrayList<>();

						SubStrategy[] strats = broadcastStrategy.value().getSubStrategies();

						int stratCount = strats.length;
						SingleTierSubStrategy targetStrategy = null;
						for (int i = 0; i < stratCount; i++) {
							SingleTierSubStrategy tierStrategy = (SingleTierSubStrategy) strats[i].getIndexStrategy();
							if (tierStrategy != null && tierStrategy.tier == targetTierId) {
								targetStrategy = tierStrategy;
								break;
							}
						}
						if (targetStrategy != null) {
							Geometry geom = t._2._2;

							NumericRange xRange = new NumericRange(
									geom.getEnvelopeInternal().getMinX(),
									geom.getEnvelopeInternal().getMaxX());
							NumericRange yRange = new NumericRange(
									geom.getEnvelopeInternal().getMinY(),
									geom.getEnvelopeInternal().getMaxY());
							NumericData[] boundsRange = {
								xRange,
								yRange
							};

							// Convert the data to how the api expects and index
							// using strategy above
							BasicNumericDataset convertedBounds = new BasicNumericDataset(
									boundsRange);
							List<ByteArrayId> insertIds = targetStrategy.getInsertionIds(
									convertedBounds);

							// When we span more than one row each individual
							// get
							// added as a separate output pair

							for (Iterator<ByteArrayId> iter = insertIds.iterator(); iter.hasNext();) {
								ByteArrayId id = iter.next();
								// Id decomposes to byte array of Tier, Bin, SFC
								// (Hilbert in this case) id)
								// There may be value in decomposing the id and
								// storing tier + sfcIndex as a tuple key of new
								// RDD
								Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> indexPair = new Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>(
										id,
										t._2);
								reprojected.add(
										indexPair);
							}
						}
						else {
							LOGGER.warn(
									"Tier '" + targetTierId + "' not found in index strategy");
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