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
package org.locationtech.geowave.analytic.spark;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputFormat;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class GeoWaveRDDLoader
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveRDDLoader.class);

	public static GeoWaveRDD loadRDD(
			final SparkContext sc,
			final DataStorePluginOptions storeOptions )
			throws IOException {
		final RDDOptions defaultOptions = new RDDOptions();
		return GeoWaveRDDLoader.loadRDD(
				sc,
				storeOptions,
				defaultOptions);
	}

	public static GeoWaveRDD loadRDD(
			final SparkContext sc,
			final DataStorePluginOptions storeOptions,
			final RDDOptions rddOpts )
			throws IOException {
		final JavaPairRDD<GeoWaveInputKey, SimpleFeature> rawRDD = GeoWaveRDDLoader.loadRawRDD(
				sc,
				storeOptions,
				rddOpts);
		return new GeoWaveRDD(
				rawRDD);
	}

	public static GeoWaveIndexedRDD loadIndexedRDD(
			final SparkContext sc,
			final DataStorePluginOptions storeOptions,
			final RDDOptions rddOpts,
			final NumericIndexStrategy indexStrategy )
			throws IOException {
		final GeoWaveRDD wrappedRDD = GeoWaveRDDLoader.loadRDD(
				sc,
				storeOptions,
				rddOpts);
		if (wrappedRDD == null) {
			return null;
		}
		// Index strategy can be expensive so we will broadcast it and store it
		Broadcast<NumericIndexStrategy> broadcastStrategy = null;
		if (indexStrategy != null) {
			broadcastStrategy = (Broadcast<NumericIndexStrategy>) RDDUtils.broadcastIndexStrategy(
					sc,
					indexStrategy);
		}

		final GeoWaveIndexedRDD returnRDD = new GeoWaveIndexedRDD(
				wrappedRDD,
				broadcastStrategy);
		return returnRDD;
	}

	public static GeoWaveIndexedRDD loadIndexedRDD(
			final SparkContext sc,
			final GeoWaveRDD inputRDD,
			final NumericIndexStrategy indexStrategy )
			throws IOException {
		if ((inputRDD == null) || !inputRDD.isLoaded()) {
			return null;
		}
		// Index strategy can be expensive so we will broadcast it and store it
		Broadcast<NumericIndexStrategy> broadcastStrategy = null;
		if (indexStrategy != null) {
			broadcastStrategy = (Broadcast<NumericIndexStrategy>) RDDUtils.broadcastIndexStrategy(
					sc,
					indexStrategy);
		}

		final GeoWaveIndexedRDD returnRDD = new GeoWaveIndexedRDD(
				inputRDD,
				broadcastStrategy);
		return returnRDD;
	}

	public static JavaPairRDD<GeoWaveInputKey, SimpleFeature> loadRawRDD(
			final SparkContext sc,
			final DataStorePluginOptions storeOptions,
			final RDDOptions rddOpts )
			throws IOException {
		if (sc == null) {
			LOGGER.error("Must supply a valid Spark Context. Please set SparkContext and try again.");
			return null;
		}

		if (storeOptions == null) {
			LOGGER.error("Must supply input store to load. Please set storeOptions and try again.");
			return null;
		}

		if (rddOpts == null) {
			LOGGER.error("Must supply valid RDDOptions to load a rdd.");
			return null;
		}

		final Configuration conf = new Configuration(
				sc.hadoopConfiguration());

		GeoWaveInputFormat.setStoreOptions(
				conf,
				storeOptions);

		if (rddOpts.getQuery() != null) {
			GeoWaveInputFormat.setQuery(
					conf,
					rddOpts.getQuery(),
					storeOptions.createAdapterStore(),
					storeOptions.createInternalAdapterStore(),
					storeOptions.createIndexStore());
		}

		if ((rddOpts.getMinSplits() > -1) || (rddOpts.getMaxSplits() > -1)) {
			GeoWaveInputFormat.setMinimumSplitCount(
					conf,
					rddOpts.getMinSplits());
			GeoWaveInputFormat.setMaximumSplitCount(
					conf,
					rddOpts.getMaxSplits());
		}
		else {
			final int defaultSplitsSpark = sc.getConf().getInt(
					"spark.default.parallelism",
					-1);
			// Attempt to grab default partition count for spark and split data
			// along that.
			// Otherwise just fallback to default according to index strategy
			if (defaultSplitsSpark != -1) {
				GeoWaveInputFormat.setMinimumSplitCount(
						conf,
						defaultSplitsSpark);
				GeoWaveInputFormat.setMaximumSplitCount(
						conf,
						defaultSplitsSpark);
			}
		}

		final RDD<Tuple2<GeoWaveInputKey, SimpleFeature>> rdd = sc.newAPIHadoopRDD(
				conf,
				GeoWaveInputFormat.class,
				GeoWaveInputKey.class,
				SimpleFeature.class);

		final JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = JavaPairRDD.fromJavaRDD(rdd.toJavaRDD());

		return javaRdd;
	}

}
