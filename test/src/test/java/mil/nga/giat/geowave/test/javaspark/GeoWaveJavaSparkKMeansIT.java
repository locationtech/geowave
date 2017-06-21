/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.test.javaspark;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.analytic.javaspark.GeoWaveRDD;
import mil.nga.giat.geowave.analytic.javaspark.KMeansRunner;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import mil.nga.giat.geowave.test.basic.AbstractGeoWaveBasicVectorIT;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveJavaSparkKMeansIT extends
		AbstractGeoWaveBasicVectorIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(
			GeoWaveJavaSparkKMeansIT.class);

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
//		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStore;

	private static long startMillis;

	@BeforeClass
	public static void reportTestStart() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn(
				"-----------------------------------------");
		LOGGER.warn(
				"*                                       *");
		LOGGER.warn(
				"*  RUNNING GeoWaveJavaSparkKMeansIT     *");
		LOGGER.warn(
				"*                                       *");
		LOGGER.warn(
				"-----------------------------------------");
	}

	@AfterClass
	public static void reportTestFinish() {
		LOGGER.warn(
				"-----------------------------------------");
		LOGGER.warn(
				"*                                       *");
		LOGGER.warn(
				"* FINISHED GeoWaveJavaSparkKMeansIT     *");
		LOGGER.warn(
				"*         " + ((System.currentTimeMillis() - startMillis) / 1000) + "s elapsed.                 *");
		LOGGER.warn(
				"*                                       *");
		LOGGER.warn(
				"-----------------------------------------");
	}
	

	@Test
	public void testKMeansRunner() {
		// Load data
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				HAIL_SHAPEFILE_FILE,
				1);

		// Create the runner
		KMeansRunner runner = new KMeansRunner();
		runner.setInputDataStore(dataStore);
		
		// Run kmeans
		try {
			runner.run();
		}
		catch (IOException e) {
			throw new RuntimeException(
					"Failed to execute: " + e.getMessage());
		}

		// Show the output
		KMeansModel clusterModel = runner.getOutputModel();

		System.out.println("KMeans cluster centroids:");
		for (Vector center : clusterModel.clusterCenters()) {
			System.out.println("> " + center);
		}
	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}
}
