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

import java.io.File;

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
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
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

	private static final String TEST_BOX_FILTER_FILE = TEST_FILTER_PACKAGE + "Box-Filter.shp";
	private static final String TEST_POLYGON_FILTER_FILE = TEST_FILTER_PACKAGE + "Polygon-Filter.shp";

	@GeoWaveTestStore(value = {
//		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.HBASE
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
	public void testKMeans() {
		// Set up Spark
		SparkConf sparkConf = new SparkConf();

		sparkConf.setAppName(
				"JavaSparkKMeansIT");
		sparkConf.setMaster(
				"local");
		JavaSparkContext context = new JavaSparkContext(
				sparkConf);

		// ingest both lines and points
		long mark = System.currentTimeMillis();
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				HAIL_SHAPEFILE_FILE,
				1);

		long dur = (System.currentTimeMillis() - mark);
		LOGGER.warn(
				"Ingest (points) duration = " + dur + " ms.");

		try {
			mark = System.currentTimeMillis();

			final DistributableQuery query = TestUtils.resourceToQuery(
					new File(
							TEST_BOX_FILTER_FILE).toURI().toURL());

			// Load RDD from datastore
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaPairRdd = GeoWaveRDD.rddForSimpleFeatures(
					context.sc(),
					dataStore,
					query);

			// Retrieve the centroids
			JavaRDD<Vector> centroidVectors = GeoWaveRDD.rddPointToVector(
					GeoWaveRDD.rddCentroids(
							javaPairRdd));
			
			centroidVectors.cache();

			// Run KMeans
			int numClusters = 8;
			int numIterations = 20;
			KMeansModel clusters = KMeans.train(
					centroidVectors.rdd(),
					numClusters,
					numIterations);

			System.out.println(
					"Cluster centers:");
			for (Vector center : clusters.clusterCenters()) {
				System.out.println(
						" " + center);
			}
			double cost = clusters.computeCost(
					centroidVectors.rdd());
			System.out.println(
					"Cost: " + cost);

			// Evaluate clustering by computing Within Set Sum of Squared Errors
			double WSSSE = clusters.computeCost(
					centroidVectors.rdd());
			System.out.println(
					"Within Set Sum of Squared Errors = " + WSSSE);

			// Save and load model
			clusters.save(
					context.sc(),
					"target/org/apache/spark/JavaKMeansExample/KMeansModel");
			KMeansModel sameModel = KMeansModel.load(
					context.sc(),
					"target/org/apache/spark/JavaKMeansExample/KMeansModel");
			// $example off$

			context.stop();

			dur = (System.currentTimeMillis() - mark);

		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(
					dataStore);
			Assert.fail(
					"Error occurred while testing a bounding box query of spatial index: '" + e.getLocalizedMessage()
							+ "'");
		}

		TestUtils.deleteAll(
				dataStore);
	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}
}
