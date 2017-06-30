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

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.type.BasicFeatureTypes;
import org.geotools.referencing.CRS;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.analytic.javaspark.KMeansHullGenerator;
import mil.nga.giat.geowave.analytic.javaspark.KMeansRunner;
import mil.nga.giat.geowave.analytic.javaspark.KMeansUtils;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import scala.Tuple2;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveJavaSparkKMeansIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(
			GeoWaveJavaSparkKMeansIT.class);

	protected static final String HAIL_TEST_CASE_PACKAGE = TestUtils.TEST_CASE_BASE + "hail_test_case/";
	protected static final String HAIL_SHAPEFILE_FILE = HAIL_TEST_CASE_PACKAGE + "hail.shp";

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		// GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions inputDataStore;

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
		TestUtils.deleteAll(
				inputDataStore);

		// Load data
		TestUtils.testLocalIngest(
				inputDataStore,
				DimensionalityType.SPATIAL,
				HAIL_SHAPEFILE_FILE,
				1);

		// Create the runner
		final KMeansRunner runner = new KMeansRunner();
		runner.setInputDataStore(
				inputDataStore);

		// Run kmeans
		try {
			runner.run();
		}
		catch (final IOException e) {
			throw new RuntimeException(
					"Failed to execute: " + e.getMessage());
		}

		// Create the output
		final KMeansModel clusterModel = runner.getOutputModel();

		final JavaPairRDD<Integer, Geometry> hullsRDD = KMeansHullGenerator.generateHullsRDD(
				runner.getInputCentroids(),
				clusterModel);

		Assert.assertTrue(
				"centroids from the model should match the hull count",
				clusterModel.clusterCenters().length == hullsRDD.count());

		System.out.println(
				"KMeans cluster hulls:");
		for (final Tuple2<Integer, Geometry> hull : hullsRDD.collect()) {
			System.out.println(
					"> Hull size (verts): " + hull._2.getNumPoints());

			System.out.println(
					"> Hull centroid: " + hull._2.getCentroid().toString());

		}

		// Write out the centroid features
		DataAdapter centroidAdapter = KMeansUtils.writeClusterCentroids(
				clusterModel,
				inputDataStore,
				"kmeans-centroids-test");

		// Query back from the new adapter
		queryFeatures(
				centroidAdapter,
				clusterModel.clusterCenters().length);

		// Generate and write out the hull features
		DataAdapter hullAdapter = KMeansUtils.writeClusterHulls(
				runner.getInputCentroids(),
				clusterModel,
				inputDataStore,
				"kmeans-hulls-test");

		// Query back from the new adapter
		queryFeatures(
				hullAdapter,
				clusterModel.clusterCenters().length);

		TestUtils.deleteAll(
				inputDataStore);
	}

	private void queryFeatures(
			final DataAdapter dataAdapter,
			final int expectedCount) {
		final DataStore featureStore = inputDataStore.createDataStore();
		int count = 0;

		try (final CloseableIterator<?> iter = featureStore.query(
				new QueryOptions(
						dataAdapter.getAdapterId(),
						TestUtils.DEFAULT_SPATIAL_INDEX.getId()),
				new EverythingQuery())) {

			while (iter.hasNext()) {
				final Object maybeFeat = iter.next();
				Assert.assertTrue(
						"Iterator should return simple feature in this test",
						maybeFeat instanceof SimpleFeature);

				final SimpleFeature isFeat = (SimpleFeature) maybeFeat;

				final Geometry geom = (Geometry) isFeat.getAttribute(
						0);

				count++;
				LOGGER.warn(
						count + ": " + isFeat.getID() + " - " + geom.toString());
			}

			LOGGER.warn(
					"Counted " + count + " features in datastore for " + StringUtils.stringFromBinary(
							dataAdapter.getAdapterId().getBytes()));
		}
		catch (final Exception e) {
			e.printStackTrace();
		}

		Assert.assertTrue(
				"Iterator should return " + expectedCount + " features in this test",
				count == expectedCount);

	}
}
