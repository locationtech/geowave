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
package mil.nga.giat.geowave.test.spark;

import java.io.IOException;
import java.util.Date;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.analytic.spark.kmeans.KMeansHullGenerator;
import mil.nga.giat.geowave.analytic.spark.kmeans.KMeansRunner;
import mil.nga.giat.geowave.core.geotime.TimeUtils;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.annotation.Environments;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.Environments.Environment;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import scala.Tuple2;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.SPARK
})
public class GeoWaveJavaSparkKMeansIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveJavaSparkKMeansIT.class);

	protected static final String HAIL_TEST_CASE_PACKAGE = TestUtils.TEST_CASE_BASE + "hail_test_case/";
	protected static final String HAIL_SHAPEFILE_FILE = HAIL_TEST_CASE_PACKAGE + "hail.shp";
	protected static final String CQL_FILTER = "BBOX(the_geom, -100, 30, -90, 40)";

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.DYNAMODB,
		GeoWaveStoreType.CASSANDRA
	})
	protected DataStorePluginOptions inputDataStore;

	private static long startMillis;

	@BeforeClass
	public static void reportTestStart() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*  RUNNING GeoWaveJavaSparkKMeansIT     *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTestFinish() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED GeoWaveJavaSparkKMeansIT     *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Test
	public void testKMeansRunner() {
		SparkContext context = SparkTestEnvironment.getInstance().getDefaultContext();

		// Load data
		TestUtils.testLocalIngest(
				inputDataStore,
				DimensionalityType.SPATIAL,
				HAIL_SHAPEFILE_FILE,
				1);

		String adapterId = "hail";

		// Create the runner
		long mark = System.currentTimeMillis();
		final KMeansRunner runner = new KMeansRunner();
		runner.setJavaSparkContext(JavaSparkContext.fromSparkContext(context));
		runner.setInputDataStore(inputDataStore);
		runner.setAdapterId(adapterId);
		runner.setCqlFilter(CQL_FILTER);
		runner.setUseTime(true);
		// Set output params to write centroids + hulls to store.
		runner.setOutputDataStore(inputDataStore);
		runner.setCentroidTypeName("kmeans-centroids-test");

		runner.setGenerateHulls(true);
		runner.setComputeHullData(true);
		runner.setHullTypeName("kmeans-hulls-test");

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

		long dur = (System.currentTimeMillis() - mark);
		LOGGER.warn("KMeans duration: " + dur + " ms.");
		// Write out the centroid features

		short centroidInternalAdapterId = inputDataStore.createInternalAdapterStore().getInternalAdapterId(
				new ByteArrayId(
						"kmeans-centroids-test"));
		DataAdapter centroidAdapter = inputDataStore.createAdapterStore().getAdapter(
				centroidInternalAdapterId);

		// Query back from the new adapter
		mark = System.currentTimeMillis();
		queryFeatures(
				centroidAdapter,
				clusterModel.clusterCenters().length);
		dur = (System.currentTimeMillis() - mark);
		LOGGER.warn("Centroid verify: " + dur + " ms.");

		// Generate the hulls
		final JavaPairRDD<Integer, Iterable<Vector>> groupByRDD = KMeansHullGenerator.groupByIndex(
				runner.getInputCentroids(),
				clusterModel);
		final JavaPairRDD<Integer, Geometry> hullsRDD = KMeansHullGenerator.generateHullsRDD(groupByRDD);

		Assert.assertTrue(
				"centroids from the model should match the hull count",
				clusterModel.clusterCenters().length == hullsRDD.count());

		System.out.println("KMeans cluster hulls:");
		for (final Tuple2<Integer, Geometry> hull : hullsRDD.collect()) {
			System.out.println("> Hull size (verts): " + hull._2.getNumPoints());

			System.out.println("> Hull centroid: " + hull._2.getCentroid().toString());

		}

		short hullInternalAdapterId = inputDataStore.createInternalAdapterStore().getInternalAdapterId(
				new ByteArrayId(
						"kmeans-hulls-test"));
		// Write out the hull features w/ metadata
		DataAdapter hullAdapter = inputDataStore.createAdapterStore().getAdapter(
				hullInternalAdapterId);

		mark = System.currentTimeMillis();
		// Query back from the new adapter
		queryFeatures(
				hullAdapter,
				clusterModel.clusterCenters().length);
		dur = (System.currentTimeMillis() - mark);
		LOGGER.warn("Hull verify: " + dur + " ms.");

		TestUtils.deleteAll(inputDataStore);

	}

	private void queryFeatures(
			final DataAdapter dataAdapter,
			final int expectedCount ) {
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

				final Geometry geom = (Geometry) isFeat.getAttribute(0);

				count++;
				LOGGER.warn(count + ": " + isFeat.getID() + " - " + geom.toString());

				for (AttributeDescriptor attrDesc : isFeat.getFeatureType().getAttributeDescriptors()) {
					final Class<?> bindingClass = attrDesc.getType().getBinding();
					if (TimeUtils.isTemporal(bindingClass)) {
						String timeField = attrDesc.getLocalName();
						Date time = (Date) isFeat.getAttribute(timeField);
						LOGGER.warn("  time = " + time);
					}
					else {
						LOGGER.warn(attrDesc.getLocalName() + " = " + isFeat.getAttribute(attrDesc.getLocalName()));
					}
				}

			}

			LOGGER.warn("Counted " + count + " features in datastore for "
					+ StringUtils.stringFromBinary(dataAdapter.getAdapterId().getBytes()));
		}
		catch (final Exception e) {
			e.printStackTrace();
		}

		Assert.assertTrue(
				"Iterator should return " + expectedCount + " features in this test",
				count == expectedCount);

	}
}
