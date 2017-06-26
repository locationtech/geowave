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

import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
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
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.AdapterIdQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

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
		TestUtils.deleteAll(inputDataStore);

		// Load data
		TestUtils.testLocalIngest(
				inputDataStore,
				DimensionalityType.SPATIAL,
				HAIL_SHAPEFILE_FILE,
				1);

		// Create the runner
		KMeansRunner runner = new KMeansRunner();
		runner.setInputDataStore(
				inputDataStore);

		// Run kmeans
		try {
			runner.run();
		}
		catch (IOException e) {
			throw new RuntimeException(
					"Failed to execute: " + e.getMessage());
		}

		// Create the output
		KMeansModel clusterModel = runner.getOutputModel();
		
		// Test the convex hull generator
		Geometry[] hulls = KMeansHullGenerator.generateHulls(
				runner.getInputCentroids(),
				clusterModel);
		
		Assert.assertTrue(
				"centroids from the model should match the hull count",
				clusterModel.clusterCenters().length == hulls.length);

		System.out.println(
				"KMeans cluster hulls:");
		for (Geometry hull : hulls) {
			System.out.println(
					"> Hull size (verts): " + hull.getNumPoints());
			
			System.out.println(
					"> Hull centroid: " + hull.getCentroid().toString());

		}		

		DataStore featureStore = writeFeatures(
				clusterModel.clusterCenters());
		
		TestUtils.deleteAll(inputDataStore);
	}

	private DataStore writeFeatures(
			Vector[] centers ) {
		LOGGER.warn(
				"KMeans cluster centroids:");

		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName(
				"kmeans-centroids");
		
		final AttributeTypeBuilder attrBuilder = new AttributeTypeBuilder();

		typeBuilder.add(
				attrBuilder
						.binding(
								Geometry.class)
						.nillable(
								false)
						.buildDescriptor(
								Geometry.class.getName().toString()));
		
		typeBuilder.add(
				attrBuilder
						.binding(
								String.class)
						.nillable(
								false)
						.buildDescriptor(
								"KMeansData"));

		final SimpleFeatureType sfType = typeBuilder.buildFeatureType();
		final SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(
				sfType);

		final FeatureDataAdapter featureAdapter = new FeatureDataAdapter(
				sfType);

		DataStore featureStore = inputDataStore.createDataStore();

		try {
			IndexWriter writer = featureStore.createWriter(
					featureAdapter,
					TestUtils.DEFAULT_SPATIAL_INDEX);

			int i = 0;
			for (Vector center : centers) {
				LOGGER.warn(
						"> " + center);

				double lon = center.apply(
						0);
				double lat = center.apply(
						1);

				sfBuilder.set(
						Geometry.class.getName(),
						GeometryUtils.GEOMETRY_FACTORY.createPoint(
								new Coordinate(
										lon,
										lat)));

				sfBuilder.set(
						"KMeansData",
						"KMeansCentroid");			

				final SimpleFeature sf = sfBuilder.buildFeature(
						"Centroid-" + i++);

				writer.write(
						sf);
			}
		}
		catch (Exception e) {
			LOGGER.error("Error writing centroids!");
			e.printStackTrace();
		}

		// Query back from the new adapter
		queryFeatures(
				featureStore,
				featureAdapter);

		return featureStore;
	}

	private void queryFeatures(
			DataStore featureStore,
			FeatureDataAdapter featureAdapter) {
		try (final CloseableIterator<?> iter = featureStore.query(
				new QueryOptions(),
				new AdapterIdQuery(featureAdapter.getAdapterId()))) {

			int count = 0;
			while (iter.hasNext()) {
				final Object maybeFeat = iter.next();
				Assert.assertTrue(
						"Iterator should return simple feature in this test",
						maybeFeat instanceof SimpleFeature);

				final SimpleFeature isFeat = (SimpleFeature) maybeFeat;

				Geometry geom = (Geometry) isFeat.getAttribute(
						0);

				count++;
				LOGGER.warn(count + ": " + isFeat.getID() + " - " +
						geom.toString());
			}

			LOGGER.warn(
					"Counted " + count + " kmeans centroids in datastore");

			Assert.assertTrue(
					"Iterator should return 8 centroids in this test",
					count == 8);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
