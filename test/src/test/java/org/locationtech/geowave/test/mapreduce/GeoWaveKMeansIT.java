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
package org.locationtech.geowave.test.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.analytic.AnalyticItemWrapper;
import org.locationtech.geowave.analytic.GeometryDataSetGenerator;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.SimpleFeatureItemWrapperFactory;
import org.locationtech.geowave.analytic.clustering.CentroidManager;
import org.locationtech.geowave.analytic.clustering.CentroidManagerGeoWave;
import org.locationtech.geowave.analytic.distance.FeatureCentroidDistanceFn;
import org.locationtech.geowave.analytic.mapreduce.clustering.runner.MultiLevelJumpKMeansClusteringJobRunner;
import org.locationtech.geowave.analytic.mapreduce.clustering.runner.MultiLevelKMeansClusteringJobRunner;
import org.locationtech.geowave.analytic.param.ClusteringParameters;
import org.locationtech.geowave.analytic.param.ExtractParameters;
import org.locationtech.geowave.analytic.param.GlobalParameters;
import org.locationtech.geowave.analytic.param.JumpParameters;
import org.locationtech.geowave.analytic.param.MapReduceParameters;
import org.locationtech.geowave.analytic.param.ParameterEnum;
import org.locationtech.geowave.analytic.param.SampleParameters;
import org.locationtech.geowave.analytic.param.StoreParameters.StoreParam;
import org.locationtech.geowave.analytic.store.PersistableStore;
import org.locationtech.geowave.core.geotime.store.query.SpatialQuery;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.MAP_REDUCE
})
public class GeoWaveKMeansIT
{
	@GeoWaveTestStore({
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.REDIS
	})
	protected DataStorePluginOptions dataStorePluginOptions;

	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveKMeansIT.class);
	private static long startMillis;

	@BeforeClass
	public static void startTimer() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*         RUNNING GeoWaveKMeansIT       *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*      FINISHED GeoWaveKMeansIT         *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	private SimpleFeatureBuilder getBuilder() {
		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName("test");
		typeBuilder.setCRS(DefaultGeographicCRS.WGS84); // <- Coordinate
														// reference
		// add attributes in order
		typeBuilder.add(
				"geom",
				Geometry.class);
		typeBuilder.add(
				"name",
				String.class);
		typeBuilder.add(
				"count",
				Long.class);

		// build the type
		return new SimpleFeatureBuilder(
				typeBuilder.buildFeatureType());
	}

	final GeometryDataSetGenerator dataGenerator = new GeometryDataSetGenerator(
			new FeatureCentroidDistanceFn(),
			getBuilder());

	private void testIngest(
			final DataStore dataStore )
			throws IOException {

		dataGenerator.writeToGeoWave(
				dataStore,
				dataGenerator.generatePointSet(
						0.15,
						0.2,
						3,
						800,
						new double[] {
							-100,
							-45
						},
						new double[] {
							-90,
							-35
						}));
		dataGenerator.writeToGeoWave(
				dataStore,
				dataGenerator.generatePointSet(
						0.15,
						0.2,
						6,
						600,
						new double[] {
							0,
							0
						},
						new double[] {
							10,
							10
						}));
		dataGenerator.writeToGeoWave(
				dataStore,
				dataGenerator.generatePointSet(
						0.15,
						0.2,
						4,
						900,
						new double[] {
							65,
							35
						},
						new double[] {
							75,
							45
						}));

	}

	@Test
	public void testIngestAndQueryGeneralGpx()
			throws Exception {
		TestUtils.deleteAll(dataStorePluginOptions);
		testIngest(dataStorePluginOptions.createDataStore());

		runKPlusPlus(new SpatialQuery(
				dataGenerator.getBoundingRegion()));
	}

	private void runKPlusPlus(
			final QueryConstraints query )
			throws Exception {

		final MultiLevelKMeansClusteringJobRunner jobRunner = new MultiLevelKMeansClusteringJobRunner();
		final int res = jobRunner.run(
				MapReduceTestUtils.getConfiguration(),
				new PropertyManagement(
						new ParameterEnum[] {
							ExtractParameters.Extract.QUERY,
							ExtractParameters.Extract.MIN_INPUT_SPLIT,
							ExtractParameters.Extract.MAX_INPUT_SPLIT,
							ClusteringParameters.Clustering.ZOOM_LEVELS,
							ClusteringParameters.Clustering.MAX_ITERATIONS,
							ClusteringParameters.Clustering.RETAIN_GROUP_ASSIGNMENTS,
							ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID,
							StoreParam.INPUT_STORE,
							GlobalParameters.Global.BATCH_ID,
							MapReduceParameters.MRConfig.HDFS_BASE_DIR,
							SampleParameters.Sample.MAX_SAMPLE_SIZE,
							SampleParameters.Sample.MIN_SAMPLE_SIZE
						},
						new Object[] {
							QueryBuilder.newBuilder().constraints(
									query).build(),
							MapReduceTestUtils.MIN_INPUT_SPLITS,
							MapReduceTestUtils.MAX_INPUT_SPLITS,
							2,
							2,
							false,
							"centroid",
							new PersistableStore(
									dataStorePluginOptions),
							"bx1",
							TestUtils.TEMP_DIR + File.separator + MapReduceTestEnvironment.HDFS_BASE_DIRECTORY + "/t1",
							3,
							2
						}));

		Assert.assertEquals(
				0,
				res);

		final DataStore dataStore = dataStorePluginOptions.createDataStore();
		final IndexStore indexStore = dataStorePluginOptions.createIndexStore();
		final PersistentAdapterStore adapterStore = dataStorePluginOptions.createAdapterStore();
		final InternalAdapterStore internalAdapterStore = dataStorePluginOptions.createInternalAdapterStore();
		final int resultCounLevel1 = countResults(
				dataStore,
				indexStore,
				adapterStore,
				internalAdapterStore,
				"bx1",
				1, // level
				1);
		final int resultCounLevel2 = countResults(
				dataStore,
				indexStore,
				adapterStore,
				internalAdapterStore,
				"bx1",
				2, // level
				resultCounLevel1);
		Assert.assertTrue(resultCounLevel2 >= 2);
		// for travis-ci to run, we want to limit the memory consumption
		System.gc();
	}

	private void runKJumpPlusPlus(
			final QueryConstraints query )
			throws Exception {

		final MultiLevelJumpKMeansClusteringJobRunner jobRunner2 = new MultiLevelJumpKMeansClusteringJobRunner();
		final int res2 = jobRunner2.run(
				MapReduceTestUtils.getConfiguration(),
				new PropertyManagement(
						new ParameterEnum[] {
							ExtractParameters.Extract.QUERY,
							ExtractParameters.Extract.MIN_INPUT_SPLIT,
							ExtractParameters.Extract.MAX_INPUT_SPLIT,
							ClusteringParameters.Clustering.ZOOM_LEVELS,
							ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID,
							StoreParam.INPUT_STORE,
							GlobalParameters.Global.BATCH_ID,
							MapReduceParameters.MRConfig.HDFS_BASE_DIR,
							JumpParameters.Jump.RANGE_OF_CENTROIDS,
							JumpParameters.Jump.KPLUSPLUS_MIN,
							ClusteringParameters.Clustering.MAX_ITERATIONS
						},
						new Object[] {
							QueryBuilder.newBuilder().constraints(
									query).build(),
							MapReduceTestUtils.MIN_INPUT_SPLITS,
							MapReduceTestUtils.MAX_INPUT_SPLITS,
							2,
							"centroid",
							new PersistableStore(
									dataStorePluginOptions),
							"bx2",
							TestUtils.TEMP_DIR + File.separator + MapReduceTestEnvironment.HDFS_BASE_DIRECTORY + "/t2",
							new NumericRange(
									4,
									7),
							5,
							2
						}));

		Assert.assertEquals(
				0,
				res2);

		final DataStore dataStore = dataStorePluginOptions.createDataStore();
		final IndexStore indexStore = dataStorePluginOptions.createIndexStore();
		final PersistentAdapterStore adapterStore = dataStorePluginOptions.createAdapterStore();
		final InternalAdapterStore internalAdapterStore = dataStorePluginOptions.createInternalAdapterStore();
		final int jumpRresultCounLevel1 = countResults(
				dataStore,
				indexStore,
				adapterStore,
				internalAdapterStore,
				"bx2",
				1,
				1);
		final int jumpRresultCounLevel2 = countResults(
				dataStore,
				indexStore,
				adapterStore,
				internalAdapterStore,
				"bx2",
				2,
				jumpRresultCounLevel1);
		Assert.assertTrue(jumpRresultCounLevel1 >= 2);
		Assert.assertTrue(jumpRresultCounLevel2 >= 2);
		// for travis-ci to run, we want to limit the memory consumption
		System.gc();
	}

	private int countResults(
			final DataStore dataStore,
			final IndexStore indexStore,
			final PersistentAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore,
			final String batchID,
			final int level,
			final int expectedParentCount )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {

		final CentroidManager<SimpleFeature> centroidManager = new CentroidManagerGeoWave<>(
				dataStore,
				indexStore,
				adapterStore,
				new SimpleFeatureItemWrapperFactory(),
				"centroid",
				internalAdapterStore.addTypeName("centroid"),
				TestUtils.DEFAULT_SPATIAL_INDEX.getName(),
				batchID,
				level);

		final CentroidManager<SimpleFeature> hullManager = new CentroidManagerGeoWave<>(
				dataStore,
				indexStore,
				adapterStore,
				new SimpleFeatureItemWrapperFactory(),
				"convex_hull",
				internalAdapterStore.addTypeName("convex_hull"),
				TestUtils.DEFAULT_SPATIAL_INDEX.getName(),
				batchID,
				level);

		int childCount = 0;
		int parentCount = 0;
		for (final String grp : centroidManager.getAllCentroidGroups()) {
			final List<AnalyticItemWrapper<SimpleFeature>> centroids = centroidManager.getCentroidsForGroup(grp);
			final List<AnalyticItemWrapper<SimpleFeature>> hulls = hullManager.getCentroidsForGroup(grp);

			for (final AnalyticItemWrapper<SimpleFeature> centroid : centroids) {
				if (centroid.getAssociationCount() == 0) {
					continue;
				}
				Assert.assertTrue(centroid.getGeometry() != null);
				Assert.assertTrue(centroid.getBatchID() != null);
				boolean found = false;
				final List<SimpleFeature> features = new ArrayList<>();
				for (final AnalyticItemWrapper<SimpleFeature> hull : hulls) {
					found |= (hull.getName().equals(centroid.getName()));
					Assert.assertTrue(hull.getGeometry() != null);
					Assert.assertTrue(hull.getBatchID() != null);
					features.add(hull.getWrappedItem());
				}
				System.out.println(features);
				Assert.assertTrue(
						grp,
						found);
				childCount++;
			}
			parentCount++;
		}
		Assert.assertEquals(
				batchID,
				expectedParentCount,
				parentCount);
		return childCount;

	}
}
