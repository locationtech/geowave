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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.analytic.AnalyticItemWrapper;
import org.locationtech.geowave.analytic.GeometryDataSetGenerator;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.ShapefileTool;
import org.locationtech.geowave.analytic.SimpleFeatureItemWrapperFactory;
import org.locationtech.geowave.analytic.clustering.CentroidManager;
import org.locationtech.geowave.analytic.clustering.CentroidManagerGeoWave;
import org.locationtech.geowave.analytic.clustering.ClusteringUtils;
import org.locationtech.geowave.analytic.distance.FeatureCentroidOrthodromicDistanceFn;
import org.locationtech.geowave.analytic.mapreduce.GeoWaveInputFormatConfiguration;
import org.locationtech.geowave.analytic.mapreduce.dbscan.DBScanIterationsJobRunner;
import org.locationtech.geowave.analytic.param.ClusteringParameters;
import org.locationtech.geowave.analytic.param.ExtractParameters;
import org.locationtech.geowave.analytic.param.GlobalParameters;
import org.locationtech.geowave.analytic.param.InputParameters;
import org.locationtech.geowave.analytic.param.MapReduceParameters;
import org.locationtech.geowave.analytic.param.OutputParameters;
import org.locationtech.geowave.analytic.param.ParameterEnum;
import org.locationtech.geowave.analytic.param.PartitionParameters;
import org.locationtech.geowave.analytic.param.StoreParameters.StoreParam;
import org.locationtech.geowave.analytic.partitioner.OrthodromicDistancePartitioner;
import org.locationtech.geowave.analytic.store.PersistableStore;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.query.SpatialQuery;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveIT;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.FactoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.MAP_REDUCE
})
public class DBScanIT extends
		AbstractGeoWaveIT
{
	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStorePluginOptions;

	private final static Logger LOGGER = LoggerFactory.getLogger(DBScanIT.class);
	private static long startMillis;

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStorePluginOptions;
	}

	@BeforeClass
	public static void startTimer() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*         RUNNING DBScanIT              *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*      FINISHED DBScanIT                *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	private SimpleFeatureBuilder getBuilder() {
		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName("test");
		typeBuilder.setSRS(ClusteringUtils.CLUSTERING_CRS);
		try {
			typeBuilder.setCRS(CRS.decode(
					ClusteringUtils.CLUSTERING_CRS,
					true));
		}
		catch (final FactoryException e) {
			e.printStackTrace();
			return null;
		}
		// add attributes in order
		typeBuilder.add(
				"geom",
				Point.class);
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
			new FeatureCentroidOrthodromicDistanceFn(),
			getBuilder());

	@Test
	public void testDBScan() {
		dataGenerator.setIncludePolygons(false);
		try {
			ingest(dataStorePluginOptions.createDataStore());
		}
		catch (final IOException e1) {
			e1.printStackTrace();
			TestUtils.deleteAll(dataStorePluginOptions);
			Assert.fail("Unable to ingest data in DBScanIT");
		}

		try {
			runScan(new SpatialQuery(
					dataGenerator.getBoundingRegion()));
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStorePluginOptions);
			Assert.fail("Exception during scan of DBScanIT");
		}

		TestUtils.deleteAll(dataStorePluginOptions);
	}

	private void runScan(
			final QueryConstraints query )
			throws Exception {

		final DBScanIterationsJobRunner jobRunner = new DBScanIterationsJobRunner();
		final Configuration conf = MapReduceTestUtils.getConfiguration();
		final int res = jobRunner.run(
				conf,
				new PropertyManagement(
						new ParameterEnum[] {
							ExtractParameters.Extract.QUERY,
							ExtractParameters.Extract.MIN_INPUT_SPLIT,
							ExtractParameters.Extract.MAX_INPUT_SPLIT,
							PartitionParameters.Partition.MAX_DISTANCE,
							PartitionParameters.Partition.PARTITIONER_CLASS,
							ClusteringParameters.Clustering.MINIMUM_SIZE,
							StoreParam.INPUT_STORE,
							MapReduceParameters.MRConfig.HDFS_BASE_DIR,
							OutputParameters.Output.REDUCER_COUNT,
							InputParameters.Input.INPUT_FORMAT,
							GlobalParameters.Global.BATCH_ID,
							PartitionParameters.Partition.PARTITION_DECREASE_RATE,
							PartitionParameters.Partition.PARTITION_PRECISION
						},
						new Object[] {
							QueryBuilder.newBuilder().constraints(
									query).build(),
							Integer.toString(MapReduceTestUtils.MIN_INPUT_SPLITS),
							Integer.toString(MapReduceTestUtils.MAX_INPUT_SPLITS),
							10000.0,
							OrthodromicDistancePartitioner.class,
							10,
							new PersistableStore(
									dataStorePluginOptions),
							TestUtils.TEMP_DIR + File.separator + MapReduceTestEnvironment.HDFS_BASE_DIRECTORY + "/t1",
							2,
							GeoWaveInputFormatConfiguration.class,
							"bx5",
							0.15,
							0.95
						}));

		Assert.assertEquals(
				0,
				res);

		Assert.assertTrue(readHulls() > 2);
		// for travis-ci to run, we want to limit the memory consumption
		System.gc();
	}

	private int readHulls()
			throws Exception {
		final CentroidManager<SimpleFeature> centroidManager = new CentroidManagerGeoWave<>(
				dataStorePluginOptions.createDataStore(),
				dataStorePluginOptions.createIndexStore(),
				dataStorePluginOptions.createAdapterStore(),
				new SimpleFeatureItemWrapperFactory(),
				"concave_hull",
				dataStorePluginOptions.createInternalAdapterStore().addTypeName(
						"concave_hull"),
				new SpatialDimensionalityTypeProvider().createIndex(
						new SpatialOptions()).getName(),
				"bx5",
				0);

		int count = 0;
		for (final String grp : centroidManager.getAllCentroidGroups()) {
			for (final AnalyticItemWrapper<SimpleFeature> feature : centroidManager.getCentroidsForGroup(grp)) {
				ShapefileTool.writeShape(
						feature.getName(),
						new File(
								"./target/test_final_" + feature.getName()),
						new Geometry[] {
							feature.getGeometry()
						});
				count++;

			}
		}
		return count;
	}

	private void ingest(
			final DataStore dataStore )
			throws IOException {
		final List<SimpleFeature> features = dataGenerator.generatePointSet(
				0.05,
				0.5,
				4,
				800,
				new double[] {
					-86,
					-30
				},
				new double[] {
					-90,
					-34
				});

		features.addAll(dataGenerator.generatePointSet(
				dataGenerator.getFactory().createLineString(
						new Coordinate[] {
							new Coordinate(
									-87,
									-32),
							new Coordinate(
									-87.5,
									-32.3),
							new Coordinate(
									-87.2,
									-32.7)
						}),
				0.2,
				500));

		ShapefileTool.writeShape(
				new File(
						"./target/test_in"),
				features);
		dataGenerator.writeToGeoWave(
				dataStore,
				features);
	}
}
