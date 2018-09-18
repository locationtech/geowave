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
package org.locationtech.geowave.analytic.mapreduce.clustering.runner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.geotools.feature.type.BasicFeatureTypes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.analytic.AnalyticFeature;
import org.locationtech.geowave.analytic.AnalyticItemWrapperFactory;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.ScopedJobConfiguration;
import org.locationtech.geowave.analytic.SimpleFeatureItemWrapperFactory;
import org.locationtech.geowave.analytic.clustering.ClusteringUtils;
import org.locationtech.geowave.analytic.distance.DistanceFn;
import org.locationtech.geowave.analytic.distance.FeatureCentroidDistanceFn;
import org.locationtech.geowave.analytic.distance.GeometryCentroidDistanceFn;
import org.locationtech.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import org.locationtech.geowave.analytic.mapreduce.MapReduceIntegration;
import org.locationtech.geowave.analytic.mapreduce.SequenceFileInputFormatConfiguration;
import org.locationtech.geowave.analytic.mapreduce.clustering.GroupAssignmentMapReduce;
import org.locationtech.geowave.analytic.param.CentroidParameters;
import org.locationtech.geowave.analytic.param.CommonParameters;
import org.locationtech.geowave.analytic.param.GlobalParameters;
import org.locationtech.geowave.analytic.param.MapReduceParameters.MRConfig;
import org.locationtech.geowave.analytic.param.ParameterHelper;
import org.locationtech.geowave.analytic.param.StoreParameters.StoreParam;
import org.locationtech.geowave.analytic.store.PersistableStore;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapterWrapper;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;
import org.opengis.feature.simple.SimpleFeatureType;

public class GroupAssigmentJobRunnerTest
{

	final GroupAssigmentJobRunner runner = new GroupAssigmentJobRunner();
	final PropertyManagement runTimeProperties = new PropertyManagement();
	@Rule
	public TestName name = new TestName();

	@Before
	public void init() {
		final SimpleFeatureType ftype = AnalyticFeature.createGeometryFeatureAdapter(
				"centroidtest",
				new String[] {
					"extra1"
				},
				BasicFeatureTypes.DEFAULT_NAMESPACE,
				ClusteringUtils.CLUSTERING_CRS).getFeatureType();

		runner.setMapReduceIntegrater(new MapReduceIntegration() {
			@Override
			public int submit(
					final Configuration configuration,
					final PropertyManagement runTimeProperties,
					final GeoWaveAnalyticJobRunner tool )
					throws Exception {
				tool.setConf(configuration);
				((ParameterHelper<Object>) StoreParam.INPUT_STORE.getHelper()).setValue(
						configuration,
						GroupAssignmentMapReduce.class,
						StoreParam.INPUT_STORE.getHelper().getValue(
								runTimeProperties));
				return tool.run(new String[] {});
			}

			@Override
			public Counters waitForCompletion(
					final Job job )
					throws ClassNotFoundException,
					IOException,
					InterruptedException {

				Assert.assertEquals(
						SequenceFileInputFormat.class,
						job.getInputFormatClass());
				Assert.assertEquals(
						10,
						job.getNumReduceTasks());
				final ScopedJobConfiguration configWrapper = new ScopedJobConfiguration(
						job.getConfiguration(),
						GroupAssignmentMapReduce.class);
				Assert.assertEquals(
						"file://foo/bin",
						job.getConfiguration().get(
								"mapred.input.dir"));

				Assert.assertEquals(
						3,
						configWrapper.getInt(
								CentroidParameters.Centroid.ZOOM_LEVEL,
								-1));
				Assert.assertEquals(
						"b1234",
						configWrapper.getString(
								GlobalParameters.Global.PARENT_BATCH_ID,
								""));
				Assert.assertEquals(
						"b12345",
						configWrapper.getString(
								GlobalParameters.Global.BATCH_ID,
								""));

				try {
					final AnalyticItemWrapperFactory<?> wrapper = configWrapper.getInstance(
							CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
							AnalyticItemWrapperFactory.class,
							SimpleFeatureItemWrapperFactory.class);

					Assert.assertEquals(
							SimpleFeatureItemWrapperFactory.class,
							wrapper.getClass());

					final DistanceFn<?> distancFn = configWrapper.getInstance(
							CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
							DistanceFn.class,
							GeometryCentroidDistanceFn.class);

					Assert.assertEquals(
							FeatureCentroidDistanceFn.class,
							distancFn.getClass());

				}
				catch (final InstantiationException e) {
					throw new IOException(
							"Unable to configure system",
							e);
				}
				catch (final IllegalAccessException e) {
					throw new IOException(
							"Unable to configure system",
							e);
				}

				return new Counters();
			}

			@Override
			public Job getJob(
					final Tool tool )
					throws IOException {
				return new Job(
						tool.getConf());
			}

			@Override
			public Configuration getConfiguration(
					final PropertyManagement runTimeProperties )
					throws IOException {
				return new Configuration();
			}
		});
		runner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
				new Path(
						"file://foo/bin")));
		runner.setZoomLevel(3);
		runner.setReducerCount(10);

		runTimeProperties.store(
				MRConfig.HDFS_BASE_DIR,
				"/");

		runTimeProperties.store(
				GlobalParameters.Global.BATCH_ID,
				"b12345");
		runTimeProperties.store(
				GlobalParameters.Global.PARENT_BATCH_ID,
				"b1234");

		runTimeProperties.store(
				CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
				FeatureCentroidDistanceFn.class);

		final DataStorePluginOptions pluginOptions = new DataStorePluginOptions();
		GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().put(
				"memory",
				new MemoryStoreFactoryFamily());
		pluginOptions.selectPlugin("memory");
		final MemoryRequiredOptions opts = (MemoryRequiredOptions) pluginOptions.getFactoryOptions();
		final String namespace = "test_" + getClass().getName() + "_" + name.getMethodName();
		opts.setGeowaveNamespace(namespace);
		final PersistableStore store = new PersistableStore(
				pluginOptions);

		runTimeProperties.store(
				StoreParam.INPUT_STORE,
				store);

		final FeatureDataAdapter adapter = new FeatureDataAdapter(
				ftype);
		final Index index = new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());
		adapter.init(index);
		pluginOptions.createAdapterStore().addAdapter(
				new InternalDataAdapterWrapper<>(
						adapter,
						pluginOptions.createInternalAdapterStore().addTypeName(
								adapter.getTypeName())));
	}

	@Test
	public void test()
			throws Exception {

		runner.run(runTimeProperties);
	}
}
