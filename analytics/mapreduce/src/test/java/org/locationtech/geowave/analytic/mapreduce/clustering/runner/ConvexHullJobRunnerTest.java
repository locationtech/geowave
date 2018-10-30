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
import org.locationtech.geowave.analytic.Projection;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.ScopedJobConfiguration;
import org.locationtech.geowave.analytic.SimpleFeatureProjection;
import org.locationtech.geowave.analytic.clustering.ClusteringUtils;
import org.locationtech.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import org.locationtech.geowave.analytic.mapreduce.MapReduceIntegration;
import org.locationtech.geowave.analytic.mapreduce.SequenceFileInputFormatConfiguration;
import org.locationtech.geowave.analytic.mapreduce.clustering.ConvexHullMapReduce;
import org.locationtech.geowave.analytic.param.CentroidParameters;
import org.locationtech.geowave.analytic.param.GlobalParameters;
import org.locationtech.geowave.analytic.param.HullParameters;
import org.locationtech.geowave.analytic.param.InputParameters;
import org.locationtech.geowave.analytic.param.MapReduceParameters.MRConfig;
import org.locationtech.geowave.analytic.param.ParameterHelper;
import org.locationtech.geowave.analytic.param.StoreParameters.StoreParam;
import org.locationtech.geowave.analytic.store.PersistableStore;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapterWrapper;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;
import org.opengis.feature.simple.SimpleFeatureType;

public class ConvexHullJobRunnerTest
{
	private final ConvexHullJobRunner hullRunner = new ConvexHullJobRunner();
	private final PropertyManagement runTimeProperties = new PropertyManagement();
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

		hullRunner.setMapReduceIntegrater(new MapReduceIntegration() {
			@Override
			public int submit(
					final Configuration configuration,
					final PropertyManagement runTimeProperties,
					final GeoWaveAnalyticJobRunner tool )
					throws Exception {
				tool.setConf(configuration);
				((ParameterHelper<Object>) StoreParam.INPUT_STORE.getHelper()).setValue(
						configuration,
						ConvexHullMapReduce.class,
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
						ConvexHullMapReduce.class);
				Assert.assertEquals(
						"file://foo/bin",
						job.getConfiguration().get(
								"mapred.input.dir"));
				final PersistableStore persistableStore = (PersistableStore) StoreParam.INPUT_STORE
						.getHelper()
						.getValue(
								job,
								ConvexHullMapReduce.class,
								null);
				final IndexStore indexStore = persistableStore.getDataStoreOptions().createIndexStore();
				try {
					Assert.assertTrue(indexStore.indexExists("spatial"));

					final PersistableStore persistableAdapterStore = (PersistableStore) StoreParam.INPUT_STORE
							.getHelper()
							.getValue(
									job,
									ConvexHullMapReduce.class,
									null);
					final PersistentAdapterStore adapterStore = persistableAdapterStore
							.getDataStoreOptions()
							.createAdapterStore();

					Assert.assertTrue(adapterStore.adapterExists(persistableAdapterStore
							.getDataStoreOptions()
							.createInternalAdapterStore()
							.getAdapterId(
									"centroidtest")));

					final Projection<?> projection = configWrapper.getInstance(
							HullParameters.Hull.PROJECTION_CLASS,
							Projection.class,
							SimpleFeatureProjection.class);

					Assert.assertEquals(
							SimpleFeatureProjection.class,
							projection.getClass());

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

				Assert.assertEquals(
						10,
						job.getNumReduceTasks());
				Assert.assertEquals(
						2,
						configWrapper.getInt(
								CentroidParameters.Centroid.ZOOM_LEVEL,
								-1));
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
		hullRunner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration());

		runTimeProperties.store(
				MRConfig.HDFS_BASE_DIR,
				"/");
		runTimeProperties.store(
				InputParameters.Input.HDFS_INPUT_PATH,
				new Path(
						"file://foo/bin"));
		runTimeProperties.store(
				GlobalParameters.Global.BATCH_ID,
				"b1234");
		runTimeProperties.store(
				HullParameters.Hull.DATA_TYPE_ID,
				"hullType");
		runTimeProperties.store(
				HullParameters.Hull.REDUCER_COUNT,
				10);
		runTimeProperties.store(
				HullParameters.Hull.INDEX_NAME,
				"spatial");

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

		hullRunner.run(runTimeProperties);
	}
}
