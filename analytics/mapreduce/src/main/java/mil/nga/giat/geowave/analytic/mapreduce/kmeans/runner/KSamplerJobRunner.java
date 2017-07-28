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
package mil.nga.giat.geowave.analytic.mapreduce.kmeans.runner;

import java.util.UUID;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveOutputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.kmeans.KSamplerMapReduce;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.SampleParameters;
import mil.nga.giat.geowave.analytic.sample.function.RandomSamplingRankFunction;
import mil.nga.giat.geowave.analytic.sample.function.SamplingRankFunction;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;

/**
 * 
 * Samples 'K' number of data items by evaluating a {@link SamplingRankFunction}
 * 
 * For KMeans Parallel, the initial step requires seeding the centroids with a
 * single point. In this case, K=1 and the rank function is random. This means
 * the top selected geometry is random. In addition, each subsequent iteration
 * samples based on probability function and K is some provided sample size.
 * 
 * 
 */
public class KSamplerJobRunner extends
		GeoWaveAnalyticJobRunner implements
		MapReduceJobRunner
{
	protected int zoomLevel = 1;
	private Class<? extends SamplingRankFunction> samplingRankFunctionClass = RandomSamplingRankFunction.class;

	public KSamplerJobRunner() {
		super.setOutputFormatConfiguration(new GeoWaveOutputFormatConfiguration());
	}

	public void setSamplingRankFunctionClass(
			final Class<? extends SamplingRankFunction> samplingRankFunctionClass ) {
		this.samplingRankFunctionClass = samplingRankFunctionClass;
	}

	public void setZoomLevel(
			final int zoomLevel ) {
		this.zoomLevel = zoomLevel;
	}

	@Override
	public Class<?> getScope() {
		return KSamplerMapReduce.class;
	}

	@Override
	public void configure(
			final Job job )
			throws Exception {
		job.setMapperClass(KSamplerMapReduce.SampleMap.class);
		job.setMapOutputKeyClass(GeoWaveInputKey.class);
		job.setMapOutputValueClass(ObjectWritable.class);
		job.setReducerClass(KSamplerMapReduce.SampleReducer.class);
		job.setPartitionerClass(KSamplerMapReduce.SampleKeyPartitioner.class);
		job.setReduceSpeculativeExecution(false);
		job.setOutputKeyClass(GeoWaveOutputKey.class);
		job.setOutputValueClass(Object.class);
	}

	private DataAdapter<?> getAdapter(
			final PropertyManagement runTimeProperties )
			throws Exception {
		final AdapterStore adapterStore = super.getAdapterStore(runTimeProperties);

		return adapterStore.getAdapter(new ByteArrayId(
				runTimeProperties.getPropertyAsString(
						SampleParameters.Sample.DATA_TYPE_ID,
						"sample")));
	}

	private PrimaryIndex getIndex(
			final PropertyManagement runTimeProperties )
			throws Exception {
		final IndexStore indexStore = super.getIndexStore(runTimeProperties);

		return (PrimaryIndex) indexStore.getIndex(new ByteArrayId(
				runTimeProperties.getPropertyAsString(
						SampleParameters.Sample.INDEX_ID,
						"index")));
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		runTimeProperties.storeIfEmpty(
				GlobalParameters.Global.BATCH_ID,
				UUID.randomUUID().toString());

		runTimeProperties.storeIfEmpty(
				SampleParameters.Sample.DATA_TYPE_ID,
				"sample");

		runTimeProperties.store(
				CentroidParameters.Centroid.ZOOM_LEVEL,
				zoomLevel);

		runTimeProperties.storeIfEmpty(
				SampleParameters.Sample.INDEX_ID,
				new SpatialTemporalDimensionalityTypeProvider().createPrimaryIndex(
						new SpatialTemporalOptions()).getId());
		runTimeProperties.setConfig(
				new ParameterEnum[] {
					GlobalParameters.Global.BATCH_ID,
					SampleParameters.Sample.INDEX_ID,
					SampleParameters.Sample.SAMPLE_SIZE,
					SampleParameters.Sample.DATA_TYPE_ID,
					CentroidParameters.Centroid.EXTRACTOR_CLASS,
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
					CentroidParameters.Centroid.ZOOM_LEVEL
				},
				config,
				getScope());

		((ParameterEnum<Class<?>>) SampleParameters.Sample.SAMPLE_RANK_FUNCTION).getHelper().setValue(
				config,
				getScope(),
				samplingRankFunctionClass);

		NestedGroupCentroidAssignment.setParameters(
				config,
				getScope(),
				runTimeProperties);

		addDataAdapter(
				config,
				getAdapter(runTimeProperties));
		addIndex(
				config,
				getIndex(runTimeProperties));

		super.setReducerCount(zoomLevel);
		// HP Fortify "Command Injection" false positive
		// What Fortify considers "externally-influenced input"
		// comes only from users with OS-level access anyway
		return super.run(
				config,
				runTimeProperties);

	}

	@Override
	protected String getJobName() {
		return "K-Sampler";
	}
}
