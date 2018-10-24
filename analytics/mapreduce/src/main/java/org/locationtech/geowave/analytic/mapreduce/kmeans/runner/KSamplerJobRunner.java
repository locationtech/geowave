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
package org.locationtech.geowave.analytic.mapreduce.kmeans.runner;

import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import org.locationtech.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import org.locationtech.geowave.analytic.mapreduce.GeoWaveOutputFormatConfiguration;
import org.locationtech.geowave.analytic.mapreduce.MapReduceJobRunner;
import org.locationtech.geowave.analytic.mapreduce.kmeans.KSamplerMapReduce;
import org.locationtech.geowave.analytic.param.CentroidParameters;
import org.locationtech.geowave.analytic.param.GlobalParameters;
import org.locationtech.geowave.analytic.param.ParameterEnum;
import org.locationtech.geowave.analytic.param.SampleParameters;
import org.locationtech.geowave.analytic.sample.function.RandomSamplingRankFunction;
import org.locationtech.geowave.analytic.sample.function.SamplingRankFunction;
import org.locationtech.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialTemporalOptions;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.adapter.AdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;

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

	private InternalDataAdapter<?> getAdapter(
			final PropertyManagement runTimeProperties )
			throws Exception {
		final PersistentAdapterStore adapterStore = super.getAdapterStore(runTimeProperties);

		final InternalAdapterStore internalAdapterStore = getInternalAdapterStore(runTimeProperties);
		Short sampleInternalAdapterId = internalAdapterStore.getAdapterId(runTimeProperties.getPropertyAsString(
				SampleParameters.Sample.DATA_TYPE_NAME,
				"sample"));
		if (sampleInternalAdapterId == null) {
			return null;
		}
		return adapterStore.getAdapter(sampleInternalAdapterId);
	}

	private Index getIndex(
			final PropertyManagement runTimeProperties )
			throws Exception {
		final IndexStore indexStore = super.getIndexStore(runTimeProperties);

		return (Index) indexStore.getIndex(runTimeProperties.getPropertyAsString(
				SampleParameters.Sample.INDEX_NAME,
				"index"));
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
				SampleParameters.Sample.DATA_TYPE_NAME,
				"sample");

		runTimeProperties.store(
				CentroidParameters.Centroid.ZOOM_LEVEL,
				zoomLevel);

		runTimeProperties.storeIfEmpty(
				SampleParameters.Sample.INDEX_NAME,
				new SpatialTemporalDimensionalityTypeProvider().createIndex(
						new SpatialTemporalOptions()).getName());
		runTimeProperties.setConfig(
				new ParameterEnum[] {
					GlobalParameters.Global.BATCH_ID,
					SampleParameters.Sample.INDEX_NAME,
					SampleParameters.Sample.SAMPLE_SIZE,
					SampleParameters.Sample.DATA_TYPE_NAME,
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
