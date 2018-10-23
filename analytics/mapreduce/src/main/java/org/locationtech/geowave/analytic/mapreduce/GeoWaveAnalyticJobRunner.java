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
package org.locationtech.geowave.analytic.mapreduce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.util.Tool;
import org.geotools.feature.type.BasicFeatureTypes;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.analytic.AnalyticFeature;
import org.locationtech.geowave.analytic.IndependentJobRunner;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.ScopedJobConfiguration;
import org.locationtech.geowave.analytic.clustering.ClusteringUtils;
import org.locationtech.geowave.analytic.param.FormatConfiguration;
import org.locationtech.geowave.analytic.param.InputParameters;
import org.locationtech.geowave.analytic.param.OutputParameters;
import org.locationtech.geowave.analytic.param.OutputParameters.Output;
import org.locationtech.geowave.analytic.param.ParameterEnum;
import org.locationtech.geowave.analytic.param.StoreParameters;
import org.locationtech.geowave.analytic.param.StoreParameters.StoreParam;
import org.locationtech.geowave.analytic.store.PersistableStore;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapterWrapper;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.CustomNameIndex;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.mapreduce.JobContextAdapterStore;
import org.locationtech.geowave.mapreduce.JobContextIndexStore;
import org.locationtech.geowave.mapreduce.JobContextInternalAdapterStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class managers the input and output formats for a map reduce job. It
 * also controls job submission, isolating some of the job management
 * responsibilities. One key benefit is support of unit testing for job runner
 * instances.
 */
public abstract class GeoWaveAnalyticJobRunner extends
		Configured implements
		Tool,
		MapReduceJobRunner,
		IndependentJobRunner
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveAnalyticJobRunner.class);

	private FormatConfiguration inputFormat = null;
	private FormatConfiguration outputFormat = null;
	private int reducerCount = 1;
	private MapReduceIntegration mapReduceIntegrater = new ToolRunnerMapReduceIntegration();
	private Counters lastCounterSet = null;

	public FormatConfiguration getInputFormatConfiguration() {
		return inputFormat;
	}

	public void setInputFormatConfiguration(
			final FormatConfiguration inputFormat ) {
		this.inputFormat = inputFormat;
	}

	public FormatConfiguration getOutputFormatConfiguration() {
		return outputFormat;
	}

	public void setOutputFormatConfiguration(
			final FormatConfiguration outputFormat ) {
		this.outputFormat = outputFormat;
	}

	public MapReduceIntegration getMapReduceIntegrater() {
		return mapReduceIntegrater;
	}

	public void setMapReduceIntegrater(
			final MapReduceIntegration mapReduceIntegrater ) {
		this.mapReduceIntegrater = mapReduceIntegrater;
	}

	public int getReducerCount() {
		return reducerCount;
	}

	public void setReducerCount(
			final int reducerCount ) {
		this.reducerCount = reducerCount;
	}

	public GeoWaveAnalyticJobRunner() {}

	protected static Logger getLogger() {
		return LOGGER;
	}

	public Class<?> getScope() {
		return this.getClass();
	}

	public PersistentAdapterStore getAdapterStore(
			final PropertyManagement runTimeProperties )
			throws Exception {
		final PersistableStore store = (PersistableStore) StoreParameters.StoreParam.INPUT_STORE.getHelper().getValue(
				runTimeProperties);
		return store.getDataStoreOptions().createAdapterStore();
	}

	public InternalAdapterStore getInternalAdapterStore(
			final PropertyManagement runTimeProperties )
			throws Exception {
		final PersistableStore store = (PersistableStore) StoreParameters.StoreParam.INPUT_STORE.getHelper().getValue(
				runTimeProperties);
		return store.getDataStoreOptions().createInternalAdapterStore();
	}

	public IndexStore getIndexStore(
			final PropertyManagement runTimeProperties )
			throws Exception {
		final PersistableStore store = (PersistableStore) StoreParameters.StoreParam.INPUT_STORE.getHelper().getValue(
				runTimeProperties);
		return store.getDataStoreOptions().createIndexStore();
	}

	@Override
	public int run(
			final Configuration configuration,
			final PropertyManagement runTimeProperties )
			throws Exception {

		if ((inputFormat == null) && runTimeProperties.hasProperty(InputParameters.Input.INPUT_FORMAT)) {
			inputFormat = runTimeProperties.getClassInstance(
					InputParameters.Input.INPUT_FORMAT,
					FormatConfiguration.class,
					null);
		}
		if (inputFormat != null) {
			InputParameters.Input.INPUT_FORMAT.getHelper().setValue(
					configuration,
					getScope(),
					inputFormat.getClass());
			inputFormat.setup(
					runTimeProperties,
					configuration);
		}
		if ((outputFormat == null) && runTimeProperties.hasProperty(OutputParameters.Output.OUTPUT_FORMAT)) {
			outputFormat = runTimeProperties.getClassInstance(
					OutputParameters.Output.OUTPUT_FORMAT,
					FormatConfiguration.class,
					null);
		}

		if (outputFormat != null) {
			OutputParameters.Output.OUTPUT_FORMAT.getHelper().setValue(
					configuration,
					getScope(),
					outputFormat.getClass());
			outputFormat.setup(
					runTimeProperties,
					configuration);
		}

		runTimeProperties.setConfig(
				new ParameterEnum[] {
					StoreParam.INPUT_STORE
				},
				configuration,
				getScope());

		OutputParameters.Output.REDUCER_COUNT.getHelper().setValue(
				configuration,
				getScope(),
				runTimeProperties.getPropertyAsInt(
						OutputParameters.Output.REDUCER_COUNT,
						reducerCount));
		return mapReduceIntegrater.submit(
				configuration,
				runTimeProperties,
				this);
	}

	public static void addDataAdapter(
			final Configuration config,
			final InternalDataAdapter<?> adapter ) {
		JobContextAdapterStore.addDataAdapter(
				config,
				adapter.getAdapter());
		JobContextInternalAdapterStore.addTypeName(
				config,
				adapter.getTypeName(),
				adapter.getAdapterId());
	}

	public static void addIndex(
			final Configuration config,
			final Index index ) {
		JobContextIndexStore.addIndex(
				config,
				index);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int run(
			final String[] args )
			throws Exception {
		final Job job = mapReduceIntegrater.getJob(this);

		configure(job);

		final ScopedJobConfiguration configWrapper = new ScopedJobConfiguration(
				job.getConfiguration(),
				getScope());

		final FormatConfiguration inputFormat = configWrapper.getInstance(
				InputParameters.Input.INPUT_FORMAT,
				FormatConfiguration.class,
				null);

		if (inputFormat != null) {
			job.setInputFormatClass((Class<? extends InputFormat>) inputFormat.getFormatClass());
		}

		final FormatConfiguration outputFormat = configWrapper.getInstance(
				OutputParameters.Output.OUTPUT_FORMAT,
				FormatConfiguration.class,
				null);

		if (outputFormat != null) {
			job.setOutputFormatClass((Class<? extends OutputFormat>) outputFormat.getFormatClass());
		}

		job.setNumReduceTasks(configWrapper.getInt(
				OutputParameters.Output.REDUCER_COUNT,
				1));

		job.setJobName(getJobName());

		job.setJarByClass(this.getClass());
		final Counters counters = mapReduceIntegrater.waitForCompletion(job);
		lastCounterSet = counters;
		return (counters == null) ? 1 : 0;
	}

	abstract protected String getJobName();

	public long getCounterValue(
			final Enum<?> counterEnum ) {
		return (lastCounterSet != null) ? (lastCounterSet.findCounter(counterEnum)).getValue() : 0;
	}

	public abstract void configure(
			final Job job )
			throws Exception;

	@Override
	public Collection<ParameterEnum<?>> getParameters() {
		final List<ParameterEnum<?>> params = new ArrayList<>();
		if (inputFormat != null) {
			params.addAll(inputFormat.getParameters());
		}
		if (outputFormat != null) {
			params.addAll(outputFormat.getParameters());
		}
		params.addAll(Arrays.asList(new ParameterEnum<?>[] {
			StoreParam.INPUT_STORE,
			Output.REDUCER_COUNT,
			Output.OUTPUT_FORMAT
		}));
		return params;
	}

	@Override
	public int run(
			final PropertyManagement runTimeProperties )
			throws Exception {
		return this.run(
				mapReduceIntegrater.getConfiguration(runTimeProperties),
				runTimeProperties);
	}

	protected InternalDataAdapter<?> getAdapter(
			final PropertyManagement runTimeProperties,
			final ParameterEnum dataTypeEnum,
			final ParameterEnum dataNameSpaceEnum )
			throws Exception {

		final String projectionDataTypeId = runTimeProperties.storeIfEmpty(
				dataTypeEnum,
				"convex_hull").toString();

		final PersistentAdapterStore adapterStore = getAdapterStore(runTimeProperties);
		final InternalAdapterStore internalAdapterStore = getInternalAdapterStore(runTimeProperties);
		final Short convexHullInternalAdapterId = internalAdapterStore.getAdapterId(projectionDataTypeId);
		if (convexHullInternalAdapterId == null) {
			final String namespaceURI = runTimeProperties.storeIfEmpty(
					dataNameSpaceEnum,
					BasicFeatureTypes.DEFAULT_NAMESPACE).toString();
			final FeatureDataAdapter adapter = AnalyticFeature.createGeometryFeatureAdapter(
					projectionDataTypeId,
					new String[0],
					namespaceURI,
					ClusteringUtils.CLUSTERING_CRS);
			final short internalAdapterId = internalAdapterStore.addTypeName(adapter.getTypeName());
			final InternalDataAdapter<?> internalAdapter = new InternalDataAdapterWrapper<>(
					adapter,
					internalAdapterId);
			adapterStore.addAdapter(internalAdapter);
			return internalAdapter;
		}
		return adapterStore.getAdapter(convexHullInternalAdapterId);
	}

	protected String checkIndex(
			final PropertyManagement runTimeProperties,
			final ParameterEnum indexIdEnum,
			final String defaultIdxName )
			throws Exception {

		final String indexName = runTimeProperties.getPropertyAsString(
				indexIdEnum,
				defaultIdxName);

		final IndexStore indexStore = getIndexStore(runTimeProperties);

		Index index = indexStore.getIndex(indexName);
		if (index == null) {
			final Index defaultSpatialIndex = new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());
			index = new CustomNameIndex(
					defaultSpatialIndex.getIndexStrategy(),
					defaultSpatialIndex.getIndexModel(),
					indexName);
			indexStore.addIndex(index);
		}
		return indexName;
	}

}
