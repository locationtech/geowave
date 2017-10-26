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
package mil.nga.giat.geowave.analytic.mapreduce.clustering.runner;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.IndependentJobRunner;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.ScopedJobConfiguration;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.extract.DimensionExtractor;
import mil.nga.giat.geowave.analytic.extract.SimpleFeatureGeometryExtractor;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobController;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.SimpleFeatureOutputReducer;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.StoreParameters;
import mil.nga.giat.geowave.analytic.param.StoreParameters.StoreParam;
import mil.nga.giat.geowave.analytic.store.PersistableStore;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.dedupe.GeoWaveDedupeJobRunner;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;

/**
 * 
 * Run a map reduce job to extract a population of data from GeoWave (Accumulo),
 * remove duplicates, and output a SimpleFeature with the ID and the extracted
 * geometry from each of the GeoWave data item.
 * 
 */
public class GeoWaveAnalyticExtractJobRunner extends
		GeoWaveDedupeJobRunner implements
		MapReduceJobRunner,
		IndependentJobRunner
{

	private String outputBaseDir = "/tmp";
	private int reducerCount = 1;

	public GeoWaveAnalyticExtractJobRunner() {
		super(
				null); // Datastore options are set in configure()
	}

	@Override
	protected int getNumReduceTasks() {
		return reducerCount;
	}

	@Override
	protected String getHdfsOutputBase() {
		return outputBaseDir;
	}

	@Override
	protected void configure(
			final Job job )
			throws Exception {

		final ScopedJobConfiguration configWrapper = new ScopedJobConfiguration(
				job.getConfiguration(),
				SimpleFeatureOutputReducer.class);

		reducerCount = Math.max(
				configWrapper.getInt(
						ExtractParameters.Extract.REDUCER_COUNT,
						8),
				1);

		outputBaseDir = configWrapper.getString(
				MapReduceParameters.MRConfig.HDFS_BASE_DIR,
				"/tmp");

		LOGGER.info("Output base directory " + outputBaseDir);

		super.configure(job);

		@SuppressWarnings("rawtypes")
		final Class<? extends DimensionExtractor> dimensionExtractorClass = job.getConfiguration().getClass(
				GeoWaveConfiguratorBase.enumToConfKey(
						SimpleFeatureOutputReducer.class,
						ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS),
				SimpleFeatureGeometryExtractor.class,
				DimensionExtractor.class);

		GeoWaveOutputFormat.addDataAdapter(
				job.getConfiguration(),
				createAdapter(
						job.getConfiguration().get(
								GeoWaveConfiguratorBase.enumToConfKey(
										SimpleFeatureOutputReducer.class,
										ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID)),
						job.getConfiguration().get(
								GeoWaveConfiguratorBase.enumToConfKey(
										SimpleFeatureOutputReducer.class,
										ExtractParameters.Extract.DATA_NAMESPACE_URI)),
						dimensionExtractorClass));

		job.setJobName("GeoWave Extract (" + dataStoreOptions.getGeowaveNamespace() + ")");
		job.setReduceSpeculativeExecution(false);

	}

	private FeatureDataAdapter createAdapter(
			final String outputDataTypeID,
			final String namespaceURI,
			@SuppressWarnings("rawtypes")
			final Class<? extends DimensionExtractor> dimensionExtractorClass )
			throws InstantiationException,
			IllegalAccessException {
		final DimensionExtractor<?> extractor = dimensionExtractorClass.newInstance();
		return AnalyticFeature.createGeometryFeatureAdapter(
				outputDataTypeID,
				extractor.getDimensionNames(),
				namespaceURI,
				ClusteringUtils.CLUSTERING_CRS);
	}

	@Override
	public Path getHdfsOutputPath() {
		return new Path(
				getHdfsOutputBase() + "/" + dataStoreOptions.getGeowaveNamespace() + "_dedupe");
	}

	@Override
	@SuppressWarnings("rawtypes")
	protected Class<? extends Reducer> getReducer() {
		return SimpleFeatureOutputReducer.class;
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		runTimeProperties.storeIfEmpty(
				ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID,
				"centroid");
		runTimeProperties.setConfig(
				new ParameterEnum[] {
					MapReduceParameters.MRConfig.HDFS_BASE_DIR,
					ExtractParameters.Extract.REDUCER_COUNT,
					ExtractParameters.Extract.DATA_NAMESPACE_URI,
					ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID
				},
				config,
				SimpleFeatureOutputReducer.class);

		config.set(
				GeoWaveConfiguratorBase.enumToConfKey(
						SimpleFeatureOutputReducer.class,
						ExtractParameters.Extract.GROUP_ID),
				runTimeProperties.getPropertyAsString(
						ExtractParameters.Extract.GROUP_ID,
						UUID.randomUUID().toString()));

		config.set(
				GeoWaveConfiguratorBase.enumToConfKey(
						SimpleFeatureOutputReducer.class,
						GlobalParameters.Global.BATCH_ID),
				runTimeProperties.getPropertyAsString(
						GlobalParameters.Global.BATCH_ID,
						UUID.randomUUID().toString()));

		DistributableQuery myQuery = query;
		if (myQuery == null) {
			myQuery = runTimeProperties.getPropertyAsQuery(ExtractParameters.Extract.QUERY);
		}
		setMinInputSplits(runTimeProperties.getPropertyAsInt(
				ExtractParameters.Extract.MIN_INPUT_SPLIT,
				1));
		setMaxInputSplits(runTimeProperties.getPropertyAsInt(
				ExtractParameters.Extract.MAX_INPUT_SPLIT,
				10000));
		if (myQuery != null) {
			GeoWaveInputFormat.setQuery(
					config,
					query);
		}
		if (minInputSplits != null) {
			GeoWaveInputFormat.setMinimumSplitCount(
					config,
					minInputSplits);
		}
		if (maxInputSplits != null) {
			GeoWaveInputFormat.setMaximumSplitCount(
					config,
					maxInputSplits);
		}

		setConf(config);

		config.setClass(
				GeoWaveConfiguratorBase.enumToConfKey(
						SimpleFeatureOutputReducer.class,
						ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS),
				runTimeProperties.getPropertyAsClass(
						ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS,
						DimensionExtractor.class,
						SimpleFeatureGeometryExtractor.class),
				DimensionExtractor.class);

		final PersistableStore store = ((PersistableStore) runTimeProperties.getProperty(StoreParam.INPUT_STORE));
		setQueryOptions(runTimeProperties.getPropertyAsQueryOptions(ExtractParameters.Extract.QUERY_OPTIONS));
		dataStoreOptions = store.getDataStoreOptions();

		GeoWaveInputFormat.setStoreOptions(
				config,
				dataStoreOptions);

		GeoWaveOutputFormat.setStoreOptions(
				config,
				dataStoreOptions);

		try (final FileSystem fs = FileSystem.get(config)) {
			if (fs.exists(this.getHdfsOutputPath())) {
				fs.delete(
						// HPFortify "Path Manipulation"
						// False positive - path is internally managed
						getHdfsOutputPath(),
						true);
			}

			return ToolRunner.run(
					config,
					this,
					new String[] {});
		}
	}

	@Override
	public Collection<ParameterEnum<?>> getParameters() {
		final Set<ParameterEnum<?>> params = new HashSet<ParameterEnum<?>>();
		params.addAll(Arrays.asList(new ParameterEnum<?>[] {
			ExtractParameters.Extract.REDUCER_COUNT,
			ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID,
			ExtractParameters.Extract.DATA_NAMESPACE_URI,
			ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS,
			ExtractParameters.Extract.MIN_INPUT_SPLIT,
			ExtractParameters.Extract.MAX_INPUT_SPLIT,
			ExtractParameters.Extract.QUERY,
			ExtractParameters.Extract.QUERY_OPTIONS,
			StoreParam.INPUT_STORE,
			GlobalParameters.Global.BATCH_ID
		}));

		params.addAll(MapReduceParameters.getParameters());
		return params;
	}

	@Override
	public int run(
			final PropertyManagement runTimeProperties )
			throws Exception {
		return this.run(
				MapReduceJobController.getConfiguration(runTimeProperties),
				runTimeProperties);
	}

	@Override
	public boolean runOperation(
			final String[] args )
			throws ParseException {

		try {
			final Job job = new Job(
					super.getConf());
			job.setJarByClass(this.getClass());
			configure(job);
			return job.waitForCompletion(true);
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to run job",
					e);
			throw new ParseException(
					e.getMessage());
		}
	}

}
