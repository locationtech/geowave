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

import mil.nga.giat.geowave.analytic.IndependentJobRunner;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileOutputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.InputToOutputKeyReducer;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * Run a map reduce job to extract a population of data from GeoWave (Accumulo),
 * remove duplicates, and output a SimpleFeature with the ID and the extracted
 * geometry from each of the GeoWave data item.
 * 
 */
public class GeoWaveInputLoadJobRunner extends
		GeoWaveAnalyticJobRunner implements
		MapReduceJobRunner,
		IndependentJobRunner
{
	public GeoWaveInputLoadJobRunner() {
		// defaults
		super.setInputFormatConfiguration(new GeoWaveInputFormatConfiguration());
		super.setOutputFormatConfiguration(new SequenceFileOutputFormatConfiguration());
	}

	@Override
	public void configure(
			final Job job )
			throws Exception {

		job.setMapperClass(Mapper.class);
		job.setReducerClass(InputToOutputKeyReducer.class);
		job.setMapOutputKeyClass(GeoWaveInputKey.class);
		job.setMapOutputValueClass(ObjectWritable.class);
		job.setOutputKeyClass(GeoWaveOutputKey.class);
		job.setOutputValueClass(Object.class);
		job.setSpeculativeExecution(false);

		job.setJobName("GeoWave Input to Output");
		job.setReduceSpeculativeExecution(false);

	}

	@Override
	public Class<?> getScope() {
		return InputToOutputKeyReducer.class;
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {
		final String indexId = checkIndex(
				runTimeProperties,
				OutputParameters.Output.INDEX_ID,
				runTimeProperties.getPropertyAsString(
						CentroidParameters.Centroid.INDEX_ID,
						new SpatialDimensionalityTypeProvider().createPrimaryIndex(
								new SpatialOptions()).getId().getString()));
		OutputParameters.Output.INDEX_ID.getHelper().setValue(
				config,
				getScope(),
				indexId);

		addDataAdapter(
				config,
				getAdapter(
						runTimeProperties,
						OutputParameters.Output.DATA_TYPE_ID,
						OutputParameters.Output.DATA_NAMESPACE_URI));
		runTimeProperties.setConfig(
				new ParameterEnum[] {
					OutputParameters.Output.DATA_TYPE_ID,
					OutputParameters.Output.DATA_NAMESPACE_URI,
					OutputParameters.Output.INDEX_ID
				},
				config,
				getScope());
		// HP Fortify "Command Injection" false positive
		// What Fortify considers "externally-influenced input"
		// comes only from users with OS-level access anyway
		return super.run(
				config,
				runTimeProperties);
	}

	@Override
	public Collection<ParameterEnum<?>> getParameters() {
		final Collection<ParameterEnum<?>> params = super.getParameters();
		params.addAll(Arrays.asList(new OutputParameters.Output[] {
			OutputParameters.Output.INDEX_ID,
			OutputParameters.Output.DATA_TYPE_ID,
			OutputParameters.Output.DATA_NAMESPACE_URI
		}));
		params.addAll(MapReduceParameters.getParameters());
		return params;
	}

	@Override
	protected String getJobName() {
		return "Input Load";
	}
}
