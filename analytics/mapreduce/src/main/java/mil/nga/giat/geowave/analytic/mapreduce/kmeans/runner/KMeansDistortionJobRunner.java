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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.clustering.DistortionGroupManagement.DistortionDataAdapter;
import mil.nga.giat.geowave.analytic.clustering.DistortionGroupManagement.DistortionEntry;
import mil.nga.giat.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytic.mapreduce.CountofDoubleWritable;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.kmeans.KMeansDistortionMapReduce;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.JumpParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

/**
 * 
 * Calculate the distortation.
 * 
 * See Catherine A. Sugar and Gareth M. James (2003).
 * "Finding the number of clusters in a data set: An information theoretic approach"
 * Journal of the American Statistical Association 98 (January): 750–763
 * 
 * 
 */
public class KMeansDistortionJobRunner extends
		GeoWaveAnalyticJobRunner
{
	private int k = 1;
	private DataStorePluginOptions dataStoreOptions;

	public KMeansDistortionJobRunner() {
		setReducerCount(8);
	}

	public void setDataStoreOptions(
			final DataStorePluginOptions dataStoreOptions ) {
		this.dataStoreOptions = dataStoreOptions;
	}

	public void setCentroidsCount(
			final int k ) {
		this.k = k;
	}

	@Override
	public void configure(
			final Job job )
			throws Exception {

		job.setMapperClass(KMeansDistortionMapReduce.KMeansDistortionMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CountofDoubleWritable.class);
		job.setReducerClass(KMeansDistortionMapReduce.KMeansDistortionReduce.class);
		job.setCombinerClass(KMeansDistortionMapReduce.KMeansDistorationCombiner.class);
		job.setOutputKeyClass(GeoWaveOutputKey.class);
		job.setOutputValueClass(DistortionEntry.class);
		job.setOutputFormatClass(GeoWaveOutputFormat.class);
		// extends wait time to 15 minutes (default: 600 seconds)
		final long milliSeconds = 1000L * 60L * 15L;
		final Configuration conf = job.getConfiguration();
		conf.setLong(
				"mapred.task.timeout",
				milliSeconds);
		((ParameterEnum<Integer>) JumpParameters.Jump.COUNT_OF_CENTROIDS).getHelper().setValue(
				conf,
				KMeansDistortionMapReduce.class,
				Integer.valueOf(k));

		// Required since the Mapper uses the input format parameters to lookup
		// the adapter
		GeoWaveInputFormat.setStoreOptions(
				conf,
				dataStoreOptions);

		GeoWaveOutputFormat.addDataAdapter(
				conf,
				new DistortionDataAdapter());
	}

	@Override
	public Class<?> getScope() {
		return KMeansDistortionMapReduce.class;
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {
		setReducerCount(runTimeProperties.getPropertyAsInt(
				ClusteringParameters.Clustering.MAX_REDUCER_COUNT,
				super.getReducerCount()));
		runTimeProperties.setConfig(
				new ParameterEnum[] {
					CentroidParameters.Centroid.EXTRACTOR_CLASS,
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
					GlobalParameters.Global.PARENT_BATCH_ID
				},
				config,
				getScope());

		NestedGroupCentroidAssignment.setParameters(
				config,
				getScope(),
				runTimeProperties);

		// HP Fortify "Command Injection" false positive
		// What Fortify considers "externally-influenced input"
		// comes only from users with OS-level access anyway
		return super.run(
				config,
				runTimeProperties);
	}

	@Override
	protected String getJobName() {
		return "K-Means Distortion";
	}
}
