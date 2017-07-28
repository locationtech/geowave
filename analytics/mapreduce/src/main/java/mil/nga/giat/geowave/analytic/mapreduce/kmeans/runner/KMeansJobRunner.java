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

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveOutputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.GroupIDText;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.kmeans.KMeansMapReduce;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.opengis.feature.simple.SimpleFeature;

/**
 * 
 * Run 'K' means one time to move the centroids towards the mean.
 * 
 * 
 */
public class KMeansJobRunner extends
		GeoWaveAnalyticJobRunner implements
		MapReduceJobRunner
{

	public KMeansJobRunner() {
		super.setOutputFormatConfiguration(new GeoWaveOutputFormatConfiguration());
	}

	@Override
	public void setReducerCount(
			final int reducerCount ) {
		super.setReducerCount(Math.min(
				2,
				reducerCount));
	}

	@Override
	public void configure(
			final Job job )
			throws Exception {
		job.setMapperClass(KMeansMapReduce.KMeansMapper.class);
		job.setMapOutputKeyClass(GroupIDText.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setReducerClass(KMeansMapReduce.KMeansReduce.class);
		job.setCombinerClass(KMeansMapReduce.KMeansCombiner.class);
		job.setReduceSpeculativeExecution(false);
		job.setOutputKeyClass(GeoWaveOutputKey.class);
		job.setOutputValueClass(SimpleFeature.class);
	}

	@Override
	public Class<?> getScope() {
		return KMeansMapReduce.class;
	}

	@Override
	public int run(
			final Configuration configuration,
			final PropertyManagement runTimeProperties )
			throws Exception {
		NestedGroupCentroidAssignment.setParameters(
				configuration,
				getScope(),
				runTimeProperties);
		super.setReducerCount(runTimeProperties.getPropertyAsInt(
				ClusteringParameters.Clustering.MAX_REDUCER_COUNT,
				Math.max(
						2,
						super.getReducerCount())));
		runTimeProperties.setConfig(
				new ParameterEnum[] {
					CentroidParameters.Centroid.EXTRACTOR_CLASS,
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS
				},
				configuration,
				getScope());

		// HP Fortify "Command Injection" false positive
		// What Fortify considers "externally-influenced input"
		// comes only from users with OS-level access anyway
		return super.run(
				configuration,
				runTimeProperties);
	}

	@Override
	protected String getJobName() {
		return "K-Means";
	}
}
