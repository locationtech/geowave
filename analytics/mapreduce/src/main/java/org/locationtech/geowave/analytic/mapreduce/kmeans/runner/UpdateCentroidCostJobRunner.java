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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.clustering.CentroidManagerGeoWave;
import org.locationtech.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import org.locationtech.geowave.analytic.mapreduce.CountofDoubleWritable;
import org.locationtech.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import org.locationtech.geowave.analytic.mapreduce.GeoWaveOutputFormatConfiguration;
import org.locationtech.geowave.analytic.mapreduce.GroupIDText;
import org.locationtech.geowave.analytic.mapreduce.MapReduceJobRunner;
import org.locationtech.geowave.analytic.mapreduce.kmeans.UpdateCentroidCostMapReduce;
import org.locationtech.geowave.analytic.param.CentroidParameters;
import org.locationtech.geowave.analytic.param.ParameterEnum;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;
import org.opengis.feature.simple.SimpleFeature;

/**
 * Update the centroid with its cost, measured by the average distance of
 * assigned points.
 * 
 * 
 */
public class UpdateCentroidCostJobRunner extends
		GeoWaveAnalyticJobRunner implements
		MapReduceJobRunner
{

	public UpdateCentroidCostJobRunner() {
		super.setOutputFormatConfiguration(new GeoWaveOutputFormatConfiguration());
	}

	@Override
	public Class<?> getScope() {
		return UpdateCentroidCostMapReduce.class;
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		CentroidManagerGeoWave.setParameters(
				config,
				getScope(),
				runTimeProperties);

		NestedGroupCentroidAssignment.setParameters(
				config,
				getScope(),
				runTimeProperties);
		runTimeProperties.setConfig(
				new ParameterEnum[] {
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS
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
	public void configure(
			final Job job )
			throws Exception {

		job.setMapperClass(UpdateCentroidCostMapReduce.UpdateCentroidCostMap.class);
		job.setMapOutputKeyClass(GroupIDText.class);
		job.setMapOutputValueClass(CountofDoubleWritable.class);
		job.setCombinerClass(UpdateCentroidCostMapReduce.UpdateCentroidCostCombiner.class);
		job.setReducerClass(UpdateCentroidCostMapReduce.UpdateCentroidCostReducer.class);
		job.setReduceSpeculativeExecution(false);
		job.setOutputKeyClass(GeoWaveOutputKey.class);
		job.setOutputValueClass(SimpleFeature.class);
	}

	@Override
	protected String getJobName() {
		return "Update Centroid Cost";
	}
}
