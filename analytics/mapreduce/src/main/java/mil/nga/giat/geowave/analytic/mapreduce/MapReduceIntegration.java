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
package mil.nga.giat.geowave.analytic.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.analytic.PropertyManagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public interface MapReduceIntegration
{
	public int submit(
			final Configuration configuration,
			final PropertyManagement runTimeProperties,
			final GeoWaveAnalyticJobRunner tool )
			throws Exception;

	public Counters waitForCompletion(
			Job job )
			throws InterruptedException,
			Exception;

	public Job getJob(
			Tool tool )
			throws IOException;

	public Configuration getConfiguration(
			final PropertyManagement runTimeProperties )
			throws IOException;
}
