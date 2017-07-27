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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public abstract class AnalyticJobRunner extends
		Configured implements
		Tool
{
	@SuppressWarnings("deprecation")
	public int runJob()
			throws IOException,
			InterruptedException,
			ClassNotFoundException {
		final Configuration conf = super.getConf();

		final Job job = Job.getInstance(conf);

		job.setJarByClass(this.getClass());

		final boolean jobSuccess = job.waitForCompletion(true);

		return (jobSuccess) ? 0 : 1;

	}

	protected abstract void configure(
			Job job )
			throws Exception;

	@Override
	public int run(
			final String[] args )
			throws Exception {
		return runJob();
	}
}
