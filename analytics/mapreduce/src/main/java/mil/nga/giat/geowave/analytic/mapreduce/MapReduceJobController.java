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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import mil.nga.giat.geowave.analytic.IndependentJobRunner;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run a series of jobs in a sequence. Use the {@link PostOperationTask} to
 * allow job definitions to perform an action after running. The purpose of this
 * added task is to support information from a prior job in the sequence(such as
 * temporary file names, job IDs, stats) to be provided to the next job or set
 * of jobs.
 * 
 */
public class MapReduceJobController implements
		MapReduceJobRunner,
		IndependentJobRunner
{

	final static Logger LOGGER = LoggerFactory.getLogger(MapReduceJobController.class);

	private MapReduceJobRunner[] runners;
	private PostOperationTask[] runSetUpTasks;

	public MapReduceJobController() {}

	protected void init(
			final MapReduceJobRunner[] runners,
			final PostOperationTask[] runSetUpTasks ) {
		this.runners = runners;
		this.runSetUpTasks = runSetUpTasks;
	}

	public MapReduceJobRunner[] getRunners() {
		return runners;
	}

	public static interface PostOperationTask
	{
		public void runTask(
				Configuration config,
				MapReduceJobRunner runner );
	}

	public static final PostOperationTask DoNothingTask = new PostOperationTask() {
		@Override
		public void runTask(
				final Configuration config,
				final MapReduceJobRunner runner ) {}
	};

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {
		for (int i = 0; i < runners.length; i++) {
			final MapReduceJobRunner runner = runners[i];
			LOGGER.info("Running " + runner.getClass().toString());
			// HP Fortify "Command Injection" false positive
			// What Fortify considers "externally-influenced input"
			// comes only from users with OS-level access anyway
			final int status = runner.run(
					config,
					runTimeProperties);

			if (status != 0) {
				return status;
			}
			runSetUpTasks[i].runTask(
					config,
					runner);
		}
		return 0;
	}

	@Override
	public Collection<ParameterEnum<?>> getParameters() {
		final Set<ParameterEnum<?>> params = new HashSet<ParameterEnum<?>>();
		params.addAll(MapReduceParameters.getParameters());

		for (int i = 0; i < runners.length; i++) {
			final MapReduceJobRunner runner = runners[i];
			if (runner instanceof IndependentJobRunner) {
				params.addAll(((IndependentJobRunner) runner).getParameters());
			}
		}
		return params;
	}

	@Override
	public int run(
			final PropertyManagement runTimeProperties )
			throws Exception {
		return this.run(
				getConfiguration(runTimeProperties),
				runTimeProperties);
	}

	public static Configuration getConfiguration(
			final PropertyManagement pm )
			throws IOException {
		return new HadoopOptions(
				pm).getConfiguration();
	}

}
