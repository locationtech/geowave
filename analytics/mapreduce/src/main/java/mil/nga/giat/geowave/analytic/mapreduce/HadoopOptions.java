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

import java.io.FileInputStream;
import java.io.IOException;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters.MRConfig;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates the command-line options and parsed values specific
 * to staging intermediate data to HDFS.
 */
public class HadoopOptions
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HadoopOptions.class);
	private final String hdfsHostPort;
	private final Path basePath;
	private final String jobTrackerHostPort;
	private final Configuration config = new Configuration();

	public HadoopOptions(
			final PropertyManagement runTimeProperties )
			throws IOException {
		final boolean setRemoteInvocation = runTimeProperties.hasProperty(MRConfig.HDFS_HOST_PORT)
				|| runTimeProperties.hasProperty(MRConfig.JOBTRACKER_HOST_PORT);
		final String hostport = runTimeProperties.getPropertyAsString(
				MRConfig.HDFS_HOST_PORT,
				"localhost:53000");
		hdfsHostPort = hostport;
		basePath = new Path(
				runTimeProperties.getPropertyAsString(MRConfig.HDFS_BASE_DIR),
				"/");
		jobTrackerHostPort = runTimeProperties.getPropertyAsString(
				MRConfig.JOBTRACKER_HOST_PORT,
				runTimeProperties.getPropertyAsString(MRConfig.YARN_RESOURCE_MANAGER));

		final String name = runTimeProperties.getPropertyAsString(MapReduceParameters.MRConfig.CONFIG_FILE);

		if (name != null) {
			try (FileInputStream in = new FileInputStream(
					name)) {
				// HP Fortify "Path Manipulation" false positive
				// What fortify identifies as "user input" comes
				// only from users with OS-level access anyway
				config.addResource(
						in,
						name);
			}
			catch (final IOException ex) {
				LOGGER.error(
						"Configuration file not found",
						ex);
				throw ex;
			}
		}

		if (setRemoteInvocation) {
			GeoWaveConfiguratorBase.setRemoteInvocationParams(
					hdfsHostPort,
					jobTrackerHostPort,
					config);
		}
		else {
			LOGGER.info("Assuming local job submission");
		}
		final FileSystem fs = FileSystem.get(config);
		if (!fs.exists(basePath)) {
			LOGGER.error("HDFS base directory does not exist");
			return;
		}
	}

	public HadoopOptions(
			final String hdfsHostPort,
			final Path basePath,
			final String jobTrackerHostport ) {
		this.hdfsHostPort = hdfsHostPort;
		this.basePath = basePath;
		jobTrackerHostPort = jobTrackerHostport;
	}

	public String getHdfsHostPort() {
		return hdfsHostPort;
	}

	public Path getBasePath() {
		return basePath;
	}

	public String getJobTrackerOrResourceManagerHostPort() {
		return jobTrackerHostPort;
	}

	public Configuration getConfiguration() {
		return config;
	}
}
