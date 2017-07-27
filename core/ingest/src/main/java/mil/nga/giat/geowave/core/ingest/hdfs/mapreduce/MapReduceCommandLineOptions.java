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
package mil.nga.giat.geowave.core.ingest.hdfs.mapreduce;

import com.beust.jcommander.Parameter;

/**
 * This class encapsulates all of the options and parsed values specific to
 * setting up the GeoWave ingestion framework to run on hadoop map-reduce.
 * Currently the only required parameter is the host name and port for the
 * hadoop job tracker.
 */
public class MapReduceCommandLineOptions
{
	@Parameter(names = "--jobtracker", description = "Hadoop job tracker hostname and port in the format hostname:port")
	private String jobTrackerHostPort;

	@Parameter(names = "--resourceman", description = "Yarn resource manager hostname and port in the format hostname:port")
	private String resourceManager;

	public MapReduceCommandLineOptions() {

	}

	public String getJobTrackerHostPort() {
		return jobTrackerHostPort;
	}

	public void setJobTrackerHostPort(
			String jobTrackerHostPort ) {
		this.jobTrackerHostPort = jobTrackerHostPort;
	}

	public String getResourceManager() {
		return resourceManager;
	}

	public void setResourceManager(
			String resourceManager ) {
		this.resourceManager = resourceManager;
	}

	public String getJobTrackerOrResourceManagerHostPort() {
		return jobTrackerHostPort == null ? resourceManager : jobTrackerHostPort;
	}
}
