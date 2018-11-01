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
package org.locationtech.geowave.core.ingest.hdfs.mapreduce;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.IParameterSplitter;
import com.google.common.collect.Lists;

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
	
	@Parameter(names= "--conf", description = "Job configuration property in the format Name=Value", splitter=NoSplitter.class)
	private List<String> configurationProperties;

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
	
	public void setConfigurationProperties(
			List<String> configurationProperties ) {
		this.configurationProperties = configurationProperties;
	}
	
	public void applyConfigurationProperties(Configuration conf) {
		if (configurationProperties != null) {
			for (String property : configurationProperties) {
				String[] kvp = property.split("=");
				if (kvp.length != 2) {
					throw new IllegalArgumentException("Unable to use configuration property: " + property);
				}
				conf.set(kvp[0], kvp[1]);
			}
		}
	}
	
	public static class NoSplitter implements IParameterSplitter {

		@Override
		public List<String> split(
				String value ) {
			return Lists.newArrayList(value);
		}
		
	}
}
