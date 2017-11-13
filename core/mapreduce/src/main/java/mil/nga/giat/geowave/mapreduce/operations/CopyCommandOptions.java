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
package mil.nga.giat.geowave.mapreduce.operations;

import com.beust.jcommander.Parameter;

public class CopyCommandOptions
{
	@Parameter(names = "--hdfsHostPort", description = "The hdfs host port", converter = HdfsHostPortConverter.class)
	private String hdfsHostPort;

	@Parameter(names = "--jobSubmissionHostPort", required = true, description = "The job submission tracker")
	private String jobTrackerOrResourceManHostPort;

	@Parameter(names = "--minSplits", description = "The min partitions for the input data")
	private Integer minSplits;

	@Parameter(names = "--maxSplits", description = "The max partitions for the input data")
	private Integer maxSplits;

	@Parameter(names = "--numReducers", description = "Number of threads writing at a time (default: 8)")
	private Integer numReducers = 8;

	// Default constructor
	public CopyCommandOptions() {

	}

	public CopyCommandOptions(
			final Integer minSplits,
			final Integer maxSplits,
			final Integer numReducers ) {
		this.minSplits = minSplits;
		this.maxSplits = maxSplits;
		this.numReducers = numReducers;
	}

	public String getHdfsHostPort() {
		return hdfsHostPort;
	}

	public String getJobTrackerOrResourceManHostPort() {
		return jobTrackerOrResourceManHostPort;
	}

	public Integer getMinSplits() {
		return minSplits;
	}

	public Integer getMaxSplits() {
		return maxSplits;
	}

	public Integer getNumReducers() {
		return numReducers;
	}

	public void setHdfsHostPort(
			String hdfsHostPort ) {
		this.hdfsHostPort = hdfsHostPort;
	}

	public void setJobTrackerOrResourceManHostPort(
			String jobTrackerOrResourceManHostPort ) {
		this.jobTrackerOrResourceManHostPort = jobTrackerOrResourceManHostPort;
	}

	public void setMinSplits(
			Integer minSplits ) {
		this.minSplits = minSplits;
	}

	public void setMaxSplits(
			Integer maxSplits ) {
		this.maxSplits = maxSplits;
	}

	public void setNumReducers(
			Integer numReducers ) {
		this.numReducers = numReducers;
	}
}
