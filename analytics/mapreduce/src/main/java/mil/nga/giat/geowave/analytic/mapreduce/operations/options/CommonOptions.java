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
package mil.nga.giat.geowave.analytic.mapreduce.operations.options;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.analytic.param.InputParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.annotations.CommonParameter;
import mil.nga.giat.geowave.analytic.param.annotations.ExtractParameter;
import mil.nga.giat.geowave.analytic.param.annotations.InputParameter;
import mil.nga.giat.geowave.analytic.param.annotations.MapReduceParameter;
import mil.nga.giat.geowave.analytic.param.annotations.OutputParameter;
import mil.nga.giat.geowave.core.cli.annotations.PrefixParameter;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

public class CommonOptions
{
	@MapReduceParameter(MapReduceParameters.MRConfig.CONFIG_FILE)
	@Parameter(names = {
		"-conf",
		"--mapReduceConfigFile"
	}, description = "MapReduce Configuration")
	private String mapReduceConfigFile;

	@MapReduceParameter(MapReduceParameters.MRConfig.HDFS_BASE_DIR)
	@Parameter(names = {
		"-hdfsbase",
		"--mapReduceHdfsBaseDir"
	}, required = true, description = "Fully qualified path to the base directory in hdfs")
	private String mapReduceHdfsBaseDir;

	@MapReduceParameter(MapReduceParameters.MRConfig.HDFS_HOST_PORT)
	@Parameter(names = {
		"-hdfs",
		"--mapReduceHdfsHostPort"
	}, description = "HDFS hostname and port in the format hostname:port")
	private String mapReduceHdfsHostPort;

	@MapReduceParameter(MapReduceParameters.MRConfig.JOBTRACKER_HOST_PORT)
	@Parameter(names = {
		"-jobtracker",
		"--mapReduceJobtrackerHostPort"
	}, description = "[REQUIRED (or resourceman)] Hadoop job tracker hostname and port in the format hostname:port")
	private String mapReduceJobtrackerHostPort;

	@MapReduceParameter(MapReduceParameters.MRConfig.YARN_RESOURCE_MANAGER)
	@Parameter(names = {
		"-resourceman",
		"--mapReduceYarnResourceManager"
	}, description = "[REQUIRED (or jobtracker)] Yarn resource manager hostname and port in the format hostname:port")
	private String mapReduceYarnResourceManager;

	@CommonParameter(CommonParameters.Common.DISTANCE_FUNCTION_CLASS)
	@Parameter(names = {
		"-cdf",
		"--commonDistanceFunctionClass"
	}, description = "Distance Function Class implements mil.nga.giat.geowave.analytics.distance.DistanceFn")
	private String commonDistanceFunctionClass;

	@ParametersDelegate
	@PrefixParameter(prefix = "query")
	private QueryOptionsCommand queryOptions = new QueryOptionsCommand();

	@ExtractParameter(ExtractParameters.Extract.MAX_INPUT_SPLIT)
	@Parameter(names = {
		"-emx",
		"--extractMaxInputSplit"
	}, required = true, description = "Maximum hdfs input split size")
	private String extractMaxInputSplit;

	@ExtractParameter(ExtractParameters.Extract.MIN_INPUT_SPLIT)
	@Parameter(names = {
		"-emn",
		"--extractMinInputSplit"
	}, required = true, description = "Minimum hdfs input split size")
	private String extractMinInputSplit;

	@ExtractParameter(ExtractParameters.Extract.QUERY)
	@Parameter(names = {
		"-eq",
		"--extractQuery"
	}, description = "Query")
	private String extractQuery;

	@OutputParameter(OutputParameters.Output.OUTPUT_FORMAT)
	@Parameter(names = {
		"-ofc",
		"--outputOutputFormat"
	}, description = "Output Format Class")
	private String outputOutputFormat;

	@InputParameter(InputParameters.Input.INPUT_FORMAT)
	@Parameter(names = {
		"-ifc",
		"--inputFormatClass"
	}, description = "Input Format Class")
	private String inputFormatClass;

	@InputParameter(InputParameters.Input.HDFS_INPUT_PATH)
	@Parameter(names = {
		"-iip",
		"--inputHdfsPath"
	}, hidden = true, description = "Input Path")
	private String inputHdfsPath;

	@OutputParameter(OutputParameters.Output.REDUCER_COUNT)
	@Parameter(names = {
		"-orc",
		"--outputReducerCount"
	}, description = "Number of Reducers For Output")
	private String outputReducerCount;

	public String getCommonDistanceFunctionClass() {
		return commonDistanceFunctionClass;
	}

	public void setCommonDistanceFunctionClass(
			String commonDistanceFunctionClass ) {
		this.commonDistanceFunctionClass = commonDistanceFunctionClass;
	}

	public QueryOptionsCommand getQueryOptions() {
		return queryOptions;
	}

	public void setQueryOptions(
			QueryOptionsCommand extractQueryOptions ) {
		this.queryOptions = extractQueryOptions;
	}

	public String getExtractMaxInputSplit() {
		return extractMaxInputSplit;
	}

	public void setExtractMaxInputSplit(
			String extractMaxInputSplit ) {
		this.extractMaxInputSplit = extractMaxInputSplit;
	}

	public String getExtractMinInputSplit() {
		return extractMinInputSplit;
	}

	public void setExtractMinInputSplit(
			String extractMinInputSplit ) {
		this.extractMinInputSplit = extractMinInputSplit;
	}

	public String getExtractQuery() {
		return extractQuery;
	}

	public void setExtractQuery(
			String extractQuery ) {
		this.extractQuery = extractQuery;
	}

	public String getOutputOutputFormat() {
		return outputOutputFormat;
	}

	public void setOutputOutputFormat(
			String outputOutputFormat ) {
		this.outputOutputFormat = outputOutputFormat;
	}

	public String getOutputReducerCount() {
		return outputReducerCount;
	}

	public void setOutputReducerCount(
			String outputReducerCount ) {
		this.outputReducerCount = outputReducerCount;
	}

	public String getInputFormatClass() {
		return inputFormatClass;
	}

	public void setInputFormatClass(
			String inputFormatClass ) {
		this.inputFormatClass = inputFormatClass;
	}

	public String getInputHdfsPath() {
		return inputHdfsPath;
	}

	public void setInputHdfsPath(
			String inputHdfsPath ) {
		this.inputHdfsPath = inputHdfsPath;
	}

	/**
	 * Build the query options from the command line arguments.
	 * 
	 * @return
	 */
	public QueryOptions buildQueryOptions() {
		final QueryOptions options = new QueryOptions();
		if (queryOptions.getAdapterIds() != null && queryOptions.getAdapterIds().size() > 0)
			options.setAdapter(Lists.transform(
					queryOptions.getAdapterIds(),
					new Function<String, ByteArrayId>() {
						@Override
						public ByteArrayId apply(
								String input ) {
							return new ByteArrayId(
									input);
						}
					}));
		if (queryOptions.getAuthorizations() != null) {
			options.setAuthorizations(this.queryOptions.getAuthorizations().toArray(
					new String[this.queryOptions.getAuthorizations().size()]));
		}
		if (queryOptions.getIndexId() != null) {
			options.setIndexId(new ByteArrayId(
					queryOptions.getIndexId()));
		}
		return options;
	}

	public String getMapReduceConfigFile() {
		return mapReduceConfigFile;
	}

	public void setMapReduceConfigFile(
			String mapReduceConfigFile ) {
		this.mapReduceConfigFile = mapReduceConfigFile;
	}

	public String getMapReduceHdfsBaseDir() {
		return mapReduceHdfsBaseDir;
	}

	public void setMapReduceHdfsBaseDir(
			String mapReduceHdfsBaseDir ) {
		this.mapReduceHdfsBaseDir = mapReduceHdfsBaseDir;
	}

	public String getMapReduceHdfsHostPort() {
		return mapReduceHdfsHostPort;
	}

	public void setMapReduceHdfsHostPort(
			String mapReduceHdfsHostPort ) {
		this.mapReduceHdfsHostPort = mapReduceHdfsHostPort;
	}

	public String getMapReduceJobtrackerHostPort() {
		return mapReduceJobtrackerHostPort;
	}

	public void setMapReduceJobtrackerHostPort(
			String mapReduceJobtrackerHostPort ) {
		this.mapReduceJobtrackerHostPort = mapReduceJobtrackerHostPort;
	}

	public String getMapReduceYarnResourceManager() {
		return mapReduceYarnResourceManager;
	}

	public void setMapReduceYarnResourceManager(
			String mapReduceYarnResourceManager ) {
		this.mapReduceYarnResourceManager = mapReduceYarnResourceManager;
	}
}
