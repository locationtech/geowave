/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.operations.options;

import org.locationtech.geowave.analytic.param.CommonParameters;
import org.locationtech.geowave.analytic.param.ExtractParameters;
import org.locationtech.geowave.analytic.param.InputParameters;
import org.locationtech.geowave.analytic.param.MapReduceParameters;
import org.locationtech.geowave.analytic.param.OutputParameters;
import org.locationtech.geowave.analytic.param.annotations.CommonParameter;
import org.locationtech.geowave.analytic.param.annotations.ExtractParameter;
import org.locationtech.geowave.analytic.param.annotations.InputParameter;
import org.locationtech.geowave.analytic.param.annotations.MapReduceParameter;
import org.locationtech.geowave.analytic.param.annotations.OutputParameter;
import org.locationtech.geowave.core.cli.annotations.PrefixParameter;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class CommonOptions {
  @MapReduceParameter(MapReduceParameters.MRConfig.CONFIG_FILE)
  @Parameter(names = {"-conf", "--mapReduceConfigFile"}, description = "MapReduce Configuration")
  private String mapReduceConfigFile;

  @MapReduceParameter(MapReduceParameters.MRConfig.HDFS_BASE_DIR)
  @Parameter(
      names = {"-hdfsbase", "--mapReduceHdfsBaseDir"},
      required = true,
      description = "Fully qualified path to the base directory in hdfs")
  private String mapReduceHdfsBaseDir;

  @MapReduceParameter(MapReduceParameters.MRConfig.HDFS_HOST_PORT)
  @Parameter(
      names = {"-hdfs", "--mapReduceHdfsHostPort"},
      description = "HDFS hostname and port in the format hostname:port")
  private String mapReduceHdfsHostPort;

  @MapReduceParameter(MapReduceParameters.MRConfig.JOBTRACKER_HOST_PORT)
  @Parameter(
      names = {"-jobtracker", "--mapReduceJobtrackerHostPort"},
      description = "[REQUIRED (or resourceman)] Hadoop job tracker hostname and port in the format hostname:port")
  private String mapReduceJobtrackerHostPort;

  @MapReduceParameter(MapReduceParameters.MRConfig.YARN_RESOURCE_MANAGER)
  @Parameter(
      names = {"-resourceman", "--mapReduceYarnResourceManager"},
      description = "[REQUIRED (or jobtracker)] Yarn resource manager hostname and port in the format hostname:port")
  private String mapReduceYarnResourceManager;

  @CommonParameter(CommonParameters.Common.DISTANCE_FUNCTION_CLASS)
  @Parameter(
      names = {"-cdf", "--commonDistanceFunctionClass"},
      description = "Distance Function Class implements org.locationtech.geowave.analytics.distance.DistanceFn")
  private String commonDistanceFunctionClass;

  @ParametersDelegate
  @PrefixParameter(prefix = "query")
  private QueryOptionsCommand queryOptions = new QueryOptionsCommand();

  @ExtractParameter(ExtractParameters.Extract.MAX_INPUT_SPLIT)
  @Parameter(
      names = {"-emx", "--extractMaxInputSplit"},
      required = true,
      description = "Maximum hdfs input split size")
  private String extractMaxInputSplit;

  @ExtractParameter(ExtractParameters.Extract.MIN_INPUT_SPLIT)
  @Parameter(
      names = {"-emn", "--extractMinInputSplit"},
      required = true,
      description = "Minimum hdfs input split size")
  private String extractMinInputSplit;

  @ExtractParameter(ExtractParameters.Extract.QUERY)
  @Parameter(names = {"-eq", "--extractQuery"}, description = "Query")
  private String extractQuery;

  @OutputParameter(OutputParameters.Output.OUTPUT_FORMAT)
  @Parameter(names = {"-ofc", "--outputOutputFormat"}, description = "Output Format Class")
  private String outputOutputFormat;

  @InputParameter(InputParameters.Input.INPUT_FORMAT)
  @Parameter(names = {"-ifc", "--inputFormatClass"}, description = "Input Format Class")
  private String inputFormatClass;

  @InputParameter(InputParameters.Input.HDFS_INPUT_PATH)
  @Parameter(names = {"-iip", "--inputHdfsPath"}, hidden = true, description = "Input Path")
  private String inputHdfsPath;

  @OutputParameter(OutputParameters.Output.REDUCER_COUNT)
  @Parameter(
      names = {"-orc", "--outputReducerCount"},
      description = "Number of Reducers For Output")
  private String outputReducerCount;

  public String getCommonDistanceFunctionClass() {
    return commonDistanceFunctionClass;
  }

  public void setCommonDistanceFunctionClass(final String commonDistanceFunctionClass) {
    this.commonDistanceFunctionClass = commonDistanceFunctionClass;
  }

  public QueryOptionsCommand getQueryOptions() {
    return queryOptions;
  }

  public void setQueryOptions(final QueryOptionsCommand extractQueryOptions) {
    queryOptions = extractQueryOptions;
  }

  public String getExtractMaxInputSplit() {
    return extractMaxInputSplit;
  }

  public void setExtractMaxInputSplit(final String extractMaxInputSplit) {
    this.extractMaxInputSplit = extractMaxInputSplit;
  }

  public String getExtractMinInputSplit() {
    return extractMinInputSplit;
  }

  public void setExtractMinInputSplit(final String extractMinInputSplit) {
    this.extractMinInputSplit = extractMinInputSplit;
  }

  public String getExtractQuery() {
    return extractQuery;
  }

  public void setExtractQuery(final String extractQuery) {
    this.extractQuery = extractQuery;
  }

  public String getOutputOutputFormat() {
    return outputOutputFormat;
  }

  public void setOutputOutputFormat(final String outputOutputFormat) {
    this.outputOutputFormat = outputOutputFormat;
  }

  public String getOutputReducerCount() {
    return outputReducerCount;
  }

  public void setOutputReducerCount(final String outputReducerCount) {
    this.outputReducerCount = outputReducerCount;
  }

  public String getInputFormatClass() {
    return inputFormatClass;
  }

  public void setInputFormatClass(final String inputFormatClass) {
    this.inputFormatClass = inputFormatClass;
  }

  public String getInputHdfsPath() {
    return inputHdfsPath;
  }

  public void setInputHdfsPath(final String inputHdfsPath) {
    this.inputHdfsPath = inputHdfsPath;
  }

  /**
   * Build the query options from the command line arguments.
   */
  public Query<?> buildQuery() {
    final QueryBuilder<?, ?> bldr = QueryBuilder.newBuilder();
    if ((queryOptions.getTypeNames() != null) && (queryOptions.getTypeNames().length > 0)) {
      bldr.setTypeNames(queryOptions.getTypeNames());
    }
    if (queryOptions.getAuthorizations() != null) {
      bldr.setAuthorizations(queryOptions.getAuthorizations());
    }
    if (queryOptions.getIndexName() != null) {
      bldr.indexName(queryOptions.getIndexName());
    }
    return bldr.build();
  }

  public String getMapReduceConfigFile() {
    return mapReduceConfigFile;
  }

  public void setMapReduceConfigFile(final String mapReduceConfigFile) {
    this.mapReduceConfigFile = mapReduceConfigFile;
  }

  public String getMapReduceHdfsBaseDir() {
    return mapReduceHdfsBaseDir;
  }

  public void setMapReduceHdfsBaseDir(final String mapReduceHdfsBaseDir) {
    this.mapReduceHdfsBaseDir = mapReduceHdfsBaseDir;
  }

  public String getMapReduceHdfsHostPort() {
    return mapReduceHdfsHostPort;
  }

  public void setMapReduceHdfsHostPort(final String mapReduceHdfsHostPort) {
    this.mapReduceHdfsHostPort = mapReduceHdfsHostPort;
  }

  public String getMapReduceJobtrackerHostPort() {
    return mapReduceJobtrackerHostPort;
  }

  public void setMapReduceJobtrackerHostPort(final String mapReduceJobtrackerHostPort) {
    this.mapReduceJobtrackerHostPort = mapReduceJobtrackerHostPort;
  }

  public String getMapReduceYarnResourceManager() {
    return mapReduceYarnResourceManager;
  }

  public void setMapReduceYarnResourceManager(final String mapReduceYarnResourceManager) {
    this.mapReduceYarnResourceManager = mapReduceYarnResourceManager;
  }
}
