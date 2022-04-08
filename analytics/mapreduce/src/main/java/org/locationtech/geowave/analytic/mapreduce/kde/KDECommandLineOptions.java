/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.kde;

import org.locationtech.geowave.mapreduce.operations.HdfsHostPortConverter;
import com.beust.jcommander.Parameter;

public class KDECommandLineOptions {
  @Parameter(
      names = "--featureType",
      required = true,
      description = "The name of the feature type to run a KDE on")
  private String featureType;

  @Parameter(names = "--indexName", description = "An optional index name to filter the input data")
  private String indexName;

  @Parameter(names = "--minLevel", required = true, description = "The min level to run a KDE at")
  private Integer minLevel;

  @Parameter(names = "--maxLevel", required = true, description = "The max level to run a KDE at")
  private Integer maxLevel;

  @Parameter(names = "--minSplits", description = "The min partitions for the input data")
  private Integer minSplits;

  @Parameter(names = "--maxSplits", description = "The max partitions for the input data")
  private Integer maxSplits;

  @Parameter(names = "--coverageName", required = true, description = "The coverage name")
  private String coverageName;

  @Parameter(
      names = "--hdfsHostPort",
      description = "The hdfs host port",
      converter = HdfsHostPortConverter.class)
  private String hdfsHostPort;

  @Parameter(
      names = "--jobSubmissionHostPort",
      required = true,
      description = "The job submission tracker")
  private String jobTrackerOrResourceManHostPort;

  @Parameter(names = "--tileSize", description = "The tile size")
  private Integer tileSize = 1;

  @Parameter(
      names = "--cqlFilter",
      description = "An optional CQL filter applied to the input data")
  private String cqlFilter;

  @Parameter(
      names = "--outputIndex",
      description = "An optional index for output datastore. Only spatial index type is supported")
  private String outputIndex;

  public String getOutputIndex() {
    return outputIndex;
  }

  public void setOutputIndex(final String outputIndex) {
    this.outputIndex = outputIndex;
  }

  public KDECommandLineOptions() {}

  public String getIndexName() {
    return indexName;
  }

  public void setIndexName(final String inputIndex) {
    this.indexName = inputIndex;
  }

  public String getFeatureType() {
    return featureType;
  }

  public Integer getMinLevel() {
    return minLevel;
  }

  public Integer getMaxLevel() {
    return maxLevel;
  }

  public Integer getMinSplits() {
    return minSplits;
  }

  public Integer getMaxSplits() {
    return maxSplits;
  }

  public String getCoverageName() {
    return coverageName;
  }

  public String getHdfsHostPort() {
    return hdfsHostPort;
  }

  public String getJobTrackerOrResourceManHostPort() {
    return jobTrackerOrResourceManHostPort;
  }

  public Integer getTileSize() {
    return tileSize;
  }

  public String getCqlFilter() {
    return cqlFilter;
  }

  public void setFeatureType(final String featureType) {
    this.featureType = featureType;
  }

  public void setMinLevel(final Integer minLevel) {
    this.minLevel = minLevel;
  }

  public void setMaxLevel(final Integer maxLevel) {
    this.maxLevel = maxLevel;
  }

  public void setMinSplits(final Integer minSplits) {
    this.minSplits = minSplits;
  }

  public void setMaxSplits(final Integer maxSplits) {
    this.maxSplits = maxSplits;
  }

  public void setCoverageName(final String coverageName) {
    this.coverageName = coverageName;
  }

  public void setHdfsHostPort(final String hdfsHostPort) {
    this.hdfsHostPort = hdfsHostPort;
  }

  public void setJobTrackerOrResourceManHostPort(final String jobTrackerOrResourceManHostPort) {
    this.jobTrackerOrResourceManHostPort = jobTrackerOrResourceManHostPort;
  }

  public void setTileSize(final Integer tileSize) {
    this.tileSize = tileSize;
  }

  public void setCqlFilter(final String cqlFilter) {
    this.cqlFilter = cqlFilter;
  }
}
