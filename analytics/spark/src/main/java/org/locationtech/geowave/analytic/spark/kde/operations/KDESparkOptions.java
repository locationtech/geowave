/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.kde.operations;

import com.beust.jcommander.Parameter;

public class KDESparkOptions {

  @Parameter(names = {"-n", "--name"}, description = "The spark application name")
  private String appName = "KDE Spark";

  @Parameter(names = "--tileSize", description = "The tile size")
  private Integer tileSize = 1;

  @Parameter(names = "--indexName", description = "An optional index name to filter the input data")
  private String indexName;

  @Parameter(names = "--minLevel", required = true, description = "The min level to run a KDE at")
  private Integer minLevel;

  @Parameter(names = "--maxLevel", required = true, description = "The max level to run a KDE at")
  private Integer maxLevel;
  @Parameter(names = {"-ho", "--host"}, description = "The spark driver host")
  private String host = "localhost";

  @Parameter(names = {"-m", "--master"}, description = "The spark master designation")
  private String master = "yarn";

  @Parameter(
      names = "--cqlFilter",
      description = "An optional CQL filter applied to the input data")
  private String cqlFilter = null;

  @Parameter(names = {"-f", "--featureType"}, description = "Feature type name to query")
  private String typeName = null;

  @Parameter(names = "--minSplits", description = "The min partitions for the input data")
  private Integer minSplits = -1;

  @Parameter(names = "--maxSplits", description = "The max partitions for the input data")
  private Integer maxSplits = -1;

  @Parameter(names = "--coverageName", required = true, description = "The coverage name")
  private String coverageName;

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

  public String getAppName() {
    return appName;
  }

  public void setAppName(final String appName) {
    this.appName = appName;
  }

  public Integer getTileSize() {
    return tileSize;
  }

  public void setTileSize(final Integer tileSize) {
    this.tileSize = tileSize;
  }

  public String getIndexName() {
    return indexName;
  }

  public void setIndexName(final String indexName) {
    this.indexName = indexName;
  }

  public Integer getMinLevel() {
    return minLevel;
  }

  public void setMinLevel(final Integer minLevel) {
    this.minLevel = minLevel;
  }

  public Integer getMaxLevel() {
    return maxLevel;
  }

  public void setMaxLevel(final Integer maxLevel) {
    this.maxLevel = maxLevel;
  }

  public String getHost() {
    return host;
  }

  public void setHost(final String host) {
    this.host = host;
  }

  public String getMaster() {
    return master;
  }

  public void setMaster(final String master) {
    this.master = master;
  }

  public String getCqlFilter() {
    return cqlFilter;
  }

  public void setCqlFilter(final String cqlFilter) {
    this.cqlFilter = cqlFilter;
  }

  public String getTypeName() {
    return typeName;
  }

  public void setTypeName(final String typeName) {
    this.typeName = typeName;
  }

  public Integer getMinSplits() {
    return minSplits;
  }

  public void setMinSplits(final Integer minSplits) {
    this.minSplits = minSplits;
  }

  public Integer getMaxSplits() {
    return maxSplits;
  }

  public void setMaxSplits(final Integer maxSplits) {
    this.maxSplits = maxSplits;
  }

  public String getCoverageName() {
    return coverageName;
  }

  public void setCoverageName(final String coverageName) {
    this.coverageName = coverageName;
  }
}
