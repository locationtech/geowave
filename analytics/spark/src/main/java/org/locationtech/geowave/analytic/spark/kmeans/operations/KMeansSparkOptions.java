/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.kmeans.operations;

import com.beust.jcommander.Parameter;

public class KMeansSparkOptions {
  @Parameter(names = {"-n", "--name"}, description = "The spark application name")
  private String appName = "KMeans Spark";

  @Parameter(names = {"-ho", "--host"}, description = "The spark driver host")
  private String host = "localhost";

  @Parameter(names = {"-m", "--master"}, description = "The spark master designation")
  private String master = "yarn";

  @Parameter(names = {"-k", "--numClusters"}, description = "The number of clusters to generate")
  private Integer numClusters = 8;

  @Parameter(names = {"-i", "--numIterations"}, description = "The number of iterations to run")
  private Integer numIterations = 20;

  @Parameter(names = {"-e", "--epsilon"}, description = "The convergence tolerance")
  private Double epsilon = null;

  @Parameter(names = {"-t", "--useTime"}, description = "Use time field from input data")
  private Boolean useTime = false;

  @Parameter(names = {"-h", "--hulls"}, description = "Generate convex hulls?")
  private Boolean generateHulls = false;

  @Parameter(
      names = {"-ch", "--computeHullData"},
      description = "Compute hull count, area and density?")
  private Boolean computeHullData = false;

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

  @Parameter(
      names = {"-ct", "--centroidType"},
      description = "Feature type name for centroid output")
  private String centroidTypeName = "kmeans_centroids";

  @Parameter(names = {"-ht", "--hullType"}, description = "Feature type name for hull output")
  private String hullTypeName = "kmeans_hulls";

  public KMeansSparkOptions() {}

  public String getAppName() {
    return appName;
  }

  public void setAppName(final String appName) {
    this.appName = appName;
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

  public Integer getNumClusters() {
    return numClusters;
  }

  public void setNumClusters(final Integer numClusters) {
    this.numClusters = numClusters;
  }

  public Integer getNumIterations() {
    return numIterations;
  }

  public void setNumIterations(final Integer numIterations) {
    this.numIterations = numIterations;
  }

  public Double getEpsilon() {
    return epsilon;
  }

  public void setEpsilon(final Double epsilon) {
    this.epsilon = epsilon;
  }

  public Boolean isUseTime() {
    return useTime;
  }

  public void setUseTime(final Boolean useTime) {
    this.useTime = useTime;
  }

  public Boolean isGenerateHulls() {
    return generateHulls;
  }

  public void setGenerateHulls(final Boolean generateHulls) {
    this.generateHulls = generateHulls;
  }

  public Boolean isComputeHullData() {
    return computeHullData;
  }

  public void setComputeHullData(final Boolean computeHullData) {
    this.computeHullData = computeHullData;
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

  public String getCentroidTypeName() {
    return centroidTypeName;
  }

  public void setCentroidTypeName(final String centroidTypeName) {
    this.centroidTypeName = centroidTypeName;
  }

  public String getHullTypeName() {
    return hullTypeName;
  }

  public void setHullTypeName(final String hullTypeName) {
    this.hullTypeName = hullTypeName;
  }
}
