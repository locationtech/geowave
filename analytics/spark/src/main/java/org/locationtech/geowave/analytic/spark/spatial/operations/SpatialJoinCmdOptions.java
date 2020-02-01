/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.spatial.operations;

import com.beust.jcommander.Parameter;

public class SpatialJoinCmdOptions {
  @Parameter(names = {"-n", "--name"}, description = "The spark application name")
  private String appName = "Spatial Join Spark";

  @Parameter(names = {"-ho", "--host"}, description = "The spark driver host")
  private String host = "localhost";

  @Parameter(names = {"-m", "--master"}, description = "The spark master designation")
  private String master = "yarn";

  @Parameter(
      names = {"-pc", "--partCount",},
      description = "The default partition count to set for Spark RDDs. Should be big enough to support largest RDD that will be used. Sets spark.default.parallelism")
  private Integer partCount = -1;

  @Parameter(
      names = {"-lt", "--leftTypeName"},
      description = "Feature type name of left Store to use in join")
  private String leftAdapterTypeName = null;

  @Parameter(
      names = {"-ol", "--outLeftTypeName"},
      description = "Feature type name of left join results.")
  private String outLeftAdapterTypeName = null;

  @Parameter(
      names = {"-rt", "--rightTypeName"},
      description = "Feature type name of right Store to use in join")
  private String rightAdapterTypeName = null;

  @Parameter(
      names = {"-or", "--outRightTypeName"},
      description = "Feature type name of right join results.")
  private String outRightAdapterTypeName = null;

  @Parameter(
      names = {"-p", "--predicate"},
      description = "Name of the UDF function to use when performing Spatial Join")
  private String predicate = "GeomIntersects";

  @Parameter(
      names = {"-r", "--radius",},
      description = "Used for distance join predicate and other spatial operations that require a scalar radius.")
  private Double radius = 0.01;

  @Parameter(
      names = {"-not", "--negative",},
      description = "Used for testing a negative result from geometry predicate. i.e GeomIntersects() == false")
  private boolean negativeTest = false;

  // TODO: Experiment with collecting + broadcasting rdds when one side can
  // fit into memory

  public SpatialJoinCmdOptions() {}

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

  public Integer getPartCount() {
    return partCount;
  }

  public void setPartCount(final Integer partCount) {
    this.partCount = partCount;
  }

  public String getLeftAdapterTypeName() {
    return leftAdapterTypeName;
  }

  public void setLeftAdapterTypeName(final String leftAdapterTypeName) {
    this.leftAdapterTypeName = leftAdapterTypeName;
  }

  public String getRightAdapterTypeName() {
    return rightAdapterTypeName;
  }

  public void setRightAdapterTypeName(final String rightAdapterTypeName) {
    this.rightAdapterTypeName = rightAdapterTypeName;
  }

  public String getPredicate() {
    return predicate;
  }

  public void setPredicate(final String predicate) {
    this.predicate = predicate;
  }

  public Double getRadius() {
    return radius;
  }

  public void setRadius(final Double radius) {
    this.radius = radius;
  }

  public String getOutputLeftAdapterTypeName() {
    return outLeftAdapterTypeName;
  }

  public void setOutputLeftAdapterTypeName(final String outLeftAdapterTypeName) {
    this.outLeftAdapterTypeName = outLeftAdapterTypeName;
  }

  public String getOutputRightAdapterTypeName() {
    return outRightAdapterTypeName;
  }

  public void setOutputRightAdapterTypeName(final String outRightAdapterTypeName) {
    this.outRightAdapterTypeName = outRightAdapterTypeName;
  }

  public boolean isNegativeTest() {
    return negativeTest;
  }

  public void setNegativeTest(final boolean negativeTest) {
    this.negativeTest = negativeTest;
  }
}
