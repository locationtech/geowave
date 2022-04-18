/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.sparksql.operations;

import com.beust.jcommander.Parameter;

public class SparkSqlOptions {
  @Parameter(names = {"-n", "--name"}, description = "The spark application name")
  private String appName = "GeoWave Spark SQL";

  @Parameter(names = {"-ho", "--host"}, description = "The spark driver host")
  private String host = "localhost";

  @Parameter(names = {"-m", "--master"}, description = "The spark master designation")
  private String master = "yarn";

  @Parameter(names = {"--csv"}, description = "The output CSV file name")
  private String csvOutputFile = null;

  @Parameter(names = {"--out"}, description = "The output datastore name")
  private String outputStoreName = null;

  @Parameter(names = {"--outtype"}, description = "The output feature type (adapter) name")
  private String outputTypeName = null;

  @Parameter(names = {"-s", "--show"}, description = "Number of result rows to display")
  private int showResults = 20;

  public SparkSqlOptions() {}

  public String getOutputStoreName() {
    return outputStoreName;
  }

  public String getAppName() {
    return appName;
  }

  public String getHost() {
    return host;
  }

  public String getMaster() {
    return master;
  }

  public void setAppName(final String name) {
    appName = name;
  }

  public void setHost(final String h) {
    host = h;
  }

  public void setMaster(final String m) {
    master = m;
  }

  public void setOutputStoreName(final String outputStoreName) {
    this.outputStoreName = outputStoreName;
  }

  public int getShowResults() {
    return showResults;
  }

  public void setShowResults(final int showResults) {
    this.showResults = showResults;
  }

  public String getOutputTypeName() {
    return outputTypeName;
  }

  public void setOutputTypeName(final String outputTypeName) {
    this.outputTypeName = outputTypeName;
  }

  public String getCsvOutputFile() {
    return csvOutputFile;
  }

  public void setCsvOutputFile(final String csvOutputFile) {
    this.csvOutputFile = csvOutputFile;
  }
}
