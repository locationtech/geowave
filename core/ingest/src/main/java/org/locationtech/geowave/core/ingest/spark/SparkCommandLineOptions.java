/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.spark;

import com.beust.jcommander.Parameter;

public class SparkCommandLineOptions {

  @Parameter(names = {"-n", "--name"}, description = "The spark application name")
  private String appName = "Spark Ingest";

  @Parameter(names = {"-ho", "--host"}, description = "The spark driver host")
  private String host = "localhost";

  @Parameter(names = {"-m", "--master"}, description = "The spark master designation")
  private String master = "local";

  @Parameter(names = {"-e", "--numexecutors"}, description = "Number of executors")
  private int numExecutors = -1;

  @Parameter(names = {"-c", "--numcores"}, description = "Number of cores")
  private int numCores = -1;

  public SparkCommandLineOptions() {}

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

  public int getNumExecutors() {
    return numExecutors;
  }

  public void setNumExecutors(final int numExecutors) {
    this.numExecutors = numExecutors;
  }

  public int getNumCores() {
    return numCores;
  }

  public void setNumCores(final int numCores) {
    this.numCores = numCores;
  }
}
