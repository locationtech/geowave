/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra.cli;

import java.io.IOException;
import com.beust.jcommander.Parameter;

public class RunCassandraServerOptions {
  @Parameter(
      names = {"--clusterSize", "-c"},
      description = "The number of individual Cassandra processes to run")
  private Integer clusterSize = 1;

  @Parameter(
      names = {"--directory", "-d"},
      required = true,
      description = "The directory to use for Cassandra")
  private String directory = null;

  @Parameter(names = {"--maxMemoryMB", "-m"}, description = "The maximum memory to use in MB")
  private Integer memory = 512;

  public Integer getClusterSize() {
    return clusterSize;
  }

  public String getDirectory() {
    return directory;
  }

  public Integer getMemory() {
    return memory;
  }

  public void setClusterSize(Integer clusterSize) {
    this.clusterSize = clusterSize;
  }

  public void setDirectory(String directory) {
    this.directory = directory;
  }

  public void setMemory(Integer memory) {
    this.memory = memory;
  }

  public CassandraServer getServer() throws IOException {
    return new CassandraServer(this);
  }
}
