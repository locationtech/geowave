/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.cli;

import java.util.ArrayList;
import java.util.List;
import com.beust.jcommander.Parameter;

public class RunHBaseServerOptions {

  @Parameter(
      names = {"--auth", "-a"},
      description = "A list of authorizations to grant the 'admin' user")
  private List<String> auths = new ArrayList<>();
  @Parameter(names = {"--libDir", "-l"}, description = "Directory for HBase server-side libraries")
  private String libDir = "./lib/services/third-party/embedded-hbase/lib";
  @Parameter(names = {"--dataDir", "-d"}, description = "Directory for HBase server-side data")
  private String dataDir = "./lib/services/third-party/embedded-hbase/data";
  @Parameter(
      names = {"--zkDataDir", "-z"},
      description = "The host:port to run a single instance of zookeeper on")
  private String zkDataDir = "./lib/services/third-party/embedded-hbase/zookeeper";
  @Parameter(
      names = {"--regionServers", "-r"},
      description = "The number of region server processes")
  private int numRegionServers = 1;

  public HBaseMiniCluster getMiniCluster() throws Exception {
    return new HBaseMiniCluster(auths, zkDataDir, libDir, dataDir, numRegionServers);
  }

  public List<String> getAuths() {
    return auths;
  }

  public void setAuths(final List<String> auths) {
    this.auths = auths;
  }

  public String getLibDir() {
    return libDir;
  }

  public void setLibDir(final String libDir) {
    this.libDir = libDir;
  }

  public String getZkDataDir() {
    return zkDataDir;
  }

  public void setZkDataDir(final String zkDataDir) {
    this.zkDataDir = zkDataDir;
  }

  public String getDataDir() {
    return dataDir;
  }

  public void setDataDir(final String dataDir) {
    this.dataDir = dataDir;
  }

  public void setNumRegionServers(final int numRegionServers) {
    this.numRegionServers = numRegionServers;
  }

  public int getNumRegionServers() {
    return numRegionServers;
  }
}
