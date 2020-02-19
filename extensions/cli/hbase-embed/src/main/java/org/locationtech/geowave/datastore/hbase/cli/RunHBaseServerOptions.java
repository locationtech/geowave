/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.cli;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import com.beust.jcommander.Parameter;

public class RunHBaseServerOptions {
  private static final String DEFAULT_LIB_DIR = "lib/services/third-party/embedded-hbase/lib";
  private static final String DEFAULT_DATA_DIR = "lib/services/third-party/embedded-hbase/data";
  private static final String DEFAULT_ZOOKEEPER_DATA_DIR =
      "lib/services/third-party/embedded-hbase/zookeeper";

  @Parameter(
      names = {"--auth", "-a"},
      description = "A list of authorizations to grant the 'admin' user")
  private List<String> auths = new ArrayList<>();
  @Parameter(
      names = {"--libDir", "-l"},
      description = "Directory for HBase server-side libraries. Defaults to embedded lib directory.")
  private String libDir = null;
  @Parameter(
      names = {"--dataDir", "-d"},
      description = "Directory for HBase server-side data. Defaults to embedded data directory.")
  private String dataDir = null;
  @Parameter(
      names = {"--zkDataDir", "-z"},
      description = "The zookeeper data directory.  Defaults to embedded zookeeper data directory.")
  private String zkDataDir = null;
  @Parameter(
      names = {"--regionServers", "-r"},
      description = "The number of region server processes")
  private int numRegionServers = 1;

  public HBaseMiniCluster getMiniCluster() throws Exception {
    return new HBaseMiniCluster(auths, getZkDataDir(), getLibDir(), getDataDir(), numRegionServers);
  }

  public List<String> getAuths() {
    return auths;
  }

  public void setAuths(final List<String> auths) {
    this.auths = auths;
  }

  public String getLibDir() {
    if (libDir == null) {
      String geowaveHome = System.getProperty("geowave.home", ".");
      return Paths.get(geowaveHome, DEFAULT_LIB_DIR).toString();
    }
    return libDir;
  }

  public void setLibDir(final String libDir) {
    this.libDir = libDir;
  }

  public String getZkDataDir() {
    if (zkDataDir == null) {
      String geowaveHome = System.getProperty("geowave.home", ".");
      return Paths.get(geowaveHome, DEFAULT_ZOOKEEPER_DATA_DIR).toString();
    }
    return zkDataDir;
  }

  public void setZkDataDir(final String zkDataDir) {
    this.zkDataDir = zkDataDir;
  }

  public String getDataDir() {
    if (dataDir == null) {
      String geowaveHome = System.getProperty("geowave.home", ".");
      return Paths.get(geowaveHome, DEFAULT_DATA_DIR).toString();
    }
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
