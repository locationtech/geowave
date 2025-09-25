/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.cli;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperMiniCluster {

  private static ZookeeperMiniCluster singletonInstance = null;

  public static synchronized ZookeeperMiniCluster getInstance(
      final String hbaseLibDir,
      final String zookeeperDataDir) {
    if (singletonInstance == null) {
      singletonInstance = new ZookeeperMiniCluster(hbaseLibDir, zookeeperDataDir);
    }
    return singletonInstance;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperMiniCluster.class);
  protected String zookeeper;
  protected String zkDataDir;

  private Object zookeeperLocalCluster;
  private final String hbaseLibDir;

  private ZookeeperMiniCluster(final String hbaseLibDir, final String zkDataDir) {
    this.hbaseLibDir = hbaseLibDir;
    this.zkDataDir = zkDataDir;
  }

  public void setup() throws Exception {
    if ((zookeeper == null) || zookeeper.isEmpty()) {
      System.setProperty("zookeeper.4lw.commands.whitelist", "*");
      final ClassLoader prevCl = Thread.currentThread().getContextClassLoader();
      try {
        final ClassLoader hbaseMiniClusterCl =
            HBaseMiniClusterClassLoader.getInstance(prevCl, hbaseLibDir);
        Thread.currentThread().setContextClassLoader(hbaseMiniClusterCl);
        final Configuration conf =
            (Configuration) Class.forName(
                "org.apache.hadoop.hbase.HBaseConfiguration",
                true,
                hbaseMiniClusterCl).getMethod("create").invoke(null);
        conf.setInt("test.hbase.zookeeper.property.clientPort", 2181);
        System.setProperty(
            "test.build.data.basedirectory",
            conf.get("zookeeper.temp.dir", zkDataDir));
        final Class<?> htuClass =
            Class.forName("org.apache.hadoop.hbase.HBaseTestingUtility", true, hbaseMiniClusterCl);
        final Object instance = htuClass.getConstructor(Configuration.class).newInstance(conf);
        if (instance == null) {
          throw new IllegalStateException(
              "Mini Zookeeper cluster failed to instantiate HBaseTestingUtility");
        }
        zookeeperLocalCluster = instance;
        htuClass.getMethod("startMiniZKCluster").invoke(zookeeperLocalCluster);
      } catch (final Exception e) {
        LOGGER.error("Exception starting zookeeperLocalCluster: " + e, e);
        throw e;
      } finally {
        Thread.currentThread().setContextClassLoader(prevCl);
      }
      final Object zkCluster =
          zookeeperLocalCluster.getClass().getMethod("getZkCluster").invoke(zookeeperLocalCluster);
      zookeeper = "127.0.0.1:" + zkCluster.getClass().getMethod("getClientPort").invoke(zkCluster);
    }
  }

  public void tearDown() throws Exception {
    if (zookeeperLocalCluster == null) {
      zookeeper = null;
      return;
    }
    try {
      zookeeperLocalCluster.getClass().getMethod("shutdownMiniZKCluster").invoke(
          zookeeperLocalCluster);
      if (!(Boolean) zookeeperLocalCluster.getClass().getMethod("cleanupTestDir").invoke(
          zookeeperLocalCluster)) {
        LOGGER.warn("Unable to delete mini zookeeper temporary directory");
      }
    } catch (final Exception e) {
      LOGGER.warn("Unable to delete or shutdown mini zookeeper temporary directory", e);
    }

    zookeeper = null;
  }

  public String getZookeeper() {
    return zookeeper;
  }
}
