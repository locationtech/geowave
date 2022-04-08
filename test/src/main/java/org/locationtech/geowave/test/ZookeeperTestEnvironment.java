/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.provider.GssSaslAuthenticationProvider;
import org.apache.hadoop.hbase.security.provider.GssSaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProviders;
import org.locationtech.geowave.datastore.hbase.cli.ZookeeperMiniCluster;

public class ZookeeperTestEnvironment implements TestEnvironment {

  private static ZookeeperTestEnvironment singletonInstance = null;

  public static synchronized ZookeeperTestEnvironment getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new ZookeeperTestEnvironment();
    }
    return singletonInstance;
  }

  protected String zookeeper;

  private ZookeeperMiniCluster zookeeperMiniCluster;

  public static final String ZK_PROPERTY_NAME = "zookeeperUrl";
  public static final String DEFAULT_ZK_TEMP_DIR = "./target/zk_temp";

  private ZookeeperTestEnvironment() {}

  @Override
  public void setup() throws Exception {
    SaslClientAuthenticationProviders.getInstance(new Configuration());
    if (!TestUtils.isSet(zookeeper)) {
      zookeeper = System.getProperty(ZK_PROPERTY_NAME);

      if (!TestUtils.isSet(zookeeper)) {
        zookeeperMiniCluster =
            ZookeeperMiniCluster.getInstance("./target/hbase/lib", DEFAULT_ZK_TEMP_DIR);
        zookeeperMiniCluster.setup();
        zookeeper = zookeeperMiniCluster.getZookeeper();
      }
    }
  }

  @Override
  public void tearDown() throws Exception {
    if (zookeeperMiniCluster != null) {
      zookeeperMiniCluster.tearDown();
    }

    zookeeper = null;
  }

  public String getZookeeper() {
    return zookeeper;
  }

  @Override
  public TestEnvironment[] getDependentEnvironments() {
    return new TestEnvironment[] {};
  }
}
