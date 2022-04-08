/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test;

import java.util.Arrays;
import org.apache.hadoop.hbase.security.User;
import org.locationtech.geowave.core.store.GenericStoreFactory;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.datastore.hbase.HBaseStoreFactoryFamily;
import org.locationtech.geowave.datastore.hbase.cli.HBaseMiniCluster;
import org.locationtech.geowave.datastore.hbase.config.HBaseRequiredOptions;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseStoreTestEnvironment extends StoreTestEnvironment {
  private static final GenericStoreFactory<DataStore> STORE_FACTORY =
      new HBaseStoreFactoryFamily().getDataStoreFactory();

  private static final int NUM_REGION_SERVERS = 2;

  private static HBaseStoreTestEnvironment singletonInstance = null;

  public static synchronized HBaseStoreTestEnvironment getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new HBaseStoreTestEnvironment();
    }
    return singletonInstance;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(HBaseStoreTestEnvironment.class);

  public static final String DEFAULT_HBASE_TEMP_DIR = "./target/hbase_temp";
  protected String zookeeper;

  private HBaseMiniCluster hbaseMiniCluster;

  public HBaseStoreTestEnvironment() {}

  // VisibilityTest valid authorizations
  private static String[] auths = new String[] {"a", "b", "c", "g", "t", "z"};

  protected User SUPERUSER;

  @Override
  protected void initOptions(final StoreFactoryOptions options) {
    final HBaseRequiredOptions hbaseRequiredOptions = (HBaseRequiredOptions) options;
    hbaseRequiredOptions.setZookeeper(zookeeper);
  }

  @Override
  protected GenericStoreFactory<DataStore> getDataStoreFactory() {
    return STORE_FACTORY;
  }

  @Override
  public void setup() {
    if (hbaseMiniCluster == null) {

      if (!TestUtils.isSet(zookeeper)) {
        zookeeper = System.getProperty(ZookeeperTestEnvironment.ZK_PROPERTY_NAME);

        if (!TestUtils.isSet(zookeeper)) {
          zookeeper = ZookeeperTestEnvironment.getInstance().getZookeeper();
          LOGGER.debug("Using local zookeeper URL: " + zookeeper);
        }
      }
      hbaseMiniCluster =
          new HBaseMiniCluster(
              Arrays.asList(auths),
              ZookeeperTestEnvironment.DEFAULT_ZK_TEMP_DIR,
              "./target/hbase/lib",
              DEFAULT_HBASE_TEMP_DIR,
              NUM_REGION_SERVERS);
      hbaseMiniCluster.setup();
    }
  }

  @Override
  public void tearDown() {
    if (hbaseMiniCluster != null) {
      hbaseMiniCluster.tearDown();
      hbaseMiniCluster = null;
    }
  }

  @Override
  protected GeoWaveStoreType getStoreType() {
    return GeoWaveStoreType.HBASE;
  }

  @Override
  public TestEnvironment[] getDependentEnvironments() {
    return new TestEnvironment[] {ZookeeperTestEnvironment.getInstance()};
  }
}
