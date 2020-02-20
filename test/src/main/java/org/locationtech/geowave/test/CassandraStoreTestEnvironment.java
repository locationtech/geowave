/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test;

import java.io.File;
import java.io.IOException;
import org.codehaus.plexus.util.FileUtils;
import org.locationtech.geowave.core.store.GenericStoreFactory;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.datastore.cassandra.CassandraStoreFactoryFamily;
import org.locationtech.geowave.datastore.cassandra.cli.CassandraServer;
import org.locationtech.geowave.datastore.cassandra.config.CassandraOptions;
import org.locationtech.geowave.datastore.cassandra.config.CassandraRequiredOptions;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraStoreTestEnvironment extends StoreTestEnvironment {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraStoreTestEnvironment.class);

  private static final GenericStoreFactory<DataStore> STORE_FACTORY =
      new CassandraStoreFactoryFamily().getDataStoreFactory();
  private static CassandraStoreTestEnvironment singletonInstance = null;
  protected static final File TEMP_DIR =
      new File(System.getProperty("user.dir") + File.separator + "target", "cassandra_temp");
  protected static final String NODE_DIRECTORY_PREFIX = "cassandra";

  // for testing purposes, easily toggle between running the cassandra server
  // as multi-nodes or as standalone
  private static final boolean CLUSTERED_MODE = false;

  public static synchronized CassandraStoreTestEnvironment getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new CassandraStoreTestEnvironment();
    }
    return singletonInstance;
  }

  private boolean running = false;

  private CassandraStoreTestEnvironment() {}

  @Override
  protected void initOptions(final StoreFactoryOptions options) {
    final CassandraRequiredOptions cassandraOpts = (CassandraRequiredOptions) options;
    cassandraOpts.setContactPoint("127.0.0.1");
    ((CassandraOptions) cassandraOpts.getStoreOptions()).setBatchWriteSize(5);
  }

  @Override
  protected GenericStoreFactory<DataStore> getDataStoreFactory() {
    return STORE_FACTORY;
  }

  @Override
  public void setup() {
    if (!running) {
      if (TEMP_DIR.exists()) {
        cleanTempDir();
      }
      if (!TEMP_DIR.mkdirs()) {
        LOGGER.warn("Unable to create temporary cassandra directory");
      }
      if (CLUSTERED_MODE) {
        new CassandraServer(4, 512, TEMP_DIR.getAbsolutePath()).start();
      } else {
        new CassandraServer(1, 512, TEMP_DIR.getAbsolutePath()).start();
      }
      running = true;
    }
  }

  @Override
  public void tearDown() {
    if (running) {
      if (CLUSTERED_MODE) {
        new CassandraServer(4, 512, TEMP_DIR.getAbsolutePath()).stop();
      } else {
        new CassandraServer(1, 512, TEMP_DIR.getAbsolutePath()).stop();
      }
      running = false;
    }
    try {
      // it seems sometimes one of the nodes processes is still holding
      // onto a file, so wait a short time to be able to reliably clean up
      Thread.sleep(1500);
    } catch (final InterruptedException e) {
      LOGGER.warn("Unable to sleep waiting to delete directory", e);
    }
    cleanTempDir();
  }

  private static void cleanTempDir() {
    try {
      FileUtils.deleteDirectory(TEMP_DIR);
    } catch (final IOException e) {
      LOGGER.warn("Unable to delete temp cassandra directory", e);
    }
  }

  @Override
  protected GeoWaveStoreType getStoreType() {
    return GeoWaveStoreType.CASSANDRA;
  }

  @Override
  public TestEnvironment[] getDependentEnvironments() {
    return new TestEnvironment[] {};
  }
}
