/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test;

import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.locationtech.geowave.core.store.GenericStoreFactory;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.datastore.bigtable.BigTableStoreFactoryFamily;
import org.locationtech.geowave.datastore.bigtable.cli.BigtableEmulator;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigtableStoreTestEnvironment extends StoreTestEnvironment {
  private static final GenericStoreFactory<DataStore> STORE_FACTORY =
      new BigTableStoreFactoryFamily().getDataStoreFactory();
  private static BigtableStoreTestEnvironment singletonInstance = null;

  public static synchronized BigtableStoreTestEnvironment getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new BigtableStoreTestEnvironment();
    }
    return singletonInstance;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(BigtableStoreTestEnvironment.class);

  protected BigtableEmulator emulator;

  // Set to false if you're running an emulator elsewhere.
  // To run externally, see https://cloud.google.com/bigtable/docs/emulator
  private boolean internalEmulator = true;

  // Default host:port
  private String emulatorHostPort = "127.0.0.1:8086";

  // Default download location
  private String sdkDownloadUrl = "https://storage.googleapis.com/cloud-sdk-release";
  private String sdkFile = "google-cloud-sdk-341.0.0-linux-x86_64.tar.gz";
  private boolean environmentInitialized = false;

  private BigtableStoreTestEnvironment() {}

  @Override
  protected void initOptions(final StoreFactoryOptions options) {}

  @Override
  protected GenericStoreFactory<DataStore> getDataStoreFactory() {
    return STORE_FACTORY;
  }

  @Override
  protected GeoWaveStoreType getStoreType() {
    return GeoWaveStoreType.BIGTABLE;
  }

  @Override
  public void setup() {
    initEnv();
    if (internalEmulator && (emulator == null)) {
      final String downloadUrlProp = System.getProperty(BigtableEmulator.DOWNLOAD_URL_PROPERTY);
      if (TestUtils.isSet(downloadUrlProp)) {
        sdkDownloadUrl = downloadUrlProp;
        LOGGER.warn("Bigtable SDK download URL: " + sdkDownloadUrl);
      } else {
        LOGGER.warn("Bigtable SDK download URL (default): " + sdkDownloadUrl);
      }

      final String downloadFileProp = System.getProperty(BigtableEmulator.DOWNLOAD_FILE_PROPERTY);
      if (TestUtils.isSet(downloadFileProp)) {
        sdkFile = downloadFileProp;
        LOGGER.warn("Bigtable SDK file: " + sdkFile);
      } else {
        LOGGER.warn("Bigtable SDK file (default): " + sdkFile);
      }

      emulator =
          new BigtableEmulator(BigtableEmulator.DEFAULT_DIR.getPath(), sdkDownloadUrl, sdkFile);

      // Make sure we clean up any old processes first
      if (emulator.isRunning()) {
        emulator.stop();
      }

      if (!emulator.start(emulatorHostPort)) {
        LOGGER.error("Bigtable emulator startup failed");
      }
    }
  }

  private void initEnv() {
    if (!environmentInitialized) {
      final String internalEmulatorProp = System.getProperty(BigtableEmulator.INTERNAL_PROPERTY);
      if (TestUtils.isSet(internalEmulatorProp)) {
        internalEmulator = Boolean.parseBoolean(internalEmulatorProp);
        LOGGER.warn("Bigtable internal emulator enabled: " + internalEmulator);
      } else {
        LOGGER.warn("Bigtable internal emulator disabled by default");
      }

      final String hostPortProp = System.getProperty(BigtableEmulator.HOST_PORT_PROPERTY);
      if (TestUtils.isSet(hostPortProp)) {
        emulatorHostPort = hostPortProp;
        LOGGER.warn("Bigtable emulator will run at: " + emulatorHostPort);
      } else {
        LOGGER.warn("Bigtable emulator will run at default location: " + emulatorHostPort);
      }

      // Set the host:port property in the junit env, even if external
      // gcloud emulator
      final EnvironmentVariables environmentVariables = new EnvironmentVariables();
      environmentVariables.set("BIGTABLE_EMULATOR_HOST", emulatorHostPort);
      environmentInitialized = true;
    }
  }

  @Override
  public void tearDown() {
    if (internalEmulator) {
      if (emulator != null) {
        emulator.stop();
        emulator = null;
      }
    }
  }

  @Override
  public TestEnvironment[] getDependentEnvironments() {
    return new TestEnvironment[] {};
  }
}
