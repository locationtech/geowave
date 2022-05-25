/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test;

import java.io.File;
import org.locationtech.geowave.core.store.GenericStoreFactory;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.datastore.dynamodb.DynamoDBStoreFactoryFamily;
import org.locationtech.geowave.datastore.dynamodb.cli.DynamoDBLocal;
import org.locationtech.geowave.datastore.dynamodb.config.DynamoDBOptions;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoDBStoreTestEnvironment extends StoreTestEnvironment {
  private static final GenericStoreFactory<DataStore> STORE_FACTORY =
      new DynamoDBStoreFactoryFamily().getDataStoreFactory();

  private static DynamoDBStoreTestEnvironment singletonInstance = null;

  public static synchronized DynamoDBStoreTestEnvironment getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new DynamoDBStoreTestEnvironment();
    }
    return singletonInstance;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBStoreTestEnvironment.class);

  protected DynamoDBLocal dynamoLocal;
  public static final File DEFAULT_DIR = new File("./target/temp/dynamodb");

  private DynamoDBStoreTestEnvironment() {}

  @Override
  public void setup() {
    // DynamoDB IT's rely on an external dynamo local process
    if (dynamoLocal == null) {
      dynamoLocal = new DynamoDBLocal(DEFAULT_DIR.getAbsolutePath()); // uses tmp dir
    }

    // Make sure we clean up any old processes first
    if (dynamoLocal.isRunning()) {
      dynamoLocal.stop();
    }

    if (!dynamoLocal.start()) {
      LOGGER.error("DynamoDB emulator startup failed");
    }
  }

  @Override
  public void tearDown() {
    dynamoLocal.stop();
  }

  @Override
  protected GenericStoreFactory<DataStore> getDataStoreFactory() {
    return STORE_FACTORY;
  }

  @Override
  protected GeoWaveStoreType getStoreType() {
    return GeoWaveStoreType.DYNAMODB;
  }

  @Override
  protected void initOptions(final StoreFactoryOptions options) {
    ((DynamoDBOptions) options).setEndpoint("http://localhost:8000");
  }

  @Override
  public TestEnvironment[] getDependentEnvironments() {
    return new TestEnvironment[] {};
  }
}
